import psycopg2
import requests
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler

THROUGHPUT_SERVER_URL = 'http://localhost:9101/throughput'
WINDOW_SECONDS        = 60
DROP_WARNING_PERCENT  = 50
DROP_CRITICAL_PERCENT = 80
MIN_BASELINE_MPS      = 0.05
MIN_SAMPLES           = 3

previous_counts = {}


def get_conn():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        user='debezium',
        password='debezium',
        dbname='destination_db'
    )


def get_current_counts():
    try:
        res = requests.get(THROUGHPUT_SERVER_URL, timeout=5)
        return res.json()
    except Exception as e:
        print(f'  Could not reach throughput server -- {e}')
        return {}


def compute_hourly_baselines(cursor):
    cursor.execute(
        "SELECT DISTINCT topic FROM throughput_metrics"
    )
    topics = [row[0] for row in cursor.fetchall()]

    for topic in topics:
        for hour in range(24):
            cursor.execute("""
                SELECT
                    COALESCE(AVG(messages_per_second), 0),
                    COALESCE(STDDEV(messages_per_second), 0),
                    COUNT(*)
                FROM throughput_metrics
                WHERE topic = %s
                AND EXTRACT(HOUR FROM recorded_at) = %s
                AND recorded_at < NOW() - INTERVAL '5 minutes'
            """, (topic, hour))
            row = cursor.fetchone()

            if not row or int(row[2]) < MIN_SAMPLES:
                continue

            mean   = float(row[0])
            stddev = float(row[1])
            count  = int(row[2])

            cursor.execute("""
                INSERT INTO throughput_hourly_baselines
                    (computed_at, topic, hour_of_day,
                     mean_mps, stddev_mps, sample_count)
                VALUES (NOW(), %s, %s, %s, %s, %s)
            """, (topic, hour, mean, stddev, count))

    print(f'  Hourly baselines recomputed for {len(topics)} topic(s)')


def get_hourly_baseline(cursor, topic):
    current_hour = datetime.now(timezone.utc).hour

    cursor.execute("""
        SELECT mean_mps, stddev_mps, sample_count
        FROM throughput_hourly_baselines
        WHERE topic = %s
        AND hour_of_day = %s
        AND computed_at < NOW() - INTERVAL '5 minutes'
        ORDER BY computed_at DESC
        LIMIT 1
    """, (topic, current_hour))
    row = cursor.fetchone()

    if row and int(row[2]) >= MIN_SAMPLES:
        return float(row[0]), float(row[1]), int(row[2]), current_hour

    cursor.execute("""
        SELECT AVG(mean_mps), AVG(stddev_mps), SUM(sample_count)
        FROM throughput_hourly_baselines
        WHERE topic = %s
        AND computed_at < NOW() - INTERVAL '5 minutes'
    """, (topic,))
    row = cursor.fetchone()

    if row and row[0]:
        return float(row[0]), float(row[1] or 0), int(row[2] or 0), None

    cursor.execute("""
        SELECT AVG(messages_per_second), STDDEV(messages_per_second), COUNT(*)
        FROM throughput_metrics
        WHERE topic = %s
        AND messages_per_second > 0
        AND recorded_at > NOW() - INTERVAL '1 hour'
    """, (topic,))
    row = cursor.fetchone()

    if row and row[0]:
        return float(row[0]), float(row[1] or 0), int(row[2] or 0), None

    return None, None, 0, None


def get_peak_mps(cursor, topic):
    cursor.execute("""
        SELECT MAX(messages_per_second)
        FROM throughput_metrics
        WHERE topic = %s
    """, (topic,))
    row = cursor.fetchone()
    if row and row[0]:
        return float(row[0])
    return 0.0


def save_throughput(cursor, topic, messages_in_window, mps, peak_mps):
    cursor.execute("""
        INSERT INTO throughput_metrics
            (recorded_at, topic, messages_per_minute,
             messages_per_second, peak_per_second, window_seconds)
        VALUES (NOW(), %s, %s, %s, %s, %s)
    """, (topic, messages_in_window, mps, peak_mps, WINDOW_SECONDS))


def get_active_alert(cursor, topic):
    cursor.execute("""
        SELECT id, severity, current_mps
        FROM throughput_alerts
        WHERE topic = %s
        AND resolved = false
        ORDER BY fired_at DESC
        LIMIT 1
    """, (topic,))
    return cursor.fetchone()


def fire_alert(cursor, topic, current_mps,
               baseline_mps, drop_percent, severity):
    cursor.execute("""
        INSERT INTO throughput_alerts
            (fired_at, topic, current_mps, baseline_mps,
             drop_percent, severity)
        VALUES (NOW(), %s, %s, %s, %s, %s)
    """, (topic, current_mps, baseline_mps, drop_percent, severity))
    print(f'  ALERT [{severity}] {topic} -- '
          f'throughput dropped {round(drop_percent)}% '
          f'from {round(baseline_mps, 4)} to {round(current_mps, 4)} mps')


def resolve_alert(cursor, alert_id):
    cursor.execute("""
        UPDATE throughput_alerts
        SET resolved = true, resolved_at = NOW()
        WHERE id = %s
    """, (alert_id,))


def run_throughput_monitor():
    global previous_counts

    print(f'\n{"="*55}')
    print(f'THROUGHPUT MONITOR -- {datetime.now(timezone.utc).isoformat()}')
    print(f'{"="*55}')

    current_counts = get_current_counts()

    if not current_counts:
        print('  No throughput data available.')
        previous_counts = {}
        return

    conn   = get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    compute_hourly_baselines(cursor)

    for topic, total_count in current_counts.items():
        prev_count         = previous_counts.get(topic, 0)
        messages_in_window = total_count - prev_count
        if messages_in_window < 0:
            messages_in_window = total_count

        mps      = round(messages_in_window / WINDOW_SECONDS, 4)
        peak_mps = get_peak_mps(cursor, topic)
        peak_mps = max(peak_mps, mps)

        save_throughput(cursor, topic, messages_in_window, mps, peak_mps)

        mean_mps, stddev_mps, sample_count, hour_used = get_hourly_baseline(
            cursor, topic
        )

        active       = get_active_alert(cursor, topic)
        severity     = None
        drop_percent = 0

        if (mean_mps and
                mean_mps >= MIN_BASELINE_MPS and
                mps < mean_mps):
            drop_percent = ((mean_mps - mps) / mean_mps) * 100
            if drop_percent >= DROP_CRITICAL_PERCENT:
                severity = 'CRITICAL'
            elif drop_percent >= DROP_WARNING_PERCENT:
                severity = 'WARNING'

        if severity:
            if not active:
                fire_alert(
                    cursor, topic, mps,
                    mean_mps, drop_percent, severity
                )
            elif active[1] != severity:
                resolve_alert(cursor, active[0])
                fire_alert(
                    cursor, topic, mps,
                    mean_mps, drop_percent, severity
                )
        else:
            if active:
                resolve_alert(cursor, active[0])
                print(f'  RESOLVED throughput alert for {topic}')

        baseline_label = (
            f'{round(mean_mps, 4)} mps (hour={hour_used})'
            if hour_used is not None
            else f'{round(mean_mps, 4) if mean_mps else "building..."} mps (global fallback)'
        )

        trend = (
            'up'     if mean_mps and mps > mean_mps
            else 'down'   if mean_mps and mps < mean_mps
            else 'stable'
        )

        print(f'  {topic}')
        print(f'    Messages in window : {messages_in_window}')
        print(f'    Throughput         : {mps} msg/sec')
        print(f'    Peak               : {peak_mps} msg/sec')
        print(f'    Baseline           : {baseline_label}')
        print(f'    Samples            : {sample_count}')
        print(f'    Trend              : {trend}')
        print(f'    Status             : {severity if severity else "OK"}')

    previous_counts = dict(current_counts)

    conn.close()
    print(f'{"="*55}\n')


scheduler = BlockingScheduler()
scheduler.add_job(
    run_throughput_monitor,
    trigger='interval',
    minutes=1,
    next_run_time=datetime.now()
)

print('Throughput monitor started -- runs every 1 minute. Press Ctrl+C to stop.')
scheduler.start()