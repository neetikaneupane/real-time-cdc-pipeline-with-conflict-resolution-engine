import psycopg2
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler

# ─── Database Connection ───────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        user='debezium',
        password='debezium',
        dbname='destination_db'
    )


# ─── Get Latest Baseline ───────────────────────────────────
def get_latest_baseline(cursor, table_name):
    # Use baseline computed at least 5 minutes ago
    # so recent bursts don't immediately inflate the baseline
    cursor.execute("""
        SELECT mean_per_minute, stddev_per_minute, computed_at
        FROM conflict_baselines
        WHERE table_name = %s
        AND computed_at < NOW() - INTERVAL '5 minutes'
        ORDER BY computed_at DESC
        LIMIT 1
    """, (table_name,))
    row = cursor.fetchone()
    if not row:
        # fall back to latest if nothing older exists
        cursor.execute("""
            SELECT mean_per_minute, stddev_per_minute, computed_at
            FROM conflict_baselines
            WHERE table_name = %s
            ORDER BY computed_at DESC
            LIMIT 1
        """, (table_name,))
        return cursor.fetchone()
    return row


# ─── Get Current Rate ──────────────────────────────────────
def get_current_rate(cursor, table_name, window_minutes=5):
    cursor.execute("""
        SELECT COUNT(*) as conflict_count
        FROM conflict_metrics
        WHERE recorded_at > NOW() - INTERVAL '%s minutes'
        AND event_type = 'conflict_detected'
        AND table_name = %s
    """, (window_minutes, table_name))
    row = cursor.fetchone()
    total = float(row[0])
    # convert to per-minute rate
    return round(total / window_minutes, 4)


# ─── Get Active Anomaly ────────────────────────────────────
def get_active_anomaly(cursor, table_name):
    cursor.execute("""
        SELECT id, severity, started_at, peak_rate
        FROM conflict_anomalies
        WHERE table_name = %s
        AND resolved = false
        ORDER BY started_at DESC
        LIMIT 1
    """, (table_name,))
    return cursor.fetchone()


# ─── Classify Severity ─────────────────────────────────────
def classify_severity(current_rate, mean, stddev):
    if stddev == 0:
        return None, 0

    deviation_score = (current_rate - mean) / stddev

    if deviation_score >= 3:
        return 'CRITICAL', deviation_score
    elif deviation_score >= 2:
        return 'WARNING', deviation_score
    else:
        return None, deviation_score


# ─── Main Detection Job ────────────────────────────────────
def run_anomaly_detection():
    print(f"\n{'='*50}")
    print(f"ANOMALY DETECTION — {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*50}")

    conn = get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    # get all distinct tables
    cursor.execute("SELECT DISTINCT table_name FROM conflict_metrics")
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        print("No metrics found yet.")
        conn.close()
        return

    for table_name in tables:
        print(f"\nTable: {table_name}")

        # get latest baseline
        baseline = get_latest_baseline(cursor, table_name)
        if not baseline:
            print(f"  No baseline yet — skipping")
            continue

        mean, stddev, computed_at = baseline
        current_rate = get_current_rate(cursor, table_name)
        severity, deviation_score = classify_severity(current_rate, mean, stddev)

        print(f"  Current rate    : {current_rate} conflicts/min")
        print(f"  Baseline mean   : {round(mean, 4)} conflicts/min")
        print(f"  Stddev          : {round(stddev, 4)}")
        print(f"  Deviation score : {round(deviation_score, 4)} stddevs")
        print(f"  Warning at      : {round(mean + 2*stddev, 4)}")
        print(f"  Critical at     : {round(mean + 3*stddev, 4)}")

        active_anomaly = get_active_anomaly(cursor, table_name)

        if severity:
            print(f"  ⚠️  ANOMALY DETECTED — {severity}")

            if active_anomaly:
                # update existing anomaly
                anomaly_id   = active_anomaly[0]
                current_peak = active_anomaly[3]
                new_peak     = max(current_rate, current_peak)

                cursor.execute("""
                    UPDATE conflict_anomalies
                    SET severity         = %s,
                        peak_rate        = %s,
                        deviation_score  = %s,
                        duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::INT
                    WHERE id = %s
                """, (severity, new_peak, deviation_score, anomaly_id))
                print(f"  Updated existing anomaly id={anomaly_id}")

            else:
                # create new anomaly
                cursor.execute("""
                    INSERT INTO conflict_anomalies
                        (table_name, severity, started_at, peak_rate,
                         baseline_mean, baseline_stddev, deviation_score)
                    VALUES (%s, %s, NOW(), %s, %s, %s, %s)
                """, (table_name, severity, current_rate, mean, stddev, deviation_score))
                print(f"  New anomaly recorded")

        else:
            print(f"  ✓ Normal — no anomaly")

            if active_anomaly:
                # resolve the active anomaly
                anomaly_id = active_anomaly[0]
                cursor.execute("""
                    UPDATE conflict_anomalies
                    SET resolved         = true,
                        ended_at         = NOW(),
                        duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::INT
                    WHERE id = %s
                """, (anomaly_id,))
                print(f"  Anomaly id={anomaly_id} resolved")

    conn.close()
    print(f"\n{'='*50}\n")


# ─── Scheduler ─────────────────────────────────────────────
scheduler = BlockingScheduler()
scheduler.add_job(
    run_anomaly_detection,
    trigger='interval',
    minutes=1,
    next_run_time=datetime.now()
)

print("Anomaly detector started — runs every 1 minute. Press Ctrl+C to stop.")
scheduler.start()
