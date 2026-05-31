import psycopg2
import json
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


# ─── Get Signal Value ──────────────────────────────────────
def get_signal_value(cursor, table_name, signal_type, window_minutes=5):
    """
    Compute current value for each signal type.
    """
    if signal_type == 'conflict_rate':
        cursor.execute("""
            SELECT COUNT(*) FROM conflict_metrics
            WHERE recorded_at > NOW() - INTERVAL '%s minutes'
            AND event_type = 'conflict_detected'
            AND table_name = %s
        """, (window_minutes, table_name))
        total = float(cursor.fetchone()[0])
        return round(total / window_minutes, 4)

    elif signal_type == 'resolution_time':
        cursor.execute("""
            SELECT COALESCE(AVG(resolution_ms), 0)
            FROM conflict_metrics
            WHERE recorded_at > NOW() - INTERVAL '%s minutes'
            AND event_type = 'conflict_detected'
            AND table_name = %s
            AND resolution_ms IS NOT NULL
        """, (window_minutes, table_name))
        return round(float(cursor.fetchone()[0]), 4)

    elif signal_type == 'dlq_growth_rate':
        cursor.execute("""
            SELECT COUNT(*) FROM dead_letter_queue
            WHERE failed_at > NOW() - INTERVAL '%s minutes'
            AND resolved = false
        """, (window_minutes,))
        total = float(cursor.fetchone()[0])
        return round(total / window_minutes, 4)

    elif signal_type == 'source_lag':
        cursor.execute("""
            SELECT COALESCE(AVG(source_lag_ms), 0)
            FROM conflict_metrics
            WHERE recorded_at > NOW() - INTERVAL '%s minutes'
            AND table_name = %s
            AND source_lag_ms IS NOT NULL
        """, (window_minutes, table_name))
        return round(float(cursor.fetchone()[0]), 4)

    return 0.0


# ─── Get Hourly Baseline ───────────────────────────────────
def get_hourly_baseline(cursor, table_name, signal_type):
    """
    Get baseline for the current hour of day.
    Falls back to global baseline if no hourly baseline exists.
    """
    current_hour = datetime.now(timezone.utc).hour

    cursor.execute("""
        SELECT mean, stddev, sample_count
        FROM hourly_baselines
        WHERE table_name = %s
        AND signal_type = %s
        AND hour_of_day = %s
        AND computed_at < NOW() - INTERVAL '5 minutes'
        ORDER BY computed_at DESC
        LIMIT 1
    """, (table_name, signal_type, current_hour))
    row = cursor.fetchone()

    if row and row[2] >= 3:  # need at least 3 samples
        return float(row[0]), float(row[1])

    # fall back to global baseline
    cursor.execute("""
        SELECT mean_per_minute, stddev_per_minute
        FROM conflict_baselines
        WHERE table_name = %s
        ORDER BY computed_at DESC
        LIMIT 1
    """, (table_name,))
    row = cursor.fetchone()

    if row:
        return float(row[0]), float(row[1])

    return None, None


# ─── Compute Hourly Baselines ──────────────────────────────
def compute_hourly_baselines(cursor, table_name):
    """
    Compute per-hour-of-day baselines for all signal types.
    """
    signal_types = [
        'conflict_rate',
        'resolution_time',
        'source_lag'
    ]

    for signal_type in signal_types:
        for hour in range(24):
            if signal_type == 'conflict_rate':
                cursor.execute("""
                    WITH per_minute AS (
                        SELECT
                            DATE_TRUNC('minute', recorded_at) AS minute,
                            COUNT(*) AS cnt
                        FROM conflict_metrics
                        WHERE event_type = 'conflict_detected'
                        AND table_name = %s
                        AND EXTRACT(HOUR FROM recorded_at) = %s
                        AND recorded_at < NOW() - INTERVAL '10 minutes'
                        GROUP BY DATE_TRUNC('minute', recorded_at)
                    )
                    SELECT
                        COALESCE(AVG(cnt), 0),
                        COALESCE(STDDEV(cnt), 0),
                        COUNT(*)
                    FROM per_minute
                """, (table_name, hour))

            elif signal_type == 'resolution_time':
                cursor.execute("""
                    SELECT
                        COALESCE(AVG(resolution_ms), 0),
                        COALESCE(STDDEV(resolution_ms), 0),
                        COUNT(*)
                    FROM conflict_metrics
                    WHERE event_type = 'conflict_detected'
                    AND table_name = %s
                    AND resolution_ms IS NOT NULL
                    AND EXTRACT(HOUR FROM recorded_at) = %s
                    AND recorded_at < NOW() - INTERVAL '10 minutes'
                """, (table_name, hour))

            elif signal_type == 'source_lag':
                cursor.execute("""
                    SELECT
                        COALESCE(AVG(source_lag_ms), 0),
                        COALESCE(STDDEV(source_lag_ms), 0),
                        COUNT(*)
                    FROM conflict_metrics
                    WHERE table_name = %s
                    AND source_lag_ms IS NOT NULL
                    AND EXTRACT(HOUR FROM recorded_at) = %s
                    AND recorded_at < NOW() - INTERVAL '10 minutes'
                """, (table_name, hour))

            row = cursor.fetchone()
            if row and int(row[2]) >= 2:
                cursor.execute("""
                    INSERT INTO hourly_baselines
                        (table_name, hour_of_day, signal_type,
                         mean, stddev, sample_count, computed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, (
                    table_name, hour, signal_type,
                    float(row[0]), float(row[1]), int(row[2])
                ))


# ─── Classify Severity ─────────────────────────────────────
def classify_deviation(value, mean, stddev):
    if stddev == 0:
        return 0.0, None
    deviation = (value - mean) / stddev
    if deviation >= 3:
        return deviation, 'CRITICAL'
    elif deviation >= 2:
        return deviation, 'WARNING'
    return deviation, None


# ─── Log Signal ───────────────────────────────────────────
def log_signal(cursor, table_name, signal_type, value,
               mean, stddev, deviation, severity):
    cursor.execute("""
        INSERT INTO anomaly_signals
            (recorded_at, table_name, signal_type, value,
             baseline_mean, baseline_stddev,
             deviation_score, severity)
        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s)
    """, (
        table_name, signal_type, value,
        mean, stddev, deviation, severity
    ))


# ─── Get Active Composite Anomaly ─────────────────────────
def get_active_composite(cursor, table_name):
    cursor.execute("""
        SELECT id, composite_score, peak_score, signals_fired
        FROM composite_anomalies
        WHERE table_name = %s AND resolved = false
        ORDER BY started_at DESC
        LIMIT 1
    """, (table_name,))
    return cursor.fetchone()


# ─── Compute Composite Score ───────────────────────────────
def compute_composite_score(signal_results):
    """
    Weighted average of deviation scores across all signals.
    conflict_rate has highest weight.
    """
    weights = {
        'conflict_rate':   0.5,
        'resolution_time': 0.25,
        'dlq_growth_rate': 0.15,
        'source_lag':      0.1
    }
    total_weight = 0
    weighted_sum = 0

    for signal_type, result in signal_results.items():
        deviation = result.get('deviation', 0)
        weight    = weights.get(signal_type, 0.1)
        weighted_sum  += abs(deviation) * weight
        total_weight  += weight

    return round(weighted_sum / total_weight, 4) if total_weight > 0 else 0


# ─── Main Detection Job ────────────────────────────────────
def run_multi_signal_detection():
    print(f"\n{'='*55}")
    print(f"MULTI-SIGNAL ANOMALY DETECTION — "
          f"{datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*55}")

    conn = get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    # get all tables
    cursor.execute(
        "SELECT DISTINCT table_name FROM conflict_metrics"
    )
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        print("No metrics found yet.")
        conn.close()
        return

    # compute hourly baselines every run
    for table_name in tables:
        compute_hourly_baselines(cursor, table_name)

    signal_types = [
        'conflict_rate',
        'resolution_time',
        'dlq_growth_rate',
        'source_lag'
    ]

    for table_name in tables:
        print(f"\nTable: {table_name}")
        signal_results  = {}
        signals_fired   = []

        for signal_type in signal_types:
            value = get_signal_value(cursor, table_name, signal_type)

            if signal_type == 'dlq_growth_rate':
                # use fixed thresholds for DLQ
                mean   = 0
                stddev = 0.5
            else:
                mean, stddev = get_hourly_baseline(
                    cursor, table_name, signal_type
                )
                if mean is None:
                    print(f"  {signal_type}: {value} "
                          f"(no baseline yet)")
                    continue

            deviation, severity = classify_deviation(
                value, mean, stddev
            )

            log_signal(cursor, table_name, signal_type,
                      value, mean, stddev, deviation, severity)

            signal_results[signal_type] = {
                'value':     value,
                'mean':      mean,
                'stddev':    stddev,
                'deviation': deviation,
                'severity':  severity
            }

            status = f"⚠️  {severity}" if severity else "✓"
            print(f"  {signal_type:20} "
                  f"current={value:8.3f}  "
                  f"mean={mean:8.3f}  "
                  f"dev={deviation:+.2f}σ  "
                  f"{status}")

            if severity:
                signals_fired.append(signal_type)

        # compute composite score
        composite_score = compute_composite_score(signal_results)
        print(f"\n  Composite score : {composite_score:.4f}")
        print(f"  Signals fired   : "
              f"{signals_fired if signals_fired else 'none'}")

        # determine composite severity
        if composite_score >= 2.5:
            composite_severity = 'CRITICAL'
        elif composite_score >= 1.5:
            composite_severity = 'WARNING'
        else:
            composite_severity = None

        active = get_active_composite(cursor, table_name)

        if composite_severity:
            print(f"  🚨 COMPOSITE ANOMALY — {composite_severity} "
                  f"(score={composite_score})")
            if active:
                new_peak = max(composite_score, active[2])
                cursor.execute("""
                    UPDATE composite_anomalies
                    SET composite_score  = %s,
                        peak_score       = %s,
                        severity         = %s,
                        signals_fired    = %s,
                        duration_seconds = EXTRACT(
                            EPOCH FROM (NOW() - started_at)
                        )::INT
                    WHERE id = %s
                """, (
                    composite_score, new_peak,
                    composite_severity,
                    json.dumps(signals_fired),
                    active[0]
                ))
                print(f"  Updated composite anomaly id={active[0]}")
            else:
                cursor.execute("""
                    INSERT INTO composite_anomalies
                        (table_name, started_at, composite_score,
                         severity, signals_fired, peak_score)
                    VALUES (%s, NOW(), %s, %s, %s, %s)
                """, (
                    table_name, composite_score,
                    composite_severity,
                    json.dumps(signals_fired),
                    composite_score
                ))
                print(f"  New composite anomaly recorded")
        else:
            print(f"  ✓ No composite anomaly")
            if active:
                cursor.execute("""
                    UPDATE composite_anomalies
                    SET resolved         = true,
                        ended_at         = NOW(),
                        duration_seconds = EXTRACT(
                            EPOCH FROM (NOW() - started_at)
                        )::INT
                    WHERE id = %s
                """, (active[0],))
                print(f"  Composite anomaly id={active[0]} resolved")

    conn.close()
    print(f"\n{'='*55}\n")


# ─── Scheduler ─────────────────────────────────────────────
scheduler = BlockingScheduler()
scheduler.add_job(
    run_multi_signal_detection,
    trigger='interval',
    minutes=1,
    next_run_time=datetime.now()
)

print("Multi-signal detector started — "
      "runs every 1 minute. Press Ctrl+C to stop.")
scheduler.start()