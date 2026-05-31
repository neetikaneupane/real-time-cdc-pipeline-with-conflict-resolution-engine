import psycopg2
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler
import json

# ─── Database Connection ───────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        user='debezium',
        password='debezium',
        dbname='destination_db'
    )


# ─── Adaptive Window Sizing ────────────────────────────────
def compute_adaptive_window(cursor, table_name, metric_type):
    """
    Adjust window size based on recent data volume.
    More data = shorter window (react faster)
    Less data = longer window (more stable)
    """
    cursor.execute("""
        SELECT COUNT(*)
        FROM conflict_metrics
        WHERE table_name = %s
        AND recorded_at > NOW() - INTERVAL '60 minutes'
    """, (table_name,))
    recent_count = cursor.fetchone()[0]

    if recent_count >= 50:
        window = 30   # high traffic — short window
    elif recent_count >= 20:
        window = 60   # medium traffic — standard window
    elif recent_count >= 5:
        window = 120  # low traffic — longer window
    else:
        window = 240  # very low traffic — maximum window

    return window


# ─── Compute Metric Baseline ──────────────────────────────
def compute_metric_baseline(cursor, table_name,
                             metric_type, window_minutes):
    """
    Compute baseline stats for a specific metric type.
    Excludes last 10 minutes to prevent burst inflation.
    """
    if metric_type == 'conflict_rate':
        cursor.execute("""
            WITH per_minute AS (
                SELECT
                    DATE_TRUNC('minute', recorded_at) AS minute,
                    COUNT(*) AS cnt
                FROM conflict_metrics
                WHERE event_type = 'conflict_detected'
                AND table_name = %s
                AND recorded_at > NOW() - INTERVAL '%s minutes'
                AND recorded_at < NOW() - INTERVAL '10 minutes'
                GROUP BY DATE_TRUNC('minute', recorded_at)
            )
            SELECT
                COALESCE(AVG(cnt), 0),
                COALESCE(STDDEV(cnt), 0),
                COALESCE(PERCENTILE_CONT(0.50)
                    WITHIN GROUP (ORDER BY cnt), 0),
                COALESCE(PERCENTILE_CONT(0.75)
                    WITHIN GROUP (ORDER BY cnt), 0),
                COALESCE(PERCENTILE_CONT(0.95)
                    WITHIN GROUP (ORDER BY cnt), 0),
                COALESCE(PERCENTILE_CONT(0.99)
                    WITHIN GROUP (ORDER BY cnt), 0),
                COUNT(*) AS sample_count
            FROM per_minute
        """, (table_name, window_minutes))

    elif metric_type == 'resolution_time':
        cursor.execute("""
            SELECT
                COALESCE(AVG(resolution_ms), 0),
                COALESCE(STDDEV(resolution_ms), 0),
                COALESCE(PERCENTILE_CONT(0.50)
                    WITHIN GROUP (ORDER BY resolution_ms), 0),
                COALESCE(PERCENTILE_CONT(0.75)
                    WITHIN GROUP (ORDER BY resolution_ms), 0),
                COALESCE(PERCENTILE_CONT(0.95)
                    WITHIN GROUP (ORDER BY resolution_ms), 0),
                COALESCE(PERCENTILE_CONT(0.99)
                    WITHIN GROUP (ORDER BY resolution_ms), 0),
                COUNT(*) AS sample_count
            FROM conflict_metrics
            WHERE table_name = %s
            AND resolution_ms IS NOT NULL
            AND recorded_at > NOW() - INTERVAL '%s minutes'
            AND recorded_at < NOW() - INTERVAL '10 minutes'
        """, (table_name, window_minutes))

    elif metric_type == 'source_lag':
        cursor.execute("""
            SELECT
                COALESCE(AVG(source_lag_ms), 0),
                COALESCE(STDDEV(source_lag_ms), 0),
                COALESCE(PERCENTILE_CONT(0.50)
                    WITHIN GROUP (ORDER BY source_lag_ms), 0),
                COALESCE(PERCENTILE_CONT(0.75)
                    WITHIN GROUP (ORDER BY source_lag_ms), 0),
                COALESCE(PERCENTILE_CONT(0.95)
                    WITHIN GROUP (ORDER BY source_lag_ms), 0),
                COALESCE(PERCENTILE_CONT(0.99)
                    WITHIN GROUP (ORDER BY source_lag_ms), 0),
                COUNT(*) AS sample_count
            FROM conflict_metrics
            WHERE table_name = %s
            AND source_lag_ms IS NOT NULL
            AND recorded_at > NOW() - INTERVAL '%s minutes'
            AND recorded_at < NOW() - INTERVAL '10 minutes'
        """, (table_name, window_minutes))

    elif metric_type == 'dlq_rate':
        cursor.execute("""
            WITH per_minute AS (
                SELECT
                    DATE_TRUNC('minute', failed_at) AS minute,
                    COUNT(*) AS cnt
                FROM dead_letter_queue
                WHERE failed_at > NOW() - INTERVAL '%s minutes'
                AND failed_at < NOW() - INTERVAL '10 minutes'
                GROUP BY DATE_TRUNC('minute', failed_at)
            )
            SELECT
                COALESCE(AVG(cnt), 0),
                COALESCE(STDDEV(cnt), 0),
                COALESCE(PERCENTILE_CONT(0.50)
                    WITHIN GROUP (ORDER BY cnt), 0),
                COALESCE(PERCENTILE_CONT(0.75)
                    WITHIN GROUP (ORDER BY cnt), 0),
                COALESCE(PERCENTILE_CONT(0.95)
                    WITHIN GROUP (ORDER BY cnt), 0),
                COALESCE(PERCENTILE_CONT(0.99)
                    WITHIN GROUP (ORDER BY cnt), 0),
                COUNT(*) AS sample_count
            FROM per_minute
        """, (window_minutes,))

    row = cursor.fetchone()
    if not row:
        return None

    return {
        'mean':         round(float(row[0]), 4),
        'stddev':       round(float(row[1]), 4),
        'p50':          round(float(row[2]), 4),
        'p75':          round(float(row[3]), 4),
        'p95':          round(float(row[4]), 4),
        'p99':          round(float(row[5]), 4),
        'sample_count': int(row[6])
    }


# ─── Confidence Scoring ────────────────────────────────────
def compute_confidence(stats, window_minutes):
    """
    Score baseline reliability from 0-100.
    Factors: sample count, variance stability, window size.
    """
    if not stats or stats['sample_count'] == 0:
        return 0.0, "No data available"

    score  = 0.0
    reason = []

    # sample count score (0-40 points)
    count = stats['sample_count']
    if count >= 30:
        score += 40
    elif count >= 20:
        score += 30
        reason.append(f"only {count} samples")
    elif count >= 10:
        score += 20
        reason.append(f"low sample count ({count})")
    elif count >= 5:
        score += 10
        reason.append(f"very low sample count ({count})")
    else:
        score += 5
        reason.append(f"insufficient samples ({count})")

    # variance stability score (0-30 points)
    if stats['mean'] > 0:
        cv = stats['stddev'] / stats['mean']  # coefficient of variation
        if cv <= 0.3:
            score += 30
        elif cv <= 0.5:
            score += 20
            reason.append("moderate variance")
        elif cv <= 1.0:
            score += 10
            reason.append("high variance")
        else:
            score += 0
            reason.append("very high variance — baseline unstable")
    else:
        score += 30  # zero mean is stable

    # window size score (0-30 points)
    if window_minutes >= 120:
        score += 30
    elif window_minutes >= 60:
        score += 20
    elif window_minutes >= 30:
        score += 10
        reason.append("short window")
    else:
        score += 5
        reason.append("very short window")

    reason_str = "; ".join(reason) if reason else "Good baseline"
    return round(score, 2), reason_str


# ─── Drift Detection ──────────────────────────────────────
def detect_drift(cursor, table_name, metric_type, current_stats):
    """
    Compare current baseline against 24 hours ago.
    Flag if mean has drifted more than 50%.
    """
    cursor.execute("""
        SELECT mean_per_minute
        FROM conflict_baselines
        WHERE table_name = %s
        AND computed_at < NOW() - INTERVAL '23 hours'
        AND computed_at > NOW() - INTERVAL '25 hours'
        ORDER BY computed_at DESC
        LIMIT 1
    """, (table_name,))
    row = cursor.fetchone()

    if not row or current_stats['mean'] == 0:
        return None

    old_mean     = float(row[0])
    new_mean     = current_stats['mean']

    if old_mean == 0:
        return None

    drift_percent = ((new_mean - old_mean) / old_mean) * 100
    direction     = 'UP' if drift_percent > 0 else 'DOWN'

    # flag if drift exceeds 50%
    if abs(drift_percent) >= 50:
        return {
            'old_mean':      old_mean,
            'new_mean':      new_mean,
            'drift_percent': round(drift_percent, 2),
            'direction':     direction
        }
    return None


# ─── Log Drift Event ──────────────────────────────────────
def log_drift_event(cursor, table_name, metric_type, drift):
    # check if already logged recently
    cursor.execute("""
        SELECT id FROM baseline_drift_events
        WHERE table_name = %s
        AND metric_type = %s
        AND detected_at > NOW() - INTERVAL '1 hour'
        AND resolved = false
    """, (table_name, metric_type))

    if cursor.fetchone():
        return  # already logged

    cursor.execute("""
        INSERT INTO baseline_drift_events
            (table_name, metric_type, old_mean, new_mean,
             drift_percent, drift_direction, detected_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """, (
        table_name, metric_type,
        drift['old_mean'], drift['new_mean'],
        drift['drift_percent'], drift['direction']
    ))
    print(f"  [DRIFT] {metric_type} drifted {drift['direction']} "
          f"{drift['drift_percent']}% "
          f"({drift['old_mean']} → {drift['new_mean']})")


# ─── Save Baseline ────────────────────────────────────────
def save_baseline(cursor, table_name, stats, window_minutes):
    cursor.execute("""
        INSERT INTO conflict_baselines
            (computed_at, table_name, window_minutes,
             mean_per_minute, stddev_per_minute,
             p95_per_minute, p99_per_minute, sample_count)
        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s)
    """, (
        table_name, window_minutes,
        stats['mean'], stats['stddev'],
        stats['p95'], stats['p99'],
        stats['sample_count']
    ))


# ─── Save Percentile Trend ────────────────────────────────
def save_percentile_trend(cursor, table_name,
                          metric_type, stats, window_minutes):
    cursor.execute("""
        INSERT INTO percentile_trends
            (table_name, metric_type, p50, p75,
             p95, p99, window_minutes, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    """, (
        table_name, metric_type,
        stats['p50'], stats['p75'],
        stats['p95'], stats['p99'],
        window_minutes
    ))


# ─── Save Confidence ──────────────────────────────────────
def save_confidence(cursor, table_name, metric_type,
                    confidence, sample_count,
                    window_minutes, reason):
    cursor.execute("""
        INSERT INTO baseline_confidence
            (table_name, metric_type, confidence_score,
             sample_count, window_minutes, reason, computed_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """, (
        table_name, metric_type,
        confidence, sample_count,
        window_minutes, reason
    ))


# ─── Compute Overall Health ───────────────────────────────
def compute_overall_health(cursor, table_name, all_results):
    """
    Compute overall baseline health score for a table.
    """
    confidences = [r['confidence'] for r in all_results.values()
                   if r.get('confidence') is not None]

    if not confidences:
        return

    avg_confidence    = sum(confidences) / len(confidences)
    drift_detected    = any(r.get('drift') for r in all_results.values())
    low_confidence    = [m for m, r in all_results.items()
                         if r.get('confidence', 100) < 50]

    if avg_confidence >= 80 and not drift_detected:
        health = 'HEALTHY'
        recommendation = 'Baseline is reliable'
    elif avg_confidence >= 60:
        health = 'DEGRADED'
        recommendation = ('Increase traffic volume for '
                         'better baseline reliability')
    else:
        health = 'UNHEALTHY'
        recommendation = ('Insufficient data — '
                         'baselines may be unreliable')

    if drift_detected:
        recommendation += '. Drift detected — review system changes'

    cursor.execute("""
        INSERT INTO baseline_health
            (table_name, overall_health, avg_confidence,
             drift_detected, low_confidence_metrics,
             recommendation, computed_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """, (
        table_name, health,
        round(avg_confidence, 2), drift_detected,
        json.dumps(low_confidence), recommendation
    ))

    return health, avg_confidence


# ─── Main Computation Job ─────────────────────────────────
def run_baseline_computation():
    print(f"\n{'='*55}")
    print(f"BASELINE COMPUTATION — "
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

    metric_types = [
        'conflict_rate',
        'resolution_time',
        'source_lag',
        'dlq_rate'
    ]

    for table_name in tables:
        print(f"\nTable: {table_name}")
        all_results = {}

        # compute adaptive window
        window = compute_adaptive_window(
            cursor, table_name, 'conflict_rate'
        )
        print(f"  Adaptive window : {window} minutes")

        for metric_type in metric_types:
            stats = compute_metric_baseline(
                cursor, table_name, metric_type, window
            )

            if not stats or stats['sample_count'] == 0:
                print(f"  {metric_type:20} — no data")
                all_results[metric_type] = {}
                continue

            # compute confidence
            confidence, reason = compute_confidence(stats, window)

            # detect drift
            drift = None
            if metric_type == 'conflict_rate':
                drift = detect_drift(
                    cursor, table_name, metric_type, stats
                )
                if drift:
                    log_drift_event(
                        cursor, table_name, metric_type, drift
                    )

            # save everything
            if metric_type == 'conflict_rate':
                save_baseline(cursor, table_name, stats, window)

            save_percentile_trend(
                cursor, table_name, metric_type, stats, window
            )
            save_confidence(
                cursor, table_name, metric_type,
                confidence, stats['sample_count'], window, reason
            )

            all_results[metric_type] = {
                'confidence': confidence,
                'drift':      drift
            }

            confidence_bar = '█' * int(confidence / 10)
            print(f"  {metric_type:20} "
                  f"mean={stats['mean']:8.3f}  "
                  f"p95={stats['p95']:8.3f}  "
                  f"samples={stats['sample_count']:3d}  "
                  f"confidence={confidence:5.1f}% "
                  f"[{confidence_bar:<10}]"
                  f"{'   DRIFT' if drift else ''}")

        # compute overall health
        result = compute_overall_health(
            cursor, table_name, all_results
        )
        if result:
            health, avg_conf = result
            print(f"\n  Overall health  : {health} "
                  f"(avg confidence={avg_conf:.1f}%)")

    conn.close()
    print(f"\n{'='*55}\n")


# ─── Scheduler ────────────────────────────────────────────
scheduler = BlockingScheduler()
scheduler.add_job(
    run_baseline_computation,
    trigger='interval',
    minutes=5,
    next_run_time=datetime.now()
)

print("Baseline computer started — "
      "runs every 5 minutes. Press Ctrl+C to stop.")
scheduler.start()