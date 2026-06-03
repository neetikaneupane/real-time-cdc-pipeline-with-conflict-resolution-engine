import psycopg2
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler

LAG_WEIGHT        = 40
DLQ_WEIGHT        = 20
CONFLICT_WEIGHT   = 20
RESOLUTION_WEIGHT = 20

LAG_WARNING_THRESHOLD  = 100
LAG_CRITICAL_THRESHOLD = 500

DLQ_WARNING_THRESHOLD  = 5
DLQ_CRITICAL_THRESHOLD = 20

RESOLUTION_WARNING_MS  = 500
RESOLUTION_CRITICAL_MS = 2000


def get_conn():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        user='debezium',
        password='debezium',
        dbname='destination_db'
    )


def compute_lag_score(cursor):
    cursor.execute("""
        SELECT topic, partition_id, lag
        FROM (
            SELECT DISTINCT ON (topic, partition_id)
                topic, partition_id, lag
            FROM consumer_lag
            ORDER BY topic, partition_id, recorded_at DESC
        ) latest
    """)
    rows = cursor.fetchall()

    if not rows:
        return LAG_WEIGHT, 'No lag data available -- assuming healthy'

    total_lag = sum(r[2] for r in rows)
    max_lag   = max(r[2] for r in rows)

    if max_lag == 0:
        score  = LAG_WEIGHT
        detail = f'All topics at lag=0'
    elif max_lag >= LAG_CRITICAL_THRESHOLD:
        score  = 0
        detail = f'Critical lag detected -- max lag={max_lag}'
    elif max_lag >= LAG_WARNING_THRESHOLD:
        ratio  = 1 - (max_lag - LAG_WARNING_THRESHOLD) / (LAG_CRITICAL_THRESHOLD - LAG_WARNING_THRESHOLD)
        score  = round(LAG_WEIGHT * ratio * 0.5, 2)
        detail = f'Warning lag detected -- max lag={max_lag}'
    else:
        ratio  = 1 - (max_lag / LAG_WARNING_THRESHOLD)
        score  = round(LAG_WEIGHT * ratio, 2)
        detail = f'Low lag -- max lag={max_lag}'

    return score, detail


def compute_dlq_score(cursor):
    cursor.execute("""
        SELECT COUNT(*) FROM dead_letter_queue
        WHERE resolved = false
    """)
    unresolved = cursor.fetchone()[0]

    if unresolved == 0:
        score  = DLQ_WEIGHT
        detail = 'DLQ is empty'
    elif unresolved >= DLQ_CRITICAL_THRESHOLD:
        score  = 0
        detail = f'Critical -- {unresolved} unresolved DLQ items'
    elif unresolved >= DLQ_WARNING_THRESHOLD:
        ratio  = 1 - (unresolved - DLQ_WARNING_THRESHOLD) / (DLQ_CRITICAL_THRESHOLD - DLQ_WARNING_THRESHOLD)
        score  = round(DLQ_WEIGHT * ratio * 0.5, 2)
        detail = f'Warning -- {unresolved} unresolved DLQ items'
    else:
        ratio  = 1 - (unresolved / DLQ_WARNING_THRESHOLD)
        score  = round(DLQ_WEIGHT * ratio, 2)
        detail = f'{unresolved} unresolved DLQ items'

    return score, detail


def compute_conflict_score(cursor):
    cursor.execute("""
        SELECT COUNT(*) FROM conflict_metrics
        WHERE recorded_at > NOW() - INTERVAL '5 minutes'
        AND event_type = 'conflict_detected'
    """)
    recent_conflicts = cursor.fetchone()[0]
    current_rate     = round(recent_conflicts / 5.0, 4)

    cursor.execute("""
        SELECT mean_per_minute, stddev_per_minute
        FROM conflict_baselines
        ORDER BY computed_at DESC
        LIMIT 1
    """)
    row = cursor.fetchone()

    if not row:
        score  = CONFLICT_WEIGHT
        detail = f'No baseline yet -- current rate={current_rate}/min'
        return score, detail

    mean   = float(row[0])
    stddev = float(row[1])

    if stddev == 0:
        score  = CONFLICT_WEIGHT
        detail = f'Stable -- rate={current_rate}/min baseline={round(mean, 4)}/min'
        return score, detail

    deviation = (current_rate - mean) / stddev

    if deviation >= 3:
        score  = 0
        detail = f'Critical spike -- rate={current_rate}/min is {round(deviation, 2)} stddevs above baseline'
    elif deviation >= 2:
        score  = round(CONFLICT_WEIGHT * 0.5, 2)
        detail = f'Elevated -- rate={current_rate}/min is {round(deviation, 2)} stddevs above baseline'
    else:
        score  = CONFLICT_WEIGHT
        detail = f'Normal -- rate={current_rate}/min baseline={round(mean, 4)}/min'

    return score, detail


def compute_resolution_score(cursor):
    cursor.execute("""
        SELECT AVG(resolution_ms)
        FROM conflict_metrics
        WHERE recorded_at > NOW() - INTERVAL '10 minutes'
        AND resolution_ms IS NOT NULL
    """)
    row = cursor.fetchone()

    if not row or row[0] is None:
        score  = RESOLUTION_WEIGHT
        detail = 'No recent resolution data -- assuming healthy'
        return score, detail

    avg_ms = float(row[0])

    if avg_ms >= RESOLUTION_CRITICAL_MS:
        score  = 0
        detail = f'Critical -- avg resolution time={round(avg_ms)}ms'
    elif avg_ms >= RESOLUTION_WARNING_MS:
        ratio  = 1 - (avg_ms - RESOLUTION_WARNING_MS) / (RESOLUTION_CRITICAL_MS - RESOLUTION_WARNING_MS)
        score  = round(RESOLUTION_WEIGHT * ratio * 0.5, 2)
        detail = f'Slow -- avg resolution time={round(avg_ms)}ms'
    else:
        score  = RESOLUTION_WEIGHT
        detail = f'Fast -- avg resolution time={round(avg_ms)}ms'

    return score, detail


def compute_health_status(score):
    if score >= 80:
        return 'HEALTHY'
    elif score >= 60:
        return 'DEGRADED'
    else:
        return 'UNHEALTHY'


def run_health_monitor():
    print(f'\n{"="*50}')
    print(f'HEALTH MONITOR -- {datetime.now(timezone.utc).isoformat()}')
    print(f'{"="*50}')

    conn   = get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    lag_score,        lag_detail        = compute_lag_score(cursor)
    dlq_score,        dlq_detail        = compute_dlq_score(cursor)
    conflict_score,   conflict_detail   = compute_conflict_score(cursor)
    resolution_score, resolution_detail = compute_resolution_score(cursor)

    overall_score = round(
        lag_score + dlq_score + conflict_score + resolution_score, 2
    )
    health_status = compute_health_status(overall_score)

    cursor.execute("""
        INSERT INTO pipeline_health
            (recorded_at, overall_score, health_status,
             lag_score, dlq_score, conflict_score, resolution_score,
             lag_detail, dlq_detail, conflict_detail, resolution_detail)
        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        overall_score, health_status,
        lag_score, dlq_score, conflict_score, resolution_score,
        lag_detail, dlq_detail, conflict_detail, resolution_detail
    ))

    print(f'  Overall score    : {overall_score} / 100')
    print(f'  Status           : {health_status}')
    print(f'  Lag score        : {lag_score} / {LAG_WEIGHT}  -- {lag_detail}')
    print(f'  DLQ score        : {dlq_score} / {DLQ_WEIGHT}  -- {dlq_detail}')
    print(f'  Conflict score   : {conflict_score} / {CONFLICT_WEIGHT}  -- {conflict_detail}')
    print(f'  Resolution score : {resolution_score} / {RESOLUTION_WEIGHT}  -- {resolution_detail}')

    conn.close()
    print(f'{"="*50}\n')


scheduler = BlockingScheduler()
scheduler.add_job(
    run_health_monitor,
    trigger='interval',
    minutes=1,
    next_run_time=datetime.now()
)

print('Health monitor started -- runs every 1 minute. Press Ctrl+C to stop.')
scheduler.start()
