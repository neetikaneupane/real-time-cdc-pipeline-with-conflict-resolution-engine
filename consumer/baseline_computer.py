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


# ─── Compute Baseline for One Table ───────────────────────
def compute_baseline(cursor, table_name, window_minutes=60):
    # Step 1 — get conflicts per minute for the window
    cursor.execute("""
        WITH per_minute AS (
            SELECT
                DATE_TRUNC('minute', recorded_at) AS minute,
                COUNT(*) AS conflict_count
            FROM conflict_metrics
            WHERE recorded_at > NOW() - INTERVAL '%s minutes'
            AND event_type = 'conflict_detected'
            AND table_name = %s
            GROUP BY DATE_TRUNC('minute', recorded_at)
        )
        SELECT
            COALESCE(AVG(conflict_count), 0)    AS mean,
            COALESCE(STDDEV(conflict_count), 0) AS stddev,
            COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY conflict_count), 0) AS p95,
            COALESCE(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY conflict_count), 0) AS p99,
            COUNT(*) AS sample_count
        FROM per_minute
    """, (window_minutes, table_name))

    row = cursor.fetchone()
    mean         = float(row[0])
    stddev       = float(row[1])
    p95          = float(row[2])
    p99          = float(row[3])
    sample_count = int(row[4])

    # Step 2 — store the baseline
    cursor.execute("""
        INSERT INTO conflict_baselines
            (computed_at, table_name, window_minutes,
             mean_per_minute, stddev_per_minute,
             p95_per_minute, p99_per_minute, sample_count)
        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s)
    """, (table_name, window_minutes, mean, stddev, p95, p99, sample_count))

    return {
        'table_name':   table_name,
        'mean':         round(mean, 4),
        'stddev':       round(stddev, 4),
        'p95':          round(p95, 4),
        'p99':          round(p99, 4),
        'sample_count': sample_count
    }


# ─── Main Job ──────────────────────────────────────────────
def run_baseline_computation():
    print(f"\n{'='*50}")
    print(f"BASELINE COMPUTATION — {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*50}")

    conn = get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    # Get all distinct tables from conflict_metrics
    cursor.execute("""
        SELECT DISTINCT table_name FROM conflict_metrics
    """)
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        print("No metrics found yet — skipping baseline computation")
        conn.close()
        return

    for table in tables:
        result = compute_baseline(cursor, table, window_minutes=120)
        print(f"\nTable: {result['table_name']}")
        print(f"  Mean conflicts/min : {result['mean']}")
        print(f"  Stddev             : {result['stddev']}")
        print(f"  P95                : {result['p95']}")
        print(f"  P99                : {result['p99']}")
        print(f"  Sample count       : {result['sample_count']} minutes")
        print(f"  Warning threshold  : {round(result['mean'] + 2 * result['stddev'], 4)}")
        print(f"  Critical threshold : {round(result['mean'] + 3 * result['stddev'], 4)}")

    conn.close()
    print(f"\n{'='*50}\n")


# ─── Scheduler ─────────────────────────────────────────────
scheduler = BlockingScheduler()
scheduler.add_job(
    run_baseline_computation,
    trigger='interval',
    seconds=30,
    next_run_time=datetime.now()
)

print("Baseline computer started — runs every 5 minutes. Press Ctrl+C to stop.")
scheduler.start()