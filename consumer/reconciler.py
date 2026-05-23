from apscheduler.schedulers.blocking import BlockingScheduler
import psycopg2
from datetime import datetime

# ─── Connections ───────────────────────────────────────────
def get_conn(dbname):
    return psycopg2.connect(
        host='localhost',
        port=5432,
        user='debezium',
        password='debezium',
        dbname=dbname
    )

def get_mysql_conn():
    import pymysql
    return pymysql.connect(
        host='localhost',
        port=3307,
        user='debezium',
        password='debezium',
        database='source_eu'
    )


# ─── Row Count Audit ───────────────────────────────────────
def get_pg_count(conn, dbname):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM customers")
    return cur.fetchone()[0]

def get_mysql_count(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM customers")
    return cur.fetchone()[0]

def get_resolved_count(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM customers_resolved")
    return cur.fetchone()[0]


# ─── Checksum Audit ────────────────────────────────────────
def get_pg_checksum(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT MD5(STRING_AGG(customer_id, ',' ORDER BY customer_id))
        FROM customers
    """)
    return cur.fetchone()[0]

def get_resolved_checksum(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT MD5(STRING_AGG(customer_id, ',' ORDER BY customer_id))
        FROM customers_resolved
    """)
    return cur.fetchone()[0]


# ─── Quarantine Summary ────────────────────────────────────
def get_quarantine_summary(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) as total_conflicts,
            COUNT(DISTINCT customer_id) as unique_customers,
            MAX(detected_at) as last_conflict_at
        FROM customers_quarantine
    """)
    return cur.fetchone()


# ─── Main Reconciliation Job ───────────────────────────────
def run_reconciliation():
    print(f"\n{'='*50}")
    print(f"RECONCILIATION RUN — {datetime.utcnow().isoformat()}")
    print(f"{'='*50}")

    try:
        pg_us_conn   = get_conn('source_us')
        dest_conn    = get_conn('destination_db')

        # Row counts
        pg_count       = get_pg_count(pg_us_conn, 'source_us')
        resolved_count = get_resolved_count(dest_conn)

        print(f"\nROW COUNTS:")
        print(f"  PostgreSQL US : {pg_count}")
        print(f"  Resolved DB   : {resolved_count}")

        # Try MySQL count
        try:
            mysql_conn  = get_mysql_conn()
            mysql_count = get_mysql_count(mysql_conn)
            print(f"  MySQL EU      : {mysql_count}")
            mysql_conn.close()
        except Exception as e:
            print(f"  MySQL EU      : ERROR — {e}")

        # Checksum audit
        pg_checksum       = get_pg_checksum(pg_us_conn)
        resolved_checksum = get_resolved_checksum(dest_conn)

        print(f"\nCHECKSUMS:")
        print(f"  PostgreSQL US : {pg_checksum}")
        print(f"  Resolved DB   : {resolved_checksum}")

        cur = dest_conn.cursor()
        cur.execute("""
            SELECT customer_id FROM customers_resolved
            ORDER BY customer_id
        """)
        resolved_ids = set(r[0] for r in cur.fetchall())

        cur2 = pg_us_conn.cursor()
        cur2.execute("SELECT customer_id FROM customers ORDER BY customer_id")
        pg_ids = set(r[0] for r in cur2.fetchall())

        missing = pg_ids - resolved_ids
        if missing:
            print(f"  STATUS        : MISMATCH ✗ — missing from resolved: {missing}")
        else:
            print(f"  STATUS        : ALL PG CUSTOMERS PRESENT IN RESOLVED ✓")

        # Quarantine summary
        total, unique, last = get_quarantine_summary(dest_conn)
        print(f"\nQUARANTINE SUMMARY:")
        print(f"  Total conflicts logged   : {total}")
        print(f"  Unique customers affected: {unique}")
        print(f"  Last conflict at         : {last}")

        # Overall health
        print(f"\nOVERALL STATUS:")
        if resolved_count > 0:
            print(f"  HEALTHY — pipeline is running and writing resolved records")
        else:
            print(f"  WARNING — no resolved records found")

        pg_us_conn.close()
        dest_conn.close()

    except Exception as e:
        print(f"  RECONCILIATION FAILED — {e}")

    print(f"{'='*50}\n")


# ─── Scheduler ─────────────────────────────────────────────
scheduler = BlockingScheduler()
scheduler.add_job(
    run_reconciliation,
    trigger='interval',
    minutes=1,        # runs every 1 minute for testing
    next_run_time=datetime.now()  # run immediately on start
)

print("Reconciler started — runs every 1 minute. Press Ctrl+C to stop.")
scheduler.start()