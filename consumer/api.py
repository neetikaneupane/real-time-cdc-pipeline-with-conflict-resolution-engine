from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import psycopg2

app = FastAPI(title="CDC Pipeline API")
templates = Jinja2Templates(directory="templates")


# ─── Database Connection ───────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        user='debezium',
        password='debezium',
        dbname='destination_db'
    )


# ─── Health ───────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}


# ─── Metrics ──────────────────────────────────────────────
@app.get("/metrics")
def metrics():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM customers_resolved")
    resolved_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM customers_quarantine")
    conflict_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT customer_id) FROM customers_quarantine")
    unique_conflicts = cur.fetchone()[0]

    cur.execute("SELECT MAX(resolved_at) FROM customers_resolved")
    last_resolved = cur.fetchone()[0]

    cur.execute("SELECT MAX(detected_at) FROM customers_quarantine")
    last_conflict = cur.fetchone()[0]

    conn.close()

    return {
        "resolved_records":            resolved_count,
        "total_conflicts":             conflict_count,
        "unique_customers_conflicted": unique_conflicts,
        "last_resolved_at":            last_resolved,
        "last_conflict_at":            last_conflict
    }


# ─── Conflicts ────────────────────────────────────────────
@app.get("/conflicts")
def get_conflicts():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT customer_id, winning_source, losing_source,
               winning_email, losing_email, strategy, detected_at
        FROM customers_quarantine
        ORDER BY detected_at DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "customer_id":   r[0],
            "winner":        r[1],
            "loser":         r[2],
            "winning_email": r[3],
            "losing_email":  r[4],
            "strategy":      r[5],
            "detected_at":   r[6]
        }
        for r in rows
    ]


# ─── Customer Detail ──────────────────────────────────────
@app.get("/customers/{customer_id}")
def get_customer(customer_id: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT customer_id, name, email, phone,
               source_region, winning_source, strategy, resolved_at
        FROM customers_resolved
        WHERE customer_id = %s
    """, (customer_id,))
    row = cur.fetchone()

    if not row:
        return {"error": f"customer {customer_id} not found"}

    cur.execute("""
        SELECT winning_source, losing_source,
               winning_email, losing_email, detected_at
        FROM customers_quarantine
        WHERE customer_id = %s
        ORDER BY detected_at DESC
    """, (customer_id,))
    conflicts = cur.fetchall()
    conn.close()

    return {
        "customer_id":    row[0],
        "name":           row[1],
        "email":          row[2],
        "phone":          row[3],
        "source_region":  row[4],
        "winning_source": row[5],
        "strategy":       row[6],
        "resolved_at":    row[7],
        "conflict_history": [
            {
                "winner":        c[0],
                "loser":         c[1],
                "winning_email": c[2],
                "losing_email":  c[3],
                "detected_at":   c[4]
            }
            for c in conflicts
        ]
    }


# ─── Dashboard ────────────────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    conn = get_conn()
    cur = conn.cursor()

    # metrics
    cur.execute("SELECT COUNT(*) FROM customers_resolved")
    resolved = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM customers_quarantine")
    conflicts = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT customer_id) FROM customers_quarantine")
    unique = cur.fetchone()[0]

    cur.execute("SELECT MAX(resolved_at) FROM customers_resolved")
    last_resolved = cur.fetchone()[0]

    cur.execute("SELECT MAX(detected_at) FROM customers_quarantine")
    last_conflict = cur.fetchone()[0]

    # per table counts
    table_counts = []
    for table in [('customers', 'customers_resolved', 'customers_quarantine'),
                  ('orders',    'orders_resolved',    'orders_quarantine')]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table[1]}")
            res = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {table[2]}")
            conf = cur.fetchone()[0]
            table_counts.append({
                'table':     table[0],
                'resolved':  res,
                'conflicts': conf
            })
        except:
            pass

    # recent conflicts
    cur.execute("""
        SELECT customer_id, winning_source, losing_source,
               winning_email, losing_email, strategy, detected_at
        FROM customers_quarantine
        ORDER BY detected_at DESC
        LIMIT 10
    """)
    recent_conflicts = [
        {
            "customer_id":   r[0],
            "winner":        r[1],
            "loser":         r[2],
            "winning_email": r[3],
            "losing_email":  r[4],
            "strategy":      r[5],
            "detected_at":   r[6]
        }
        for r in cur.fetchall()
    ]

    conn.close()

    context = {
        "request": request,
        "metrics": {
            "resolved_records":            resolved,
            "total_conflicts":             conflicts,
            "unique_customers_conflicted": unique,
            "last_resolved_at":            last_resolved,
            "last_conflict_at":            last_conflict,
            "tables_watched":              2
        },
        "table_counts":  table_counts,
        "conflicts":     recent_conflicts
    }
    return templates.TemplateResponse(request=request, name="dashboard.html", context=context)

@app.get("/metrics/timeseries")
def metrics_timeseries():
    conn = get_conn()
    cur = conn.cursor()

    # conflicts per minute for last 2 hours
    cur.execute("""
        SELECT
            DATE_TRUNC('minute', recorded_at) AS minute,
            COUNT(*) AS conflict_count
        FROM conflict_metrics
        WHERE recorded_at > NOW() - INTERVAL '2 hours'
        AND event_type = 'conflict_detected'
        GROUP BY DATE_TRUNC('minute', recorded_at)
        ORDER BY minute ASC
    """)
    timeseries = [
        {
            "minute": row[0].isoformat(),
            "conflict_count": row[1]
        }
        for row in cur.fetchall()
    ]

    # latest baseline
    cur.execute("""
        SELECT mean_per_minute, stddev_per_minute
        FROM conflict_baselines
        ORDER BY computed_at DESC
        LIMIT 1
    """)
    baseline_row = cur.fetchone()
    baseline = {
        "mean": round(float(baseline_row[0]), 4) if baseline_row else 0,
        "stddev": round(float(baseline_row[1]), 4) if baseline_row else 0
    } if baseline_row else {"mean": 0, "stddev": 0}

    conn.close()

    return {
        "timeseries": timeseries,
        "baseline": baseline,
        "warning_threshold":  round(baseline["mean"] + 2 * baseline["stddev"], 4),
        "critical_threshold": round(baseline["mean"] + 3 * baseline["stddev"], 4)
    }


@app.get("/anomalies")
def get_anomalies():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, table_name, severity, started_at, ended_at,
               duration_seconds, peak_rate, baseline_mean,
               deviation_score, resolved
        FROM conflict_anomalies
        ORDER BY started_at DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "id":               r[0],
            "table_name":       r[1],
            "severity":         r[2],
            "started_at":       r[3],
            "ended_at":         r[4],
            "duration_seconds": r[5],
            "peak_rate":        r[6],
            "baseline_mean":    r[7],
            "deviation_score":  r[8],
            "resolved":         r[9]
        }
        for r in rows
    ]