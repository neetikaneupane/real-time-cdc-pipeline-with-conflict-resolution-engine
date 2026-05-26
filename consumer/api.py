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