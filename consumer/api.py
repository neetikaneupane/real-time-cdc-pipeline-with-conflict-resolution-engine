from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import psycopg2
from datetime import datetime, timezone

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
@app.get("/schema/history")
def schema_history():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name, source, version, is_current, detected_at
        FROM schema_registry
        ORDER BY table_name, detected_at
    """)
    rows = cur.fetchall()

    cur.execute("""
        SELECT table_name, source, change_type, column_name,
               old_type, new_type, auto_migrated, migration_sql, detected_at
        FROM schema_change_log
        ORDER BY detected_at DESC
        LIMIT 20
    """)
    changes = cur.fetchall()

    cur.execute("""
        SELECT table_name, change_type, details, detected_at, resolved
        FROM schema_change_alerts
        ORDER BY detected_at DESC
    """)
    alerts = cur.fetchall()

    conn.close()

    return {
        "registry": [
            {
                "table_name":  r[0],
                "source":      r[1],
                "version":     r[2],
                "is_current":  r[3],
                "detected_at": r[4]
            }
            for r in rows
        ],
        "changes": [
            {
                "table_name":    r[0],
                "source":        r[1],
                "change_type":   r[2],
                "column_name":   r[3],
                "old_type":      r[4],
                "new_type":      r[5],
                "auto_migrated": r[6],
                "migration_sql": r[7],
                "detected_at":   r[8]
            }
            for r in changes
        ],
        "alerts": [
            {
                "table_name":  r[0],
                "change_type": r[1],
                "details":     r[2],
                "detected_at": r[3],
                "resolved":    r[4]
            }
            for r in alerts
        ]
    }

@app.post("/schema/unfreeze/{table_name}")
def unfreeze_table(table_name: str):
    conn = get_conn()
    cur = conn.cursor()

    # check if frozen
    cur.execute("""
        SELECT frozen, frozen_reason
        FROM schema_circuit_breaker
        WHERE table_name = %s
    """, (table_name,))
    row = cur.fetchone()

    if not row:
        conn.close()
        return {"error": f"No circuit breaker found for {table_name}"}

    if not row[0]:
        conn.close()
        return {"message": f"{table_name} is not frozen"}

    # unfreeze
    cur.execute("""
        UPDATE schema_circuit_breaker
        SET frozen = false, unfrozen_at = NOW()
        WHERE table_name = %s
    """, (table_name,))

    # resolve all open alerts for this table
    cur.execute("""
        UPDATE schema_change_alerts
        SET resolved = true
        WHERE table_name = %s AND resolved = false
    """, (table_name,))

    conn.commit()
    conn.close()

    return {
        "message":    f"{table_name} unfrozen successfully",
        "table_name": table_name,
        "unfrozen_at": datetime.now(timezone.utc).isoformat()
    }

@app.get("/schema/divergence")
def schema_divergence():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name, column_name, source_a_type,
               source_b_type, detected_at, resolved
        FROM schema_divergence
        ORDER BY detected_at DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "table_name":    r[0],
            "column_name":   r[1],
            "source_a_type": r[2],
            "source_b_type": r[3],
            "detected_at":   r[4],
            "resolved":      r[5]
        }
        for r in rows
    ]

@app.post("/schema/divergence/{table_name}/{column_name}/resolve")
def resolve_divergence(table_name: str, column_name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE schema_divergence
        SET resolved = true, resolved_at = NOW()
        WHERE table_name = %s
        AND column_name = %s
        AND resolved = false
    """, (table_name, column_name))
    conn.commit()
    conn.close()
    return {
        "message":     f"Divergence resolved for {table_name}.{column_name}",
        "resolved_at": datetime.now(timezone.utc).isoformat()
    }
@app.get("/anomalies/composite")
def get_composite_anomalies():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, table_name, severity, composite_score,
               peak_score, signals_fired, started_at,
               ended_at, duration_seconds, resolved
        FROM composite_anomalies
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
            "composite_score":  r[3],
            "peak_score":       r[4],
            "signals_fired":    r[5],
            "started_at":       r[6],
            "ended_at":         r[7],
            "duration_seconds": r[8],
            "resolved":         r[9]
        }
        for r in rows
    ]


@app.get("/anomalies/signals")
def get_signal_history():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name, signal_type, value,
               baseline_mean, deviation_score,
               severity, recorded_at
        FROM anomaly_signals
        WHERE recorded_at > NOW() - INTERVAL '2 hours'
        ORDER BY recorded_at DESC
        LIMIT 50
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "table_name":     r[0],
            "signal_type":    r[1],
            "value":          r[2],
            "baseline_mean":  r[3],
            "deviation_score": r[4],
            "severity":       r[5],
            "recorded_at":    r[6]
        }
        for r in rows
    ]