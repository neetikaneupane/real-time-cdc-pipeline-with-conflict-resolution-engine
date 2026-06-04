import psycopg2
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler

HORIZON_MINUTES    = 5
MIN_DATA_POINTS    = 5
WARNING_THRESHOLD  = 2.0
CRITICAL_THRESHOLD = 4.0


def get_conn():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        user='debezium',
        password='debezium',
        dbname='destination_db'
    )


def get_recent_rates(cursor, table_name, n=10):
    cursor.execute("""
        SELECT
            DATE_TRUNC('minute', recorded_at) AS minute,
            COUNT(*) AS cnt
        FROM conflict_metrics
        WHERE event_type = 'conflict_detected'
        AND table_name = %s
        AND recorded_at > NOW() - INTERVAL '30 minutes'
        GROUP BY DATE_TRUNC('minute', recorded_at)
        ORDER BY minute ASC
        LIMIT %s
    """, (table_name, n))
    rows = cursor.fetchall()
    return rows


def linear_regression(data_points):
    n = len(data_points)
    if n < 2:
        return None, None

    x_vals = list(range(n))
    y_vals = [float(p) for p in data_points]

    x_mean = sum(x_vals) / n
    y_mean = sum(y_vals) / n

    numerator   = sum((x_vals[i] - x_mean) * (y_vals[i] - y_mean) for i in range(n))
    denominator = sum((x_vals[i] - x_mean) ** 2 for i in range(n))

    if denominator == 0:
        return 0.0, y_mean

    slope     = numerator / denominator
    intercept = y_mean - slope * x_mean

    return slope, intercept


def compute_confidence(data_points, slope, intercept):
    n      = len(data_points)
    y_vals = [float(p) for p in data_points]
    x_vals = list(range(n))

    if n < 2:
        return 0.0

    predicted = [slope * x + intercept for x in x_vals]
    residuals = [(y_vals[i] - predicted[i]) ** 2 for i in range(n)]
    rmse      = (sum(residuals) / n) ** 0.5
    y_mean    = sum(y_vals) / n

    if y_mean == 0:
        return 100.0 if rmse == 0 else 50.0

    cv = rmse / y_mean

    if cv <= 0.1:
        return 95.0
    elif cv <= 0.3:
        return 80.0
    elif cv <= 0.5:
        return 60.0
    elif cv <= 1.0:
        return 40.0
    else:
        return 20.0


def get_active_forecast_alert(cursor, table_name):
    cursor.execute("""
        SELECT id, severity, predicted_rate
        FROM forecast_alerts
        WHERE table_name = %s
        AND resolved = false
        ORDER BY fired_at DESC
        LIMIT 1
    """, (table_name,))
    return cursor.fetchone()


def fire_forecast_alert(cursor, table_name, current_rate,
                        predicted_rate, severity):
    cursor.execute("""
        INSERT INTO forecast_alerts
            (fired_at, table_name, current_rate,
             predicted_rate, horizon_minutes, severity)
        VALUES (NOW(), %s, %s, %s, %s, %s)
    """, (
        table_name, current_rate,
        predicted_rate, HORIZON_MINUTES, severity
    ))
    print(f'  FORECAST ALERT [{severity}] {table_name} -- '
          f'predicted rate={round(predicted_rate, 4)}/min '
          f'in {HORIZON_MINUTES} minutes')


def resolve_forecast_alert(cursor, alert_id):
    cursor.execute("""
        UPDATE forecast_alerts
        SET resolved = true, resolved_at = NOW()
        WHERE id = %s
    """, (alert_id,))


def save_forecast(cursor, table_name, current_rate,
                  predicted_rate, slope, confidence,
                  severity, data_points):
    cursor.execute("""
        INSERT INTO conflict_forecasts
            (recorded_at, table_name, current_rate,
             predicted_rate, slope, horizon_minutes,
             confidence, severity, data_points)
        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        table_name, current_rate, predicted_rate,
        slope, HORIZON_MINUTES, confidence,
        severity, data_points
    ))


def run_forecaster():
    print(f'\n{"="*55}')
    print(f'CONFLICT FORECASTER -- {datetime.now(timezone.utc).isoformat()}')
    print(f'{"="*55}')

    conn   = get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(
        'SELECT DISTINCT table_name FROM conflict_metrics'
    )
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        print('  No conflict data found yet.')
        conn.close()
        return

    for table_name in tables:
        print(f'\n  Table: {table_name}')

        rows = get_recent_rates(cursor, table_name)

        if len(rows) < MIN_DATA_POINTS:
            print(f'  Not enough data points ({len(rows)}/{MIN_DATA_POINTS}) -- skipping')
            continue

        counts       = [row[1] for row in rows]
        current_rate = float(counts[-1])

        slope, intercept = linear_regression(counts)

        if slope is None:
            print(f'  Could not compute regression -- skipping')
            continue

        predicted_rate = max(0, slope * (len(counts) + HORIZON_MINUTES) + intercept)
        confidence     = compute_confidence(counts, slope, intercept)

        if predicted_rate >= CRITICAL_THRESHOLD:
            severity = 'CRITICAL'
        elif predicted_rate >= WARNING_THRESHOLD:
            severity = 'WARNING'
        else:
            severity = None

        save_forecast(
            cursor, table_name, current_rate,
            predicted_rate, slope, confidence,
            severity, len(counts)
        )

        active = get_active_forecast_alert(cursor, table_name)

        if severity:
            if not active:
                fire_forecast_alert(
                    cursor, table_name,
                    current_rate, predicted_rate, severity
                )
            elif active[1] != severity:
                resolve_forecast_alert(cursor, active[0])
                fire_forecast_alert(
                    cursor, table_name,
                    current_rate, predicted_rate, severity
                )
        else:
            if active:
                resolve_forecast_alert(cursor, active[0])
                print(f'  Forecast alert resolved for {table_name}')

        direction = 'up' if slope > 0.1 else 'down' if slope < -0.1 else 'stable'

        print(f'  Data points      : {len(counts)}')
        print(f'  Current rate     : {round(current_rate, 4)}/min')
        print(f'  Slope            : {round(slope, 6)} (trending {direction})')
        print(f'  Predicted rate   : {round(predicted_rate, 4)}/min in {HORIZON_MINUTES} min')
        print(f'  Confidence       : {confidence}%')
        print(f'  Severity         : {severity if severity else "NORMAL"}')

    conn.close()
    print(f'\n{"="*55}\n')


scheduler = BlockingScheduler()
scheduler.add_job(
    run_forecaster,
    trigger='interval',
    minutes=1,
    next_run_time=datetime.now()
)

print('Conflict forecaster started -- runs every 1 minute. Press Ctrl+C to stop.')
scheduler.start()