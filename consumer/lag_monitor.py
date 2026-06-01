import psycopg2
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler

KAFKA_BOOTSTRAP = 'localhost:9092'
CONSUMER_GROUP  = 'cdc-lag-monitor'

TOPICS = [
    'source_a.public.customers',
    'source_a.public.orders',
    'source_b.source_eu.customers',
    'source_b.source_eu.orders'
]

WARNING_THRESHOLD  = 100
CRITICAL_THRESHOLD = 500


def get_conn():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        user='debezium',
        password='debezium',
        dbname='destination_db'
    )


def get_lag_for_topics():
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False
    )

    results = []

    for topic in TOPICS:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            print(f'  No partitions found for {topic} — skipping')
            continue

        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)

        end_offsets = consumer.end_offsets(tps)

        for tp in tps:
            log_end    = end_offsets[tp]
            committed  = consumer.committed(tp)
            committed  = committed if committed is not None else 0
            lag        = log_end - committed

            results.append({
                'topic':            tp.topic,
                'partition_id':     tp.partition,
                'log_end_offset':   log_end,
                'committed_offset': committed,
                'lag':              lag,
                'consumer_group':   CONSUMER_GROUP
            })

    consumer.close()
    return results


def save_lag(cursor, lag_data):
    for row in lag_data:
        cursor.execute("""
            INSERT INTO consumer_lag
                (recorded_at, topic, partition_id, log_end_offset,
                 committed_offset, lag, consumer_group)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s)
        """, (
            row['topic'],
            row['partition_id'],
            row['log_end_offset'],
            row['committed_offset'],
            row['lag'],
            row['consumer_group']
        ))


def get_active_alert(cursor, topic, partition_id):
    cursor.execute("""
        SELECT id, severity, lag
        FROM lag_alerts
        WHERE topic = %s
        AND partition_id = %s
        AND resolved = false
        ORDER BY fired_at DESC
        LIMIT 1
    """, (topic, partition_id))
    return cursor.fetchone()


def fire_alert(cursor, row, severity):
    cursor.execute("""
        INSERT INTO lag_alerts
            (fired_at, topic, partition_id, lag, severity)
        VALUES (NOW(), %s, %s, %s, %s)
    """, (
        row['topic'],
        row['partition_id'],
        row['lag'],
        severity
    ))
    print(f'  ALERT [{severity}] {row["topic"]} '
          f'partition={row["partition_id"]} '
          f'lag={row["lag"]}')


def resolve_alert(cursor, alert_id):
    cursor.execute("""
        UPDATE lag_alerts
        SET resolved = true, resolved_at = NOW()
        WHERE id = %s
    """, (alert_id,))


def check_alerts(cursor, lag_data):
    for row in lag_data:
        active = get_active_alert(
            cursor, row['topic'], row['partition_id']
        )

        if row['lag'] >= CRITICAL_THRESHOLD:
            severity = 'CRITICAL'
        elif row['lag'] >= WARNING_THRESHOLD:
            severity = 'WARNING'
        else:
            severity = None

        if severity:
            if not active:
                fire_alert(cursor, row, severity)
            elif active[1] != severity:
                resolve_alert(cursor, active[0])
                fire_alert(cursor, row, severity)
        else:
            if active:
                resolve_alert(cursor, active[0])
                print(f'  RESOLVED {row["topic"]} '
                      f'partition={row["partition_id"]}')


def run_lag_monitor():
    print(f'\n{"="*50}')
    print(f'LAG MONITOR -- {datetime.now(timezone.utc).isoformat()}')
    print(f'{"="*50}')

    conn   = get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        lag_data = get_lag_for_topics()

        if not lag_data:
            print('  No lag data collected.')
            conn.close()
            return

        for row in lag_data:
            status = 'OK'
            if row['lag'] >= CRITICAL_THRESHOLD:
                status = 'CRITICAL'
            elif row['lag'] >= WARNING_THRESHOLD:
                status = 'WARNING'
            print(f'  {row["topic"]} '
                  f'partition={row["partition_id"]} '
                  f'lag={row["lag"]} [{status}]')

        save_lag(cursor, lag_data)
        check_alerts(cursor, lag_data)

    except Exception as e:
        print(f'  ERROR -- {e}')

    conn.close()
    print(f'{"="*50}\n')


scheduler = BlockingScheduler()
scheduler.add_job(
    run_lag_monitor,
    trigger='interval',
    minutes=1,
    next_run_time=datetime.now()
)

print('Lag monitor started -- runs every 1 minute. Press Ctrl+C to stop.')
scheduler.start()