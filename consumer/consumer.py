from kafka import KafkaConsumer
import json
from datetime import datetime, timezone
from collections import defaultdict
import psycopg2
import yaml
import traceback

# ─── Load Table Config ─────────────────────────────────────
with open('tables_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

topic_to_config = {}
for table in config['tables']:
    topic_to_config[table['source_topic_a']] = {**table, 'source': 'SOURCE_A'}
    topic_to_config[table['source_topic_b']] = {**table, 'source': 'SOURCE_B'}

all_topics = list(topic_to_config.keys())
print(f"Watching {len(config['tables'])} tables:")
for table in config['tables']:
    print(f"  - {table['name']} (strategy: {table['strategy']})")
print()

# ─── Database Connection ───────────────────────────────────
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='debezium',
    password='debezium',
    dbname='destination_db'
)
conn.autocommit = True
cursor = conn.cursor()

# ─── Kafka Consumer ────────────────────────────────────────
consumer = KafkaConsumer(
    *all_topics,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

pending = defaultdict(lambda: defaultdict(dict))
CONFLICT_WINDOW_SECONDS = 30


# ─── Helpers ───────────────────────────────────────────────
def normalize_timestamp(value):
    if isinstance(value, str):
        return datetime.fromisoformat(
            value.replace('Z', '+00:00')
        ).timestamp()
    elif value:
        return value / 1_000_000
    return 0


def clean_value(key, value):
    if isinstance(value, str) and len(value) <= 8 and key == 'amount':
        try:
            import base64
            decoded = base64.b64decode(value)
            return float(int.from_bytes(decoded, byteorder='big')) / 100
        except:
            return value
    return value


# ─── Conflict Checker ──────────────────────────────────────
def check_conflict(table_name, primary_key, source, event):
    now = datetime.now(timezone.utc)
    pending[table_name][primary_key][source] = {
        'event': event,
        'received_at': now
    }
    sources = pending[table_name][primary_key]
    if len(sources) > 1:
        source_list = list(sources.keys())
        time_a = sources[source_list[0]]['received_at']
        time_b = sources[source_list[1]]['received_at']
        if abs((time_a - time_b).total_seconds()) <= CONFLICT_WINDOW_SECONDS:
            return True, sources
        else:
            oldest = min(source_list, key=lambda s: sources[s]['received_at'])
            del pending[table_name][primary_key][oldest]
    return False, None


# ─── Resolution Strategies ─────────────────────────────────
def last_write_wins(sources):
    source_list = list(sources.keys())
    time_a = normalize_timestamp(sources[source_list[0]]['event'].get('updated_at', 0))
    time_b = normalize_timestamp(sources[source_list[1]]['event'].get('updated_at', 0))
    winner = source_list[0] if time_a >= time_b else source_list[1]
    loser  = source_list[1] if time_a >= time_b else source_list[0]
    return winner, loser


def source_priority(sources):
    source_list = list(sources.keys())
    if source_list[0] == 'SOURCE_A':
        return source_list[0], source_list[1]
    return source_list[1], source_list[0]


def field_merge(sources, field_rules):
    source_list = list(sources.keys())
    event_a = sources[source_list[0]]['event']
    event_b = sources[source_list[1]]['event']
    source_a_event = event_a if source_list[0] == 'SOURCE_A' else event_b
    source_b_event = event_b if source_list[0] == 'SOURCE_A' else event_a
    merged = {}
    for field in source_a_event:
        merged[field] = source_a_event[field]
    for field, rule in field_rules.items():
        val_a = source_a_event.get(field)
        val_b = source_b_event.get(field)
        if rule == 'latest':
            ts_a = normalize_timestamp(source_a_event.get('updated_at', 0))
            ts_b = normalize_timestamp(source_b_event.get('updated_at', 0))
            merged[field] = val_a if ts_a >= ts_b else val_b
        elif rule == 'non_null':
            merged[field] = val_a if val_a is not None else val_b
        elif rule == 'source_a':
            merged[field] = val_a
        elif rule == 'source_b':
            merged[field] = val_b
        elif rule == 'longest':
            merged[field] = val_a if len(str(val_a or '')) >= len(str(val_b or '')) else val_b
    return merged


def resolve_conflict(sources, strategy_name, table_cfg=None):
    if strategy_name == 'field_merge':
        field_rules = table_cfg.get('field_rules', {})
        merged = field_merge(sources, field_rules)
        source_list = list(sources.keys())
        return {
            'resolved_value': merged,
            'losing_value':   sources[source_list[1]]['event'],
            'winning_source': 'FIELD_MERGE',
            'losing_source':  'BOTH',
            'strategy':       'FIELD_MERGE'
        }
    strategy_fn = {'last_write_wins': last_write_wins, 'source_priority': source_priority}.get(strategy_name, last_write_wins)
    winner, loser = strategy_fn(sources)
    return {
        'resolved_value': sources[winner]['event'],
        'losing_value':   sources[loser]['event'],
        'winning_source': winner,
        'losing_source':  loser,
        'strategy':       strategy_name.upper()
    }


# ─── Writers ───────────────────────────────────────────────
def write_resolved(resolution, table_cfg):
    dest_table = table_cfg['destination_table']
    pk         = table_cfg['primary_key']
    r          = resolution['resolved_value']
    fields = list(r.keys())
    values = []
    for f in fields:
        val = r[f]
        if f == 'updated_at':
            val = int(normalize_timestamp(val) * 1_000_000)
        else:
            val = clean_value(f, val)
        values.append(val)
    fields += ['resolved_at', 'winning_source', 'strategy']
    values += [datetime.now(timezone.utc), resolution['winning_source'], resolution['strategy']]
    cols         = ', '.join(fields)
    placeholders = ', '.join(['%s'] * len(values))
    update_set   = ', '.join([f"{f} = EXCLUDED.{f}" for f in fields if f != pk])
    sql = f"INSERT INTO {dest_table} ({cols}) VALUES ({placeholders}) ON CONFLICT ({pk}) DO UPDATE SET {update_set}"
    cursor.execute(sql, values)
    print(f"  written to {dest_table}")


def write_quarantine(resolution, table_cfg, pk_value):
    table_name = table_cfg['name']
    pk         = table_cfg['primary_key']
    cursor.execute(f"""
        INSERT INTO {table_name}_quarantine
            ({pk}, detected_at, winning_source, losing_source, strategy)
        VALUES (%s, NOW(), %s, %s, %s)
    """, (pk_value, resolution['winning_source'], resolution['losing_source'], resolution['strategy']))
    print(f"  conflict logged to {table_name}_quarantine")

# ─── Write Conflict Metric ─────────────────────────────────
def write_conflict_metric(table_name, event_type, strategy=None,
                          resolution_ms=None, pk_value=None, winning_source=None):
    cursor.execute("""
        INSERT INTO conflict_metrics
            (recorded_at, table_name, event_type, strategy,
             resolution_ms, customer_id, winning_source)
        VALUES (NOW(), %s, %s, %s, %s, %s, %s)
    """, (
        table_name,
        event_type,
        strategy,
        resolution_ms,
        pk_value,
        winning_source
    ))


def write_non_conflict(event, table_cfg, source):
    dest_table = table_cfg['destination_table']
    pk         = table_cfg['primary_key']
    fields = list(event.keys())
    values = []
    for f in fields:
        val = event[f]
        if f == 'updated_at':
            val = int(normalize_timestamp(val) * 1_000_000)
        else:
            val = clean_value(f, val)
        values.append(val)
    fields += ['resolved_at', 'winning_source', 'strategy']
    values += [datetime.now(timezone.utc), source, 'NO_CONFLICT']
    cols         = ', '.join(fields)
    placeholders = ', '.join(['%s'] * len(values))
    update_set   = ', '.join([f"{f} = EXCLUDED.{f}" for f in fields if f != pk])
    sql = f"INSERT INTO {dest_table} ({cols}) VALUES ({placeholders}) ON CONFLICT ({pk}) DO UPDATE SET {update_set}"
    cursor.execute(sql, values)


def write_dlq(message, error, retryable=True):
    error_type    = type(error).__name__
    error_message = str(error)
    stack_trace   = traceback.format_exc()
    cursor.execute("""
        INSERT INTO dead_letter_queue
            (topic, partition, offset_value, error_type,
             error_message, retryable, original_event)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        message.topic,
        message.partition,
        message.offset,
        error_type,
        f"{error_message}\n{stack_trace}",
        retryable,
        json.dumps(message.value)
    ))
    print(f"  FAILED — written to DLQ (retryable={retryable})")


# ─── Main Loop ─────────────────────────────────────────────
print("Listening for events... Press Ctrl+C to stop\n")

for message in consumer:
    try:
        topic      = message.topic
        payload    = message.value['payload']
        table_cfg  = topic_to_config.get(topic)

        if not table_cfg:
            continue

        if payload['op'] not in ('c', 'u'):
            continue

        after      = payload['after']
        pk         = table_cfg['primary_key']
        pk_value   = after[pk]
        source     = table_cfg['source']
        table_name = table_cfg['name']
        strategy   = table_cfg['strategy']

        is_conflict, sources = check_conflict(table_name, pk_value, source, after)

        if is_conflict:
            start_time = datetime.now(timezone.utc)
            print(f"CONFLICT [{table_name}] — {pk}: {pk_value}")
            resolution = resolve_conflict(sources, strategy, table_cfg)
            print(f"  WINNER: {resolution['winning_source']} via {resolution['strategy']}")
            write_resolved(resolution, table_cfg)
            write_quarantine(resolution, table_cfg, pk_value)

            # calculate resolution time in milliseconds
            resolution_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)

            write_conflict_metric(
                table_name    = table_name,
                event_type    = 'conflict_detected',
                strategy      = resolution['strategy'],
                resolution_ms = resolution_ms,
                pk_value      = pk_value,
                winning_source= resolution['winning_source']
            )
            print()
            del pending[table_name][pk_value]
        else:
            print(f"No conflict [{table_name}] — [{source}] {pk}: {pk_value}")
            write_non_conflict(after, table_cfg, source)

    except psycopg2.OperationalError as e:
        print(f"  DB CONNECTION ERROR — {e}")
        write_dlq(message, e, retryable=True)

    except psycopg2.errors.InvalidTextRepresentation as e:
        print(f"  DATA FORMAT ERROR — {e}")
        write_dlq(message, e, retryable=False)

    except KeyError as e:
        print(f"  SCHEMA ERROR — missing field {e}")
        write_dlq(message, e, retryable=False)

    except Exception as e:
        print(f"  UNKNOWN ERROR — {e}")
        write_dlq(message, e, retryable=True)