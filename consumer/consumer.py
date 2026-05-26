from kafka import KafkaConsumer
import json
from datetime import datetime, timezone
from collections import defaultdict
import psycopg2
import yaml

# ─── Load Table Config ─────────────────────────────────────
with open('tables_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Build lookup: topic → table config
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

# ─── State Store per table ─────────────────────────────────
pending = defaultdict(lambda: defaultdict(dict))
CONFLICT_WINDOW_SECONDS = 30


# ─── Timestamp Normalizer ──────────────────────────────────
def normalize_timestamp(value):
    if isinstance(value, str):
        return datetime.fromisoformat(
            value.replace('Z', '+00:00')
        ).timestamp()
    elif value:
        return value / 1_000_000
    return 0

def clean_value(key, value):
    # MySQL sends DECIMAL as base64 encoded bytes — decode it
    if isinstance(value, str) and len(value) <= 8 and key == 'amount':
        try:
            import base64
            decoded = base64.b64decode(value)
            # Convert bytes to float
            import struct
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
    # SOURCE_A always wins
    if source_list[0] == 'SOURCE_A':
        return source_list[0], source_list[1]
    return source_list[1], source_list[0]

STRATEGIES = {
    'last_write_wins': last_write_wins,
    'source_priority': source_priority
}

def resolve_conflict(sources, strategy_name):
    strategy_fn = STRATEGIES.get(strategy_name, last_write_wins)
    winner, loser = strategy_fn(sources)
    return {
        'resolved_value': sources[winner]['event'],
        'losing_value':   sources[loser]['event'],
        'winning_source': winner,
        'losing_source':  loser,
        'strategy':       strategy_name.upper()
    }


# ─── Generic Write to Resolved Table ──────────────────────
def write_resolved(resolution, table_config):
    dest_table = table_config['destination_table']
    pk         = table_config['primary_key']
    r          = resolution['resolved_value']
    pk_value   = r[pk]

    # Build column list dynamically from event fields
    fields = list(r.keys())
    values = []
    for f in fields:
        val = r[f]
        if f == 'updated_at':
            val = int(normalize_timestamp(val) * 1_000_000)
        else:
            val = clean_value(f, val)
        values.append(val)

    # Add metadata columns
    fields += ['resolved_at', 'winning_source', 'strategy']
    values += [datetime.now(timezone.utc), resolution['winning_source'], resolution['strategy']]

    cols        = ', '.join(fields)
    placeholders = ', '.join(['%s'] * len(values))
    update_set  = ', '.join([
        f"{f} = EXCLUDED.{f}"
        for f in fields if f != pk
    ])

    sql = f"""
        INSERT INTO {dest_table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT ({pk}) DO UPDATE SET {update_set}
    """
    cursor.execute(sql, values)
    print(f"  written to {dest_table}")


# ─── Generic Write to Quarantine ──────────────────────────
def write_quarantine(resolution, table_config, pk_value):
    table_name = table_config['name']
    pk         = table_config['primary_key']

    cursor.execute(f"""
        INSERT INTO {table_name}_quarantine
            ({pk}, detected_at, winning_source, losing_source, strategy)
        VALUES (%s, NOW(), %s, %s, %s)
    """, (
        pk_value,
        resolution['winning_source'],
        resolution['losing_source'],
        resolution['strategy']
    ))
    print(f"  conflict logged to {table_name}_quarantine")


# ─── Main Loop ─────────────────────────────────────────────
print("Listening for events... Press Ctrl+C to stop\n")

for message in consumer:
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
        print(f"CONFLICT [{table_name}] — {pk}: {pk_value}")
        resolution = resolve_conflict(sources, strategy)
        print(f"  WINNER: {resolution['winning_source']} via {resolution['strategy']}")
        write_resolved(resolution, table_cfg)
        write_quarantine(resolution, table_cfg, pk_value)
        print()
        del pending[table_name][pk_value]
    else:
        print(f"No conflict [{table_name}] — [{source}] {pk}: {pk_value}")