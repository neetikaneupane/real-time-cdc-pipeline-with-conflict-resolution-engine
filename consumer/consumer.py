from kafka import KafkaConsumer
import json
from datetime import datetime, timezone
from collections import defaultdict
import psycopg2

# ─── Database Connection ───────────────────────────────────
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    dbname='destination_db',
    user='debezium',
    password='debezium'
)
conn.autocommit = True
cursor = conn.cursor()

# ─── Kafka Consumer ────────────────────────────────────────
consumer = KafkaConsumer(
    'source_a.public.customers',
    'source_b.source_eu.customers',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

pending = defaultdict(dict)
CONFLICT_WINDOW_SECONDS = 30


# ─── Timestamp Normalizer ──────────────────────────────────
def normalize_timestamp(value):
    if isinstance(value, str):
        return datetime.fromisoformat(
            value.replace('Z', '+00:00')
        ).timestamp()
    else:
        return value / 1_000_000


# ─── Conflict Checker ──────────────────────────────────────
def check_conflict(customer_id, source, event):
    now = datetime.now(timezone.utc)
    pending[customer_id][source] = {
        'event': event,
        'received_at': now
    }

    sources = pending[customer_id]

    if len(sources) > 1:
        source_list = list(sources.keys())
        time_a = sources[source_list[0]]['received_at']
        time_b = sources[source_list[1]]['received_at']

        if abs((time_a - time_b).total_seconds()) <= CONFLICT_WINDOW_SECONDS:
            return True, sources
        else:
            oldest = min(source_list, key=lambda s: sources[s]['received_at'])
            del pending[customer_id][oldest]

    return False, None


# ─── Resolution Engine ─────────────────────────────────────
def resolve_conflict(sources):
    source_list = list(sources.keys())
    event_a = sources[source_list[0]]
    event_b = sources[source_list[1]]

    time_a = normalize_timestamp(event_a['event'].get('updated_at', 0))
    time_b = normalize_timestamp(event_b['event'].get('updated_at', 0))

    if time_a >= time_b:
        winner = source_list[0]
        loser  = source_list[1]
    else:
        winner = source_list[1]
        loser  = source_list[0]

    return {
        'resolved_value': sources[winner]['event'],
        'winning_source': winner,
        'losing_source':  loser,
        'losing_value':   sources[loser]['event'],
        'strategy':       'LAST_WRITE_WINS'
    }


# ─── Write to Destination DB ───────────────────────────────
def write_resolved(resolution):
    r = resolution['resolved_value']
    cursor.execute("""
        INSERT INTO customers_resolved
            (customer_id, name, email, phone, updated_at,
             source_region, resolved_at, winning_source, strategy)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s)
        ON CONFLICT (customer_id) DO UPDATE SET
            name           = EXCLUDED.name,
            email          = EXCLUDED.email,
            phone          = EXCLUDED.phone,
            updated_at     = EXCLUDED.updated_at,
            source_region  = EXCLUDED.source_region,
            resolved_at    = NOW(),
            winning_source = EXCLUDED.winning_source,
            strategy       = EXCLUDED.strategy
    """, (
        r['customer_id'],
        r['name'],
        r['email'],
        r['phone'],
        int(normalize_timestamp(r.get('updated_at', 0)) * 1_000_000),
        r['source_region'],
        resolution['winning_source'],
        resolution['strategy']
    ))
    print(f"  written to customers_resolved")


# ─── Write to Quarantine ───────────────────────────────────
def write_quarantine(resolution, customer_id):
    cursor.execute("""
        INSERT INTO customers_quarantine
            (customer_id, detected_at, winning_source, losing_source,
             winning_email, losing_email, strategy)
        VALUES (%s, NOW(), %s, %s, %s, %s, %s)
    """, (
        customer_id,
        resolution['winning_source'],
        resolution['losing_source'],
        resolution['resolved_value']['email'],
        resolution['losing_value']['email'],
        resolution['strategy']
    ))
    print(f"  conflict logged to customers_quarantine")

def write_non_conflict(event):
    cursor.execute("""
        INSERT INTO customers_resolved
            (customer_id, name, email, phone, updated_at,
             source_region, resolved_at, winning_source, strategy)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s)
        ON CONFLICT (customer_id) DO UPDATE SET
            name          = EXCLUDED.name,
            email         = EXCLUDED.email,
            phone         = EXCLUDED.phone,
            updated_at    = EXCLUDED.updated_at,
            source_region = EXCLUDED.source_region,
            resolved_at   = NOW(),
            winning_source = EXCLUDED.winning_source,
            strategy      = EXCLUDED.strategy
    """, (
        event['customer_id'],
        event['name'],
        event['email'],
        event['phone'],
        int(normalize_timestamp(event.get('updated_at', 0)) * 1_000_000),
        event['source_region'],
        source,
        'NO_CONFLICT'
    ))


# ─── Main Loop ─────────────────────────────────────────────
print("Listening for conflicts... Press Ctrl+C to stop\n")

for message in consumer:
    topic   = message.topic
    payload = message.value['payload']

    if payload['op'] not in ('c', 'u'):
        continue

    source      = 'POSTGRES_US' if 'source_a' in topic else 'MYSQL_EU'
    after       = payload['after']
    customer_id = after['customer_id']

    is_conflict, sources = check_conflict(customer_id, source, after)

    if is_conflict:
        print(f"CONFLICT DETECTED — customer_id: {customer_id}")
        for src, data in sources.items():
            print(f"  [{src}] email: {data['event']['email']}")

        resolution = resolve_conflict(sources)
        print(f"  RESOLVED via {resolution['strategy']}")
        print(f"  WINNER: {resolution['winning_source']}")
        print(f"  LOSER:  {resolution['losing_source']}")
        print(f"  FINAL email: {resolution['resolved_value']['email']}")

        write_resolved(resolution)
        write_quarantine(resolution, customer_id)
        print()
        del pending[customer_id]
    else:
        print(f"No conflict — [{source}] customer_id: {customer_id} email: {after['email']}")
        write_non_conflict(after)