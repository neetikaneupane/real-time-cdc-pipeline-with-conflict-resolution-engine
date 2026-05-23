from kafka import KafkaConsumer
import json
from datetime import datetime
from collections import defaultdict

consumer = KafkaConsumer(
    'source_a.public.customers',
    'source_b.source_eu.customers',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# State store — holds recent events per customer_id per source
pending = defaultdict(dict)
CONFLICT_WINDOW_SECONDS = 30

def check_conflict(customer_id, source, event):
    now = datetime.utcnow()
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

print("Listening for conflicts... Press Ctrl+C to stop\n")

for message in consumer:
    topic = message.topic
    payload = message.value['payload']

    if payload['op'] not in ('c', 'u'):
        continue

    source = 'POSTGRES_US' if 'source_a' in topic else 'MYSQL_EU'
    after = payload['after']
    customer_id = after['customer_id']

    is_conflict, sources = check_conflict(customer_id, source, after)

    if is_conflict:
        print(f"CONFLICT DETECTED — customer_id: {customer_id}")
        for src, data in sources.items():
            print(f"  [{src}] email: {data['event']['email']}")
        print(f"  Both arrived within {CONFLICT_WINDOW_SECONDS} seconds")
        print()
        del pending[customer_id]
    else:
        print(f"No conflict — [{source}] customer_id: {customer_id} email: {after['email']}")