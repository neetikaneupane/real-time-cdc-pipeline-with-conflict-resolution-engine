from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'source_a.public.customers',
    'source_b.source_eu.customers',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id = None,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening to both topics... Press Ctrl+C to stop\n")

for message in consumer:
    topic = message.topic
    payload = message.value['payload']
    
    op_map = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'SNAPSHOT'}
    op = op_map.get(payload['op'], payload['op'])
    
    source = 'POSTGRES (US)' if 'source_a' in topic else 'MYSQL (EU)'
    customer_id = payload['after']['customer_id'] if payload['after'] else payload['before']['customer_id']
    
    print(f"[{source}] {op} → customer_id: {customer_id}")
    print(f"  before : {payload['before']}")
    print(f"  after  : {payload['after']}")
    print(f"  source : {payload['source']['connector']} | db: {payload['source']['db']}")
    print()