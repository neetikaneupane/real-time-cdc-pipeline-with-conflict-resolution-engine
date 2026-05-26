import json
import psycopg2
from datetime import datetime, timezone
from kafka import KafkaProducer

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

# ─── Kafka Producer ────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

MAX_RETRIES = 3

def reprocess():
    cursor.execute("""
        SELECT id, topic, original_event, retry_count
        FROM dead_letter_queue
        WHERE retryable = true
        AND resolved = false
        AND retry_count < %s
        ORDER BY failed_at ASC
    """, (MAX_RETRIES,))

    rows = cursor.fetchall()

    if not rows:
        print("No retryable events in DLQ.")
        return

    print(f"Found {len(rows)} retryable events — replaying...\n")

    for row in rows:
        dlq_id        = row[0]
        topic         = row[1]
        original_event = json.loads(row[2])
        retry_count   = row[3]

        try:
            # Re-publish the original event back to its Kafka topic
            producer.send(topic, value=original_event)
            producer.flush()

            # Update retry count
            cursor.execute("""
                UPDATE dead_letter_queue
                SET retry_count = retry_count + 1,
                    last_retried_at = NOW()
                WHERE id = %s
            """, (dlq_id,))

            print(f"  Replayed DLQ id={dlq_id} to topic={topic} (attempt {retry_count + 1})")

        except Exception as e:
            print(f"  Failed to replay DLQ id={dlq_id} — {e}")

    # Mark as resolved if max retries reached
    cursor.execute("""
        UPDATE dead_letter_queue
        SET resolved = true
        WHERE retryable = true
        AND retry_count >= %s
    """, (MAX_RETRIES,))

    print("\nDone.")

if __name__ == '__main__':
    reprocess()