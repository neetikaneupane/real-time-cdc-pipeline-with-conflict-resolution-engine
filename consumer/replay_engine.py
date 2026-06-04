import json
import psycopg2
import yaml
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from datetime import datetime, timezone
from collections import defaultdict
import traceback

KAFKA_BOOTSTRAP        = 'localhost:9092'
CONFLICT_WINDOW_SECONDS = 60

with open('tables_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

topic_to_config = {}
for table in config['tables']:
    topic_to_config[table['source_topic_a']] = {**table, 'source': 'SOURCE_A'}
    topic_to_config[table['source_topic_b']] = {**table, 'source': 'SOURCE_B'}

all_topics = list(topic_to_config.keys())


def get_conn():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        user='debezium',
        password='debezium',
        dbname='destination_db'
    )


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
        except Exception:
            return value
    return value


def last_write_wins(sources):
    source_list = list(sources.keys())
    time_a = normalize_timestamp(
        sources[source_list[0]]['event'].get('updated_at', 0)
    )
    time_b = normalize_timestamp(
        sources[source_list[1]]['event'].get('updated_at', 0)
    )
    winner = source_list[0] if time_a >= time_b else source_list[1]
    loser  = source_list[1] if time_a >= time_b else source_list[0]
    return winner, loser


def source_priority(sources):
    source_list = list(sources.keys())
    if source_list[0] == 'SOURCE_A':
        return source_list[0], source_list[1]
    return source_list[1], source_list[0]


def field_merge(sources, field_rules):
    source_list  = list(sources.keys())
    event_a      = sources[source_list[0]]['event']
    event_b      = sources[source_list[1]]['event']
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
        merged      = field_merge(sources, field_rules)
        source_list = list(sources.keys())
        return {
            'resolved_value': merged,
            'losing_value':   sources[source_list[1]]['event'],
            'winning_source': 'FIELD_MERGE',
            'losing_source':  'BOTH',
            'strategy':       'FIELD_MERGE'
        }
    strategy_fn = {
        'last_write_wins': last_write_wins,
        'source_priority': source_priority
    }.get(strategy_name, last_write_wins)
    winner, loser = strategy_fn(sources)
    return {
        'resolved_value': sources[winner]['event'],
        'losing_value':   sources[loser]['event'],
        'winning_source': winner,
        'losing_source':  loser,
        'strategy':       strategy_name.upper()
    }


def write_resolved(cursor, resolution, table_cfg):
    dest_table = table_cfg['destination_table']
    pk         = table_cfg['primary_key']
    r          = resolution['resolved_value']
    fields     = list(r.keys())
    values     = []
    for f in fields:
        val = r[f]
        if f == 'updated_at':
            val = int(normalize_timestamp(val) * 1_000_000)
        else:
            val = clean_value(f, val)
        values.append(val)
    fields      += ['resolved_at', 'winning_source', 'strategy']
    values      += [
        datetime.now(timezone.utc),
        resolution['winning_source'],
        resolution['strategy']
    ]
    cols         = ', '.join(fields)
    placeholders = ', '.join(['%s'] * len(values))
    update_set   = ', '.join(
        [f"{f} = EXCLUDED.{f}" for f in fields if f != pk]
    )
    sql = (
        f"INSERT INTO {dest_table} ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk}) DO UPDATE SET {update_set}"
    )
    cursor.execute(sql, values)


def write_non_conflict(cursor, event, table_cfg, source):
    dest_table   = table_cfg['destination_table']
    pk           = table_cfg['primary_key']
    fields       = list(event.keys())
    values       = []
    for f in fields:
        val = event[f]
        if f == 'updated_at':
            val = int(normalize_timestamp(val) * 1_000_000)
        else:
            val = clean_value(f, val)
        values.append(val)
    fields      += ['resolved_at', 'winning_source', 'strategy']
    values      += [datetime.now(timezone.utc), source, 'NO_CONFLICT']
    cols         = ', '.join(fields)
    placeholders = ', '.join(['%s'] * len(values))
    update_set   = ', '.join(
        [f"{f} = EXCLUDED.{f}" for f in fields if f != pk]
    )
    sql = (
        f"INSERT INTO {dest_table} ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk}) DO UPDATE SET {update_set}"
    )
    cursor.execute(sql, values)


def create_replay_record(cursor, from_ts, to_ts, topics):
    cursor.execute("""
        INSERT INTO replay_history
            (started_at, from_timestamp, to_timestamp, topics, status)
        VALUES (NOW(), %s, %s, %s, 'RUNNING')
        RETURNING id
    """, (from_ts, to_ts, ', '.join(topics)))
    return cursor.fetchone()[0]


def update_replay_record(cursor, replay_id, events_replayed,
                         events_skipped, status, error_message=None):
    cursor.execute("""
        UPDATE replay_history
        SET completed_at    = NOW(),
            events_replayed = %s,
            events_skipped  = %s,
            status          = %s,
            error_message   = %s
        WHERE id = %s
    """, (events_replayed, events_skipped, status, error_message, replay_id))


def run_replay(from_timestamp, to_timestamp=None, topics=None):
    if topics is None:
        topics = all_topics

    print(f'\n{"="*55}')
    print(f'REPLAY ENGINE')
    print(f'From    : {from_timestamp}')
    print(f'To      : {to_timestamp if to_timestamp else "now"}')
    print(f'Topics  : {topics}')
    print(f'{"="*55}')

    conn   = get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    replay_id = create_replay_record(
        cursor, from_timestamp, to_timestamp, topics
    )
    print(f'Replay ID: {replay_id}')

    from_ms = int(from_timestamp.timestamp() * 1000)
    to_ms   = int(to_timestamp.timestamp() * 1000) if to_timestamp else None

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000
    )

    tps = []
    for topic in topics:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            print(f'  No partitions for {topic} -- skipping')
            continue
        for p in partitions:
            tps.append(TopicPartition(topic, p))

    if not tps:
        print('  No valid topic partitions found.')
        update_replay_record(cursor, replay_id, 0, 0, 'FAILED',
                             'No valid topic partitions found')
        consumer.close()
        conn.close()
        return

    consumer.assign(tps)

    offsets_for_times = consumer.offsets_for_times(
        {tp: from_ms for tp in tps}
    )

    for tp, offset_and_ts in offsets_for_times.items():
        if offset_and_ts is not None:
            consumer.seek(tp, offset_and_ts.offset)
            print(f'  Seeked {tp.topic} partition={tp.partition} '
                  f'to offset={offset_and_ts.offset}')
        else:
            end = consumer.end_offsets([tp])[tp]
            consumer.seek(tp, end)
            print(f'  No messages at/after from_timestamp '
                  f'for {tp.topic} -- seeking to end')

    events_replayed = 0
    events_skipped  = 0
    pending         = defaultdict(lambda: defaultdict(dict))

    try:
        for message in consumer:
            msg_ts = message.timestamp

            if to_ms and msg_ts > to_ms:
                events_skipped += 1
                continue

            try:
                topic     = message.topic
                payload   = message.value.get('payload', {})
                table_cfg = topic_to_config.get(topic)

                if not table_cfg:
                    events_skipped += 1
                    continue

                if payload.get('op') not in ('c', 'u'):
                    events_skipped += 1
                    continue

                after    = payload.get('after')
                if not after:
                    events_skipped += 1
                    continue

                pk       = table_cfg['primary_key']
                pk_value = after[pk]
                source   = table_cfg['source']
                table_name = table_cfg['name']
                strategy   = table_cfg['strategy']

                now = datetime.now(timezone.utc)
                pending[table_name][pk_value][source] = {
                    'event':       after,
                    'received_at': now
                }

                sources     = pending[table_name][pk_value]
                is_conflict = False

                if len(sources) > 1:
                    source_list = list(sources.keys())
                    time_a = sources[source_list[0]]['received_at']
                    time_b = sources[source_list[1]]['received_at']
                    if abs((time_a - time_b).total_seconds()) <= CONFLICT_WINDOW_SECONDS:
                        is_conflict = True

                if is_conflict:
                    resolution = resolve_conflict(sources, strategy, table_cfg)
                    write_resolved(cursor, resolution, table_cfg)
                    print(f'  CONFLICT [{table_name}] {pk}={pk_value} '
                          f'resolved via {resolution["strategy"]}')
                    del pending[table_name][pk_value]
                else:
                    write_non_conflict(cursor, after, table_cfg, source)
                    print(f'  NO CONFLICT [{table_name}] [{source}] '
                          f'{pk}={pk_value}')

                events_replayed += 1

            except Exception as e:
                print(f'  ERROR processing message -- {e}')
                events_skipped += 1

    except Exception as e:
        error = traceback.format_exc()
        print(f'  REPLAY FAILED -- {e}')
        update_replay_record(
            cursor, replay_id,
            events_replayed, events_skipped,
            'FAILED', error
        )
        consumer.close()
        conn.close()
        return

    update_replay_record(
        cursor, replay_id,
        events_replayed, events_skipped,
        'COMPLETED'
    )

    print(f'\n{"="*55}')
    print(f'REPLAY COMPLETE')
    print(f'  Replay ID       : {replay_id}')
    print(f'  Events replayed : {events_replayed}')
    print(f'  Events skipped  : {events_skipped}')
    print(f'{"="*55}\n')

    consumer.close()
    conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Replay Kafka events from a timestamp')
    parser.add_argument(
        '--from',
        dest='from_ts',
        required=True,
        help='Start timestamp in format YYYY-MM-DD HH:MM:SS'
    )
    parser.add_argument(
        '--to',
        dest='to_ts',
        required=False,
        default=None,
        help='End timestamp in format YYYY-MM-DD HH:MM:SS'
    )
    parser.add_argument(
        '--topics',
        dest='topics',
        required=False,
        default=None,
        help='Comma separated list of topics to replay'
    )

    args = parser.parse_args()

    from_ts = datetime.strptime(args.from_ts, '%Y-%m-%d %H:%M:%S').replace(
        tzinfo=timezone.utc
    )
    to_ts = None
    if args.to_ts:
        to_ts = datetime.strptime(args.to_ts, '%Y-%m-%d %H:%M:%S').replace(
            tzinfo=timezone.utc
        )

    topics = None
    if args.topics:
        topics = [t.strip() for t in args.topics.split(',')]

    run_replay(from_ts, to_ts, topics)