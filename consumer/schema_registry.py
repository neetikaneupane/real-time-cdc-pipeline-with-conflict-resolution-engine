import json
import psycopg2
from datetime import datetime, timezone


# ─── Extract Schema from Debezium Message ─────────────────
def extract_schema(debezium_message):
    """
    Debezium message schema field contains the full table schema.
    We extract column names and types from the 'after' field definition.
    """
    try:
        fields = debezium_message['schema']['fields']
        after_field = next(
            (f for f in fields if f.get('field') == 'after'),
            None
        )
        if not after_field:
            return {}

        columns = {}
        for col in after_field.get('fields', []):
            col_name = col['field']
            col_type = col.get('name') or col.get('type', 'unknown')
            columns[col_name] = col_type

        return columns
    except Exception as e:
        print(f"  [SCHEMA] Failed to extract schema: {e}")
        return {}


# ─── Get Current Schema from Registry ─────────────────────
def get_current_schema(cursor, table_name, source):
    cursor.execute("""
        SELECT schema_json, version
        FROM schema_registry
        WHERE table_name = %s
        AND source = %s
        AND is_current = true
        ORDER BY detected_at DESC
        LIMIT 1
    """, (table_name, source))
    row = cursor.fetchone()
    if not row:
        return None, 0
    return json.loads(row[0]), row[1]


# ─── Save Schema to Registry ──────────────────────────────
def save_schema(cursor, table_name, source, schema, version):
    # mark old schema as not current
    cursor.execute("""
        UPDATE schema_registry
        SET is_current = false
        WHERE table_name = %s
        AND source = %s
    """, (table_name, source))

    # insert new schema
    cursor.execute("""
        INSERT INTO schema_registry
            (table_name, source, schema_json, version, detected_at, is_current)
        VALUES (%s, %s, %s, %s, NOW(), true)
    """, (table_name, source, json.dumps(schema), version))


# ─── Diff Two Schemas ─────────────────────────────────────
def diff_schemas(old_schema, new_schema):
    """
    Compare old and new schema and return list of changes.
    Each change is a dict with type, column_name, old_type, new_type.
    """
    changes = []

    old_cols = set(old_schema.keys())
    new_cols = set(new_schema.keys())

    # columns added
    for col in new_cols - old_cols:
        changes.append({
            'change_type': 'COLUMN_ADDED',
            'column_name': col,
            'old_type':    None,
            'new_type':    new_schema[col]
        })

    # columns removed
    for col in old_cols - new_cols:
        changes.append({
            'change_type': 'COLUMN_REMOVED',
            'column_name': col,
            'old_type':    old_schema[col],
            'new_type':    None
        })

    # type changed
    for col in old_cols & new_cols:
        if old_schema[col] != new_schema[col]:
            changes.append({
                'change_type': 'TYPE_CHANGED',
                'column_name': col,
                'old_type':    old_schema[col],
                'new_type':    new_schema[col]
            })

    return changes


# ─── Log Schema Change ────────────────────────────────────
def log_schema_change(cursor, table_name, source, change,
                      auto_migrated=False, migration_sql=None):
    cursor.execute("""
        INSERT INTO schema_change_log
            (table_name, source, change_type, column_name,
             old_type, new_type, detected_at, auto_migrated, migration_sql)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s)
    """, (
        table_name,
        source,
        change['change_type'],
        change['column_name'],
        change['old_type'],
        change['new_type'],
        auto_migrated,
        migration_sql
    ))


# ─── Log Schema Alert ─────────────────────────────────────
def log_schema_alert(cursor, table_name, change_type, details):
    cursor.execute("""
        INSERT INTO schema_change_alerts
            (table_name, change_type, details, detected_at)
        VALUES (%s, %s, %s, NOW())
    """, (table_name, change_type, details))


# ─── Build Migration SQL ──────────────────────────────────
def build_migration_sql(dest_table, change):
    """
    Build the ALTER TABLE SQL for safe changes.
    Only COLUMN_ADDED is auto-migrated.
    """
    if change['change_type'] == 'COLUMN_ADDED':
        col_type = map_debezium_type(change['new_type'])
        return f"ALTER TABLE {dest_table} ADD COLUMN IF NOT EXISTS {change['column_name']} {col_type};"
    return None


# ─── Map Debezium Types to PostgreSQL Types ───────────────
def map_debezium_type(debezium_type):
    """
    Map Debezium/Kafka Connect type names to PostgreSQL types.
    """
    type_map = {
        'string':                              'TEXT',
        'int32':                               'INT',
        'int64':                               'BIGINT',
        'float32':                             'FLOAT',
        'float64':                             'DOUBLE PRECISION',
        'boolean':                             'BOOLEAN',
        'bytes':                               'BYTEA',
        'io.debezium.time.MicroTimestamp':     'BIGINT',
        'io.debezium.time.ZonedTimestamp':     'BIGINT',
        'io.debezium.time.Timestamp':          'BIGINT',
        'org.apache.kafka.connect.data.Decimal': 'NUMERIC',
        'unknown':                             'TEXT'
    }
    return type_map.get(debezium_type, 'TEXT')


# ─── Main Schema Check Function ───────────────────────────
def check_and_update_schema(cursor, dest_cursor, table_name,
                             source, dest_table, debezium_message):
    """
    Main entry point. Call this on every message.
    Returns list of changes detected (empty if no changes).
    """
    new_schema = extract_schema(debezium_message)
    if not new_schema:
        return []

    current_schema, version = get_current_schema(cursor, table_name, source)

    # first time seeing this table — just register it
    if current_schema is None:
        save_schema(cursor, table_name, source, new_schema, version=1)
        print(f"  [SCHEMA] Registered schema for {table_name} ({source}) "
              f"— {len(new_schema)} columns")
        return []

    # compare schemas
    changes = diff_schemas(current_schema, new_schema)
    if not changes:
        return []

    # schema changed
    print(f"\n  [SCHEMA] Change detected in {table_name} ({source})"
          f" — {len(changes)} change(s)")

    migrations_applied = []

    for change in changes:
        print(f"  [SCHEMA] {change['change_type']}: "
              f"{change['column_name']} "
              f"({change['old_type']} → {change['new_type']})")

        if change['change_type'] == 'COLUMN_ADDED':
            # safe — auto migrate
            migration_sql = build_migration_sql(dest_table, change)
            try:
                dest_cursor.execute(migration_sql)
                log_schema_change(cursor, table_name, source, change,
                                  auto_migrated=True,
                                  migration_sql=migration_sql)
                print(f"  [SCHEMA] Auto-migrated: {migration_sql}")
                migrations_applied.append(migration_sql)
            except Exception as e:
                print(f"  [SCHEMA] Migration failed: {e}")
                log_schema_change(cursor, table_name, source, change,
                                  auto_migrated=False)

        elif change['change_type'] == 'COLUMN_REMOVED':
            # dangerous — log alert, don't auto-migrate
            log_schema_change(cursor, table_name, source, change)
            log_schema_alert(
                cursor, table_name, 'COLUMN_REMOVED',
                f"Column '{change['column_name']}' removed from "
                f"{table_name} in {source}. Manual review required."
            )
            print(f"  [SCHEMA] ALERT: Column removed — manual review required")

        elif change['change_type'] == 'TYPE_CHANGED':
            # very dangerous — log alert, don't auto-migrate
            log_schema_change(cursor, table_name, source, change)
            log_schema_alert(
                cursor, table_name, 'TYPE_CHANGED',
                f"Column '{change['column_name']}' type changed from "
                f"'{change['old_type']}' to '{change['new_type']}' "
                f"in {table_name} ({source}). Manual review required."
            )
            print(f"  [SCHEMA] ALERT: Type changed — manual review required")

    # update registry with new schema
    save_schema(cursor, table_name, source, new_schema, version=version + 1)
    print(f"  [SCHEMA] Registry updated to version {version + 1}")

    return changes