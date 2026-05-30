import json
import psycopg2
from datetime import datetime, timezone


# ─── Extract Schema from Debezium Message ─────────────────
def extract_schema(debezium_message):
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
        SELECT schema_json, version, id
        FROM schema_registry
        WHERE table_name = %s
        AND source = %s
        AND is_current = true
        ORDER BY detected_at DESC
        LIMIT 1
    """, (table_name, source))
    row = cursor.fetchone()
    if not row:
        return None, 0, None
    return json.loads(row[0]), row[1], row[2]


# ─── Save Schema to Registry ──────────────────────────────
def save_schema(cursor, table_name, source, schema, version):
    cursor.execute("""
        UPDATE schema_registry
        SET is_current = false
        WHERE table_name = %s AND source = %s
    """, (table_name, source))
    cursor.execute("""
        INSERT INTO schema_registry
            (table_name, source, schema_json, version, detected_at, is_current)
        VALUES (%s, %s, %s, %s, NOW(), true)
    """, (table_name, source, json.dumps(schema), version))


# ─── Diff Two Schemas ─────────────────────────────────────
def diff_schemas(old_schema, new_schema):
    changes = []
    old_cols = set(old_schema.keys())
    new_cols = set(new_schema.keys())

    for col in new_cols - old_cols:
        changes.append({
            'change_type': 'COLUMN_ADDED',
            'column_name': col,
            'old_type':    None,
            'new_type':    new_schema[col]
        })
    for col in old_cols - new_cols:
        changes.append({
            'change_type': 'COLUMN_REMOVED',
            'column_name': col,
            'old_type':    old_schema[col],
            'new_type':    None
        })
    for col in old_cols & new_cols:
        if old_schema[col] != new_schema[col]:
            changes.append({
                'change_type': 'TYPE_CHANGED',
                'column_name': col,
                'old_type':    old_schema[col],
                'new_type':    new_schema[col]
            })
    return changes


# ─── Cross-Source Schema Comparator ───────────────────────
def check_cross_source_divergence(cursor, table_name):
    """
    Compare current schemas from SOURCE_A and SOURCE_B.
    Detect columns that exist in both but have different types.
    """
    cursor.execute("""
        SELECT source, schema_json
        FROM schema_registry
        WHERE table_name = %s AND is_current = true
    """, (table_name,))
    rows = cursor.fetchall()

    schemas = {row[0]: json.loads(row[1]) for row in rows}

    if 'SOURCE_A' not in schemas or 'SOURCE_B' not in schemas:
        return []

    schema_a = schemas['SOURCE_A']
    schema_b = schemas['SOURCE_B']

    divergences = []
    common_cols = set(schema_a.keys()) & set(schema_b.keys())

    for col in common_cols:
        if schema_a[col] != schema_b[col]:
            divergences.append({
                'column_name':   col,
                'source_a_type': schema_a[col],
                'source_b_type': schema_b[col]
            })

    return divergences


# ─── Log Divergence ───────────────────────────────────────
def log_divergence(cursor, table_name, divergence):
    # check if already logged and unresolved
    cursor.execute("""
        SELECT id FROM schema_divergence
        WHERE table_name = %s
        AND column_name = %s
        AND resolved = false
    """, (table_name, divergence['column_name']))

    if cursor.fetchone():
        return  # already logged

    cursor.execute("""
        INSERT INTO schema_divergence
            (table_name, column_name, source_a_type, source_b_type, detected_at)
        VALUES (%s, %s, %s, %s, NOW())
    """, (
        table_name,
        divergence['column_name'],
        divergence['source_a_type'],
        divergence['source_b_type']
    ))
    print(f"  [SCHEMA] DIVERGENCE: {table_name}.{divergence['column_name']} "
          f"SOURCE_A={divergence['source_a_type']} "
          f"SOURCE_B={divergence['source_b_type']}")


# ─── Circuit Breaker ──────────────────────────────────────
def is_table_frozen(cursor, table_name):
    cursor.execute("""
        SELECT frozen FROM schema_circuit_breaker
        WHERE table_name = %s
    """, (table_name,))
    row = cursor.fetchone()
    return row[0] if row else False


def freeze_table(cursor, table_name, reason):
    cursor.execute("""
        INSERT INTO schema_circuit_breaker
            (table_name, frozen, frozen_at, frozen_reason)
        VALUES (%s, true, NOW(), %s)
        ON CONFLICT (table_name) DO UPDATE SET
            frozen        = true,
            frozen_at     = NOW(),
            frozen_reason = EXCLUDED.frozen_reason
    """, (table_name, reason))
    print(f"  [SCHEMA] CIRCUIT BREAKER: {table_name} FROZEN — {reason}")


def unfreeze_table(cursor, table_name):
    cursor.execute("""
        UPDATE schema_circuit_breaker
        SET frozen = false, unfrozen_at = NOW()
        WHERE table_name = %s
    """, (table_name,))
    print(f"  [SCHEMA] CIRCUIT BREAKER: {table_name} UNFROZEN")


# ─── Schema Validation ────────────────────────────────────
def validate_message(event, current_schema):
    """
    Validate incoming event fields against registered schema.
    Returns list of validation errors.
    """
    if not current_schema:
        return []

    errors = []
    registered_cols = set(current_schema.keys())
    event_cols      = set(event.keys())

    # unknown columns in event not in schema
    unknown = event_cols - registered_cols
    for col in unknown:
        errors.append(f"Unknown column '{col}' not in registered schema")

    return errors


# ─── Auto-Resolve Alerts ──────────────────────────────────
def auto_resolve_alerts(cursor, table_name, current_schema):
    cursor.execute("""
        SELECT id, change_type, details
        FROM schema_change_alerts
        WHERE table_name = %s AND resolved = false
    """, (table_name,))
    open_alerts = cursor.fetchall()

    for alert in open_alerts:
        alert_id    = alert[0]
        change_type = alert[1]
        details     = alert[2]

        if change_type == 'COLUMN_REMOVED':
            # extract column name from details string
            try:
                column_name = details.split("'")[1]
                if column_name in current_schema:
                    cursor.execute("""
                        UPDATE schema_change_alerts
                        SET resolved = true
                        WHERE id = %s
                    """, (alert_id,))
                    print(f"  [SCHEMA] Alert auto-resolved: "
                          f"{column_name} reappeared in {table_name}")
            except Exception:
                pass


# ─── Backfill Engine ──────────────────────────────────────
def backfill_new_column(dest_cursor, dest_table, column_name,
                        default_value=None):
    """
    After a new column is added, update existing rows that have NULL
    with a sensible default value.
    """
    if default_value is None:
        return 0  # nothing to backfill

    dest_cursor.execute(f"""
        UPDATE {dest_table}
        SET {column_name} = %s
        WHERE {column_name} IS NULL
    """, (default_value,))

    return dest_cursor.rowcount


def log_backfill(cursor, table_name, column_name, rows_updated):
    cursor.execute("""
        INSERT INTO schema_backfill_log
            (table_name, column_name, rows_updated,
             started_at, completed_at, status)
        VALUES (%s, %s, %s, NOW(), NOW(), 'COMPLETED')
    """, (table_name, column_name, rows_updated))


# ─── Map Debezium Types to PostgreSQL Types ───────────────
def map_debezium_type(debezium_type):
    type_map = {
        'string':                                'TEXT',
        'int32':                                 'INT',
        'int64':                                 'BIGINT',
        'float32':                               'FLOAT',
        'float64':                               'DOUBLE PRECISION',
        'boolean':                               'BOOLEAN',
        'bytes':                                 'BYTEA',
        'io.debezium.time.MicroTimestamp':       'BIGINT',
        'io.debezium.time.ZonedTimestamp':       'BIGINT',
        'io.debezium.time.Timestamp':            'BIGINT',
        'org.apache.kafka.connect.data.Decimal': 'NUMERIC',
        'unknown':                               'TEXT'
    }
    return type_map.get(debezium_type, 'TEXT')


# ─── Build Migration SQL ──────────────────────────────────
def build_migration_sql(dest_table, change):
    if change['change_type'] == 'COLUMN_ADDED':
        col_type = map_debezium_type(change['new_type'])
        return (f"ALTER TABLE {dest_table} "
                f"ADD COLUMN IF NOT EXISTS "
                f"{change['column_name']} {col_type};")
    return None


# ─── Log Schema Change ────────────────────────────────────
def log_schema_change(cursor, table_name, source, change,
                      auto_migrated=False, migration_sql=None):
    cursor.execute("""
        INSERT INTO schema_change_log
            (table_name, source, change_type, column_name,
             old_type, new_type, detected_at, auto_migrated, migration_sql)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s)
    """, (
        table_name, source,
        change['change_type'], change['column_name'],
        change['old_type'], change['new_type'],
        auto_migrated, migration_sql
    ))


# ─── Log Schema Alert ─────────────────────────────────────
def log_schema_alert(cursor, table_name, change_type, details):
    cursor.execute("""
        INSERT INTO schema_change_alerts
            (table_name, change_type, details, detected_at)
        VALUES (%s, %s, %s, NOW())
    """, (table_name, change_type, details))


# ─── Main Schema Check Function ───────────────────────────
def check_and_update_schema(cursor, dest_cursor, table_name,
                             source, dest_table, debezium_message):
    new_schema = extract_schema(debezium_message)
    if not new_schema:
        return [], False

    current_schema, version, _ = get_current_schema(
        cursor, table_name, source
    )

    # first time — just register
    if current_schema is None:
        save_schema(cursor, table_name, source, new_schema, version=1)
        print(f"  [SCHEMA] Registered {table_name} ({source}) "
              f"— {len(new_schema)} columns v1")

        # check cross-source divergence
        divergences = check_cross_source_divergence(cursor, table_name)
        for d in divergences:
            log_divergence(cursor, table_name, d)

        return [], False

    # validate message against current schema
    validation_errors = validate_message(
        debezium_message.get('payload', {}).get('after', {}),
        current_schema
    )
    if validation_errors:
        for err in validation_errors:
            print(f"  [SCHEMA] VALIDATION ERROR: {err}")

    # auto-resolve any open alerts
    auto_resolve_alerts(cursor, table_name, new_schema)

    # diff schemas
    changes = diff_schemas(current_schema, new_schema)
    if not changes:
        return [], False

    print(f"\n  [SCHEMA] Change detected in {table_name} ({source})"
          f" — {len(changes)} change(s)")

    table_frozen  = False
    dangerous     = False

    for change in changes:
        print(f"  [SCHEMA] {change['change_type']}: "
              f"{change['column_name']} "
              f"({change['old_type']} → {change['new_type']})")

        if change['change_type'] == 'COLUMN_ADDED':
            migration_sql = build_migration_sql(dest_table, change)
            try:
                dest_cursor.execute(migration_sql)
                log_schema_change(cursor, table_name, source, change,
                                  auto_migrated=True,
                                  migration_sql=migration_sql)
                print(f"  [SCHEMA] Auto-migrated: {migration_sql}")

                # backfill existing rows with default value
                rows = backfill_new_column(
                    dest_cursor, dest_table,
                    change['column_name'],
                    default_value=0 if 'int' in change['new_type'].lower()
                                  else None
                )
                if rows > 0:
                    log_backfill(cursor, table_name,
                                 change['column_name'], rows)
                    print(f"  [SCHEMA] Backfilled {rows} rows "
                          f"with default value")

            except Exception as e:
                print(f"  [SCHEMA] Migration failed: {e}")
                log_schema_change(cursor, table_name, source,
                                  change, auto_migrated=False)

        elif change['change_type'] == 'COLUMN_REMOVED':
            dangerous = True
            log_schema_change(cursor, table_name, source, change)
            log_schema_alert(
                cursor, table_name, 'COLUMN_REMOVED',
                f"Column '{change['column_name']}' removed from "
                f"{table_name} in {source}. Manual review required."
            )
            freeze_table(
                cursor, table_name,
                f"COLUMN_REMOVED: {change['column_name']}"
            )
            table_frozen = True
            print(f"  [SCHEMA] FROZEN: {table_name} — "
                  f"dangerous change detected")

        elif change['change_type'] == 'TYPE_CHANGED':
            dangerous = True
            log_schema_change(cursor, table_name, source, change)
            log_schema_alert(
                cursor, table_name, 'TYPE_CHANGED',
                f"Column '{change['column_name']}' type changed "
                f"from '{change['old_type']}' to '{change['new_type']}'"
                f" in {table_name} ({source}). Manual review required."
            )
            freeze_table(
                cursor, table_name,
                f"TYPE_CHANGED: {change['column_name']} "
                f"{change['old_type']} → {change['new_type']}"
            )
            table_frozen = True

    # update registry
    save_schema(cursor, table_name, source, new_schema, version=version+1)
    print(f"  [SCHEMA] Registry updated to v{version+1}")

    # check cross-source divergence after update
    divergences = check_cross_source_divergence(cursor, table_name)
    for d in divergences:
        log_divergence(cursor, table_name, d)

    return changes, table_frozen