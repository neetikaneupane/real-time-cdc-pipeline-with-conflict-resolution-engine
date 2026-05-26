
# CDC Pipeline with Conflict Resolution Engine

A real-time Change Data Capture pipeline that streams database changes from PostgreSQL and MySQL into Kafka, detects conflicts when the same record is modified in both sources simultaneously, resolves them using pluggable strategies, and exposes pipeline health via a REST API and live dashboard.

---

## What This Project Does

Imagine a company with two regional databases: one in the US (PostgreSQL) and one in Europe (MySQL). A customer updates their email in both regions within seconds of each other. Both databases now have different values for the same record. Which one is correct?

This pipeline:
1. Captures every change from both databases in real time using CDC
2. Streams all changes into Kafka
3. Detects when the same record is modified in both sources within a time window
4. Resolves the conflict using a configurable strategy
5. Writes the resolved record to a destination database
6. Logs every conflict to a quarantine table for auditing
7. Exposes pipeline health via a REST API and live dashboard
8. Catches failed events in a Dead Letter Queue and retries them

---

## Architecture

```
PostgreSQL (US) ──► Debezium ──► Kafka topic source_a
MySQL (EU) ───────► Debezium ──► Kafka topic source_b
                                        │
                                        ▼
                              Python Consumer
                                        │
                              Conflict Detector
                              (30 second window)
                                        │
                         ┌─────────────┴─────────────┐
                      Conflict                    No Conflict
                         │                            │
                  Resolution Engine          customers_resolved
                  (pluggable strategy)
                      /       \
        customers_resolved  customers_quarantine
        (single truth)      (audit trail)
                                        │
                              Dead Letter Queue
                              (failed events)
                                        │
                              Reprocessor
                              (retries DLQ)
                                        │
                              REST API + Dashboard
                              (observability)
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Source Databases | PostgreSQL 15, MySQL 8.0 |
| CDC Layer | Debezium 2.4 |
| Message Broker | Apache Kafka |
| Stream Processor | Python, kafka-python |
| Destination DB | PostgreSQL (destination_db) |
| REST API | FastAPI, uvicorn |
| Dashboard | Jinja2 HTML templates |
| Scheduler | APScheduler |
| Infrastructure | Docker, Docker Compose |

---

## Project Structure

```
cdc-pipeline/
├── docker-compose.yml
├── postgres/
│   └── postgresql.conf
├── mysql/
│   └── my.cnf
├── init-scripts/
│   ├── postgres-init.sql
│   └── mysql-init.sql
└── consumer/
    ├── consumer.py        # main pipeline
    ├── reconciler.py      # scheduled audit job
    ├── reprocessor.py     # DLQ retry script
    ├── api.py             # REST API + dashboard
    ├── tables_config.yaml # table definitions
    └── templates/
        └── dashboard.html
```

---

## Prerequisites

- Docker Desktop
- Python 3.11+
- pip

---

## Setup

**1. Clone and enter the project:**
```bash
git clone <your-repo>
cd cdc-pipeline
```

**2. Start all infrastructure:**
```bash
docker-compose up -d
```

This starts PostgreSQL, MySQL, Zookeeper, Kafka, Kafka Connect, and Kafdrop.

**3. Wait 60 seconds for all containers to be healthy, then verify:**
```bash
docker-compose ps
```

**4. Create a Python virtual environment:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

**5. Install dependencies:**
```bash
pip install kafka-python psycopg2-binary fastapi uvicorn jinja2 apscheduler pymysql pyyaml
```

**6. Grant debezium superuser in PostgreSQL:**
```bash
docker exec -it postgres_source psql -U postgres -d source_us -c "ALTER USER debezium WITH SUPERUSER;"
```

**7. Register Debezium connectors:**
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres_source",
      "database.port": "5432",
      "database.user": "debezium",
      "database.password": "debezium",
      "database.dbname": "source_us",
      "database.server.name": "source_a",
      "table.include.list": "public.customers,public.orders",
      "plugin.name": "pgoutput",
      "topic.prefix": "source_a"
    }
  }'
```

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname": "mysql_source",
      "database.port": "3306",
      "database.user": "debezium",
      "database.password": "debezium",
      "database.server.id": "184054",
      "topic.prefix": "source_b",
      "database.include.list": "source_eu",
      "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
      "schema.history.internal.kafka.topic": "schema-changes.source_eu"
    }
  }'
```

---

## Running the Pipeline

Open four terminals, all inside the `consumer/` folder with the venv activated.

**Terminal 1 — Main consumer:**
```bash
cd consumer
python3 consumer.py
```

**Terminal 2 — Reconciliation auditor:**
```bash
cd consumer
python3 reconciler.py
```

**Terminal 3 — REST API and dashboard:**
```bash
cd consumer
uvicorn api:app --reload
```

**Terminal 4 — DLQ reprocessor (run manually when needed):**
```bash
cd consumer
python3 reprocessor.py
```

---

## Simulating Conflicts

**Insert into both databases simultaneously:**
```bash
docker exec -it postgres_source psql -U postgres -d source_us -c \
  "UPDATE customers SET email='pg@example.com', updated_at=NOW() WHERE customer_id='cust-001';"

docker exec -it mysql_source mysql -u root -proot source_eu -e \
  "UPDATE customers SET email='mysql@example.com', updated_at=NOW() WHERE customer_id='cust-001';"
```

The consumer will detect the conflict and resolve it using the configured strategy.

---

## Resolution Strategies

Configured per table in `tables_config.yaml`:

**`last_write_wins`**
Whoever has the latest `updated_at` timestamp wins. Simple but vulnerable to clock skew.

**`source_priority`**
SOURCE_A (PostgreSQL) always wins regardless of timestamp. Use when one source is always more trusted.

**`field_merge`**
Each field is resolved independently using its own rule:
- `latest` — field from whoever updated most recently
- `non_null` — field from whichever source has a value
- `source_a` — always use PostgreSQL's value
- `source_b` — always use MySQL's value
- `longest` — whichever value is longer

**Example config:**
```yaml
tables:
  - name: customers
    source_topic_a: source_a.public.customers
    source_topic_b: source_b.source_eu.customers
    primary_key: customer_id
    strategy: field_merge
    field_rules:
      email: latest
      phone: non_null
      name: source_a
    destination_table: customers_resolved

  - name: orders
    source_topic_a: source_a.public.orders
    source_topic_b: source_b.source_eu.orders
    primary_key: order_id
    strategy: source_priority
    destination_table: orders_resolved
```

---

## Adding a New Table

1. Add the table definition to `tables_config.yaml`
2. Create the table in both source databases
3. Create `<table>_resolved` and `<table>_quarantine` in `destination_db`
4. Restart `consumer.py`

No code changes needed.

---

## REST API

Base URL: `http://localhost:8000`

| Endpoint | Description |
|---|---|
| `GET /health` | Pipeline status |
| `GET /metrics` | Resolved count, conflict count, last activity |
| `GET /conflicts` | Full conflict audit log |
| `GET /customers/{id}` | Customer record with conflict history |
| `GET /dashboard` | Live HTML dashboard |
| `GET /docs` | Auto-generated Swagger UI |

---

## Dashboard

Open `http://localhost:8000/dashboard` in your browser.

Shows:
- Resolved records count
- Total conflicts
- Unique customers affected
- Records per table
- Recent conflicts with winner, loser, strategy, and timestamp

Auto-refreshes every 30 seconds.

---

## Observability

**Kafdrop** : Kafka UI at `http://localhost:9000`
Browse topics, view raw Debezium messages, inspect partitions and offsets.

**Reconciler** : runs every 1 minute
- Counts rows across all three databases
- Identifies missing customers in the resolved table
- Reports quarantine summary
- Prints overall health status

**Dead Letter Queue**
Failed events are written to `dead_letter_queue` table with error type, stack trace, retry count, and original event. Run `reprocessor.py` to retry retryable failures.

---

## Database Tables

**destination_db:**

| Table | Purpose |
|---|---|
| `customers_resolved` | Final resolved customer records |
| `customers_quarantine` | Conflict audit log for customers |
| `orders_resolved` | Final resolved order records |
| `orders_quarantine` | Conflict audit log for orders |
| `dead_letter_queue` | Failed events with error details |

---

## Key Concepts Demonstrated

- **Change Data Capture (CDC)** : tailing WAL and Binlog instead of polling
- **Exactly-at-least-once delivery** : Kafka offsets and replication slots
- **Conflict detection** : time-windowed state store per primary key
- **Pluggable resolution strategies** : config-driven, no code changes
- **Field level merge** : per-field resolution rules
- **Dead Letter Queue** : catching and retrying failed events
- **Data reconciliation** : periodic auditing of source vs destination
- **Schema evolution awareness** : Debezium schema registry per topic
- **Multi-table support** : YAML config drives entire pipeline

---

## Stopping the Pipeline

```bash
# Stop all containers
docker-compose down

# Stop and wipe all data (fresh start)
docker-compose down -v
```

