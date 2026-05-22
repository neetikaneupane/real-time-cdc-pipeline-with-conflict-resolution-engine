-- Create a dedicated replication user for Debezium
CREATE USER debezium WITH PASSWORD 'debezium' REPLICATION LOGIN;

-- Create your database
CREATE DATABASE source_us;
\c source_us;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE source_us TO debezium;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO debezium;

-- Create the table
CREATE TABLE customers (
    customer_id   VARCHAR(36) PRIMARY KEY,
    name          VARCHAR(255),
    email         VARCHAR(255),
    phone         VARCHAR(50),
    updated_at    TIMESTAMP DEFAULT NOW(),
    source_region VARCHAR(10) DEFAULT 'US'
);

-- Grant replication access on the table
ALTER TABLE customers REPLICA IDENTITY FULL;

GRANT SELECT ON TABLE customers TO debezium;