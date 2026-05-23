-- Create the table
CREATE TABLE source_eu.customers (
    customer_id   VARCHAR(36) PRIMARY KEY,
    name          VARCHAR(255),
    email         VARCHAR(255),
    phone         VARCHAR(50),
    updated_at    TIMESTAMP DEFAULT NOW(),
    source_region VARCHAR(10) DEFAULT 'EU'
);

-- Create Debezium user with required privileges
CREATE USER 'debezium'@'%' IDENTIFIED BY 'debezium';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;