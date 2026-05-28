#!/bin/bash

echo "Generating conflicts..."

CUSTOMERS=("cust-001" "cust-002" "cust-003" "cust-004" "cust-005")

for i in {1..20}
do
    # Pick a random customer
    CUST=${CUSTOMERS[$((RANDOM % 5))]}

    docker exec postgres_source psql -U postgres -d source_us -c \
        "UPDATE customers SET email='pg_${i}@example.com', updated_at=NOW() WHERE customer_id='${CUST}';" > /dev/null 2>&1

    sleep 0.5

    docker exec mysql_source mysql -u root -proot source_eu -e \
        "UPDATE customers SET email='mysql_${i}@example.com', updated_at=NOW() WHERE customer_id='${CUST}';" > /dev/null 2>&1

    echo "Conflict $i triggered for $CUST"
    sleep 1
done

echo "Done — 20 conflicts triggered"