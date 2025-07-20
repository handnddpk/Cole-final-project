#!/bin/bash

# Setup script for Kafka Connect connectors
# This script sets up Debezium CDC and Iceberg sink connectors

set -e

KAFKA_CONNECT_URL="http://localhost:8083"

echo "Setting up Kafka Connect connectors..."

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
until curl -f "$KAFKA_CONNECT_URL" > /dev/null 2>&1; do
    echo "Kafka Connect not ready yet, waiting 10 seconds..."
    sleep 10
done

echo "Kafka Connect is ready!"

# Function to create or update connector
create_connector() {
    local config_file=$1
    local connector_name=$(jq -r '.name' "$config_file")
    
    echo "Setting up connector: $connector_name"
    
    # Check if connector exists
    if curl -f "$KAFKA_CONNECT_URL/connectors/$connector_name" > /dev/null 2>&1; then
        echo "Connector $connector_name exists, updating..."
        curl -X PUT \
            -H "Content-Type: application/json" \
            --data @"$config_file" \
            "$KAFKA_CONNECT_URL/connectors/$connector_name/config"
    else
        echo "Creating new connector: $connector_name"
        curl -X POST \
            -H "Content-Type: application/json" \
            --data @"$config_file" \
            "$KAFKA_CONNECT_URL/connectors"
    fi
    
    echo "Connector $connector_name setup complete"
    echo ""
}

# Setup Debezium PostgreSQL connector
echo "=== Setting up Debezium PostgreSQL Connector ==="
create_connector "debezium-config.json"

# Setup Iceberg sink connector
echo "=== Setting up Iceberg Sink Connector ==="
create_connector "iceberg-sink-config.json"

# List all connectors
echo "=== Current Connectors ==="
curl -s "$KAFKA_CONNECT_URL/connectors" | jq .

# Check connector status
echo ""
echo "=== Connector Status ==="
for connector in $(curl -s "$KAFKA_CONNECT_URL/connectors" | jq -r '.[]'); do
    echo "Connector: $connector"
    curl -s "$KAFKA_CONNECT_URL/connectors/$connector/status" | jq .
    echo ""
done

echo "Kafka Connect setup complete!"

echo ""
echo "=== Available Kafka Topics ==="
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "=== Connector Management Commands ==="
echo "List connectors: curl $KAFKA_CONNECT_URL/connectors"
echo "Check status: curl $KAFKA_CONNECT_URL/connectors/<name>/status"
echo "Delete connector: curl -X DELETE $KAFKA_CONNECT_URL/connectors/<name>"
echo "Restart connector: curl -X POST $KAFKA_CONNECT_URL/connectors/<name>/restart"
