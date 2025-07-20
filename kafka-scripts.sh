#!/bin/bash
# Kafka Management Scripts for Session 1

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Wait for services to be ready
wait_for_services() {
    print_status "Waiting for services to be ready..."
    
    # Wait for Kafka
    print_status "Waiting for Kafka..."
    while ! docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; do
        echo -n "."
        sleep 2
    done
    echo ""
    print_status "Kafka is ready!"
    
    # Wait for Kafka Connect
    print_status "Waiting for Kafka Connect..."
    while ! curl -s http://localhost:8083/connectors &>/dev/null; do
        echo -n "."
        sleep 2
    done
    echo ""
    print_status "Kafka Connect is ready!"
    
    # Wait for PostgreSQL
    print_status "Waiting for PostgreSQL..."
    while ! docker exec postgres pg_isready -U postgres &>/dev/null; do
        echo -n "."
        sleep 2
    done
    echo ""
    print_status "PostgreSQL is ready!"
}

# Create Kafka topics
create_topics() {
    print_status "Creating Kafka topics..."
    
    # Create topics for Debezium
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 3 \
        --topic lakehouse.trips \
        --if-not-exists
    
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1 \
        --topic lakehouse.payment_types \
        --if-not-exists
    
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1 \
        --topic lakehouse.vendors \
        --if-not-exists
    
    # Create connector management topics
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1 \
        --topic my_connect_configs \
        --if-not-exists
    
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1 \
        --topic my_connect_offsets \
        --if-not-exists
    
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1 \
        --topic my_connect_statuses \
        --if-not-exists
    
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1 \
        --topic taxi.schema-changes \
        --if-not-exists
    
    print_status "Topics created successfully!"
}

# List all topics
list_topics() {
    print_status "Listing Kafka topics..."
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
}

# Deploy Debezium connector
deploy_connector() {
    print_status "Deploying Debezium PostgreSQL connector..."
    
    curl -X POST http://localhost:8083/connectors \
        -H "Content-Type: application/json" \
        -d @debezium-config.json
    
    if [ $? -eq 0 ]; then
        print_status "Connector deployed successfully!"
    else
        print_error "Failed to deploy connector"
        exit 1
    fi
}

# Check connector status
check_connector() {
    print_status "Checking connector status..."
    curl -s http://localhost:8083/connectors/taxi-postgres-connector/status | jq '.'
}

# List connectors
list_connectors() {
    print_status "Listing connectors..."
    curl -s http://localhost:8083/connectors | jq '.'
}

# Delete connector
delete_connector() {
    print_warning "Deleting connector..."
    curl -X DELETE http://localhost:8083/connectors/taxi-postgres-connector
    print_status "Connector deleted!"
}

# Consumer test for trips topic
consume_trips() {
    print_status "Consuming messages from lakehouse.trips topic (Press Ctrl+C to stop)..."
    docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic lakehouse.trips \
        --from-beginning \
        --property print.key=true \
        --property key.separator=" | "
}

# Consumer test with JSON formatting
consume_trips_formatted() {
    print_status "Consuming formatted messages from lakehouse.trips topic (Press Ctrl+C to stop)..."
    docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic lakehouse.trips \
        --from-beginning \
        --property print.key=true \
        --property key.separator=" | " | jq '.'
}

# Show topic details
describe_topic() {
    local topic=${1:-lakehouse.trips}
    print_status "Describing topic: $topic"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic $topic
}

# Reset consumer group
reset_consumer_group() {
    local group=${1:-test-group}
    print_warning "Resetting consumer group: $group"
    docker exec kafka kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --group $group \
        --reset-offsets \
        --to-earliest \
        --all-topics \
        --execute
}

# Get consumer group info
consumer_group_info() {
    local group=${1:-test-group}
    print_status "Consumer group info: $group"
    docker exec kafka kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --group $group \
        --describe
}

# Monitor lag
monitor_lag() {
    print_status "Monitoring consumer lag..."
    while true; do
        docker exec kafka kafka-consumer-groups \
            --bootstrap-server localhost:9092 \
            --group debezium \
            --describe
        sleep 5
        clear
    done
}

# Produce test message
produce_test_message() {
    print_status "Producing test message to lakehouse.trips topic..."
    echo '{"test": "message", "timestamp": "'$(date -Iseconds)'"}' | \
    docker exec -i kafka kafka-console-producer \
        --bootstrap-server localhost:9092 \
        --topic lakehouse.trips
    print_status "Test message sent!"
}

# Database operations
db_insert_test_data() {
    print_status "Inserting test data into PostgreSQL..."
    docker exec postgres psql -U postgres -d taxi_db -c "
    INSERT INTO taxi.trips (
        vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
        pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount
    ) VALUES (
        1, NOW(), NOW() + INTERVAL '15 minutes', 2, 3.5,
        -73.9857, 40.7484, -73.9857, 40.7589,
        1, 15.0, 0.5, 0.5, 3.0, 0.0, 19.0
    );
    "
    print_status "Test data inserted!"
}

# Database query
db_query_recent() {
    print_status "Querying recent trips from PostgreSQL..."
    docker exec postgres psql -U postgres -d taxi_db -c "
    SELECT id, vendor_id, pickup_datetime, fare_amount, created_at 
    FROM taxi.trips 
    ORDER BY created_at DESC 
    LIMIT 10;
    "
}

# Show help
show_help() {
    echo "Kafka Management Scripts for Session 1"
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  wait-services       - Wait for all services to be ready"
    echo "  create-topics       - Create necessary Kafka topics"
    echo "  list-topics         - List all Kafka topics"
    echo "  deploy-connector    - Deploy Debezium connector"
    echo "  check-connector     - Check connector status"
    echo "  list-connectors     - List all connectors"
    echo "  delete-connector    - Delete the connector"
    echo "  consume-trips       - Consume messages from trips topic"
    echo "  consume-formatted   - Consume messages with JSON formatting"
    echo "  describe-topic      - Describe topic details"
    echo "  reset-consumer      - Reset consumer group offsets"
    echo "  consumer-info       - Show consumer group information"
    echo "  monitor-lag         - Monitor consumer lag"
    echo "  produce-test        - Produce test message"
    echo "  db-insert-test      - Insert test data into database"
    echo "  db-query-recent     - Query recent trips from database"
    echo "  setup-all           - Complete setup (wait + create topics + deploy connector)"
    echo "  help                - Show this help message"
}

# Complete setup
setup_all() {
    print_status "Starting complete setup..."
    wait_for_services
    create_topics
    sleep 5
    deploy_connector
    print_status "Setup completed! You can now start monitoring with: ./kafka-scripts.sh consume-trips"
}

# Main script logic
case "${1:-help}" in
    wait-services)
        wait_for_services
        ;;
    create-topics)
        create_topics
        ;;
    list-topics)
        list_topics
        ;;
    deploy-connector)
        deploy_connector
        ;;
    check-connector)
        check_connector
        ;;
    list-connectors)
        list_connectors
        ;;
    delete-connector)
        delete_connector
        ;;
    consume-trips)
        consume_trips
        ;;
    consume-formatted)
        consume_trips_formatted
        ;;
    describe-topic)
        describe_topic $2
        ;;
    reset-consumer)
        reset_consumer_group $2
        ;;
    consumer-info)
        consumer_group_info $2
        ;;
    monitor-lag)
        monitor_lag
        ;;
    produce-test)
        produce_test_message
        ;;
    db-insert-test)
        db_insert_test_data
        ;;
    db-query-recent)
        db_query_recent
        ;;
    setup-all)
        setup_all
        ;;
    help)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac