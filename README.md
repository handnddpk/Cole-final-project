# Session 1: Data Ingestion Setup Guide

## Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM available
- jq installed (for JSON formatting)
- curl installed

## Project Structure

Create the following directory structure:

```
D28 - FINAL PROJECT/
├── docker-compose.yml
├── sql/
│   └── init.sql
├── data-generator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── data_generator.py
├── debezium-config.json
├── kafka-scripts.sh
└── README.md
```

## Setup Instructions

### 1. Create Project Directory

```bash
mkdir -p D28 - FINAL PROJECT/sql D28 - FINAL PROJECT/data-generator
cd D28 - FINAL PROJECT
```

### 2. Copy Configuration Files

Copy all the provided files to their respective locations:

- `docker-compose.yml` → project root
- `init.sql` → `sql/init.sql`
- `debezium-config.json` → project root
- `data_generator.py` → `data-generator/data_generator.py`
- `Dockerfile` → `data-generator/Dockerfile`
- `requirements.txt` → `data-generator/requirements.txt`
- `kafka-scripts.sh` → project root

### 3. Make Scripts Executable

```bash
chmod +x kafka-scripts.sh
```

### 4. Start the Infrastructure

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 5. Wait for Services and Setup

```bash
# Complete setup (wait for services + create topics + deploy connector)
./kafka-scripts.sh setup-all
```

## Verification Steps

### 1. Check Service Health

```bash
# Check all services are running
docker-compose ps

# Check individual service logs
docker-compose logs postgres
docker-compose logs kafka
docker-compose logs kafka-connect
```

### 2. Verify Database Connection

```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U postgres -d taxi_db

# Check tables
\dt taxi.*

# Check sample data
SELECT COUNT(*) FROM taxi.trips;
```

### 3. Verify Kafka Topics

```bash
# List topics
./kafka-scripts.sh list-topics

# Check topic details
./kafka-scripts.sh describe-topic lakehouse.trips
```

### 4. Verify Debezium Connector

```bash
# Check connector status
./kafka-scripts.sh check-connector

# List all connectors
./kafka-scripts.sh list-connectors
```

### 5. Test Data Flow

```bash
# Insert test data
./kafka-scripts.sh db-insert-test

# Consume messages from Kafka (in separate terminal)
./kafka-scripts.sh consume-trips
```

## Running the Demo

### 1. Start Data Generator

The data generator should already be running via Docker Compose. Check logs:

```bash
docker-compose logs data-generator
```

### 2. Monitor Data Flow

In separate terminal windows:

**Terminal 1 - Monitor Kafka Messages:**
```bash
./kafka-scripts.sh consume-formatted
```

**Terminal 2 - Monitor Database:**
```bash
# Query recent trips every 10 seconds
watch -n 10 './kafka-scripts.sh db-query-recent'
```

**Terminal 3 - Monitor Connector:**
```bash
# Check connector status
./kafka-scripts.sh check-connector
```

### 3. Access UIs

- **Kafka UI**: http://localhost:8080
- **Kafka Connect REST API**: http://localhost:8083
- **PostgreSQL**: localhost:5432 (postgres/postgres)

## Common Operations

### Insert Test Data

```bash
# Insert single test record
./kafka-scripts.sh db-insert-test

# Or connect to database directly
docker exec -it postgres psql -U postgres -d taxi_db
```

### Monitor Message Flow

```bash
# Consume all messages from beginning
./kafka-scripts.sh consume-trips

# Consume with JSON formatting
./kafka-scripts.sh consume-formatted

# Monitor consumer lag
./kafka-scripts.sh monitor-lag
```

### Restart Connector

```bash
# Delete and recreate connector
./kafka-scripts.sh delete-connector
sleep 5
./kafka-scripts.sh deploy-connector
```

## Troubleshooting

### Services Not Starting

```bash
# Check Docker resources
docker system df
docker system prune

# Restart specific service
docker-compose restart kafka-connect
```

### Connector Issues

```bash
# Check connector logs
docker-compose logs kafka-connect

# Check connector status
curl -s http://localhost:8083/connectors/taxi-postgres-connector/status | jq '.'

# Check connector config
curl -s http://localhost:8083/connectors/taxi-postgres-connector/config | jq '.'
```

### Database Issues

```bash
# Check PostgreSQL logs
docker-compose logs postgres

# Check replication slot
docker exec postgres psql -U postgres -d taxi_db -c "SELECT * FROM pg_replication_slots;"

# Check publication
docker exec postgres psql -U postgres -d taxi_db -c "SELECT * FROM pg_publication;"
```

### No Messages in Kafka

```bash
# Check topic has data
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic lakehouse.trips

# Check consumer groups
docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --list
```

## Learning Exercises

### Exercise 1: Understanding CDC
1. Insert a new trip in PostgreSQL
2. Observe the CDC event in Kafka
3. Update the trip record
4. Observe the update event

### Exercise 2: Schema Changes
1. Add a new column to the trips table
2. Observe how Debezium handles schema evolution
3. Insert data with the new column

### Exercise 3: Performance Testing
1. Increase the data generator batch size
2. Monitor Kafka lag and throughput
3. Observe connector performance

### Exercise 4: Error Handling
1. Stop PostgreSQL temporarily
2. Observe connector behavior
3. Restart PostgreSQL and check recovery

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove all data (optional)
docker-compose down -v

# Clean up Docker resources
docker system prune -f
```

## Next Steps

After completing Session 1, you should have:
- Running Kafka cluster with Debezium
- PostgreSQL with logical replication enabled
- Real-time data ingestion pipeline
- Understanding of CDC concepts
- Monitoring and troubleshooting skills

This foundation will be used in Session 2 for data transformation with Spark and Iceberg.