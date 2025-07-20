# NYC Taxi Data Pipeline with Airflow and Iceberg

This project demonstrates an end-to-end data pipeline for processing NYC taxi data using Apache Airflow, Apache Spark, and Apache Iceberg.

## Architecture

- **Apache Kafka**: Stream processing and CDC from PostgreSQL
- **Apache Airflow**: Workflow orchestration
- **Apache Spark**: Data processing engine
- **Apache Iceberg**: Modern table format for analytics
- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Source database and Airflow metadata store

## Getting Started

### Prerequisites

- Docker and Docker Compose
- At least 8GB RAM available for containers
- 10GB+ free disk space

### Setup Instructions

1. **Start the infrastructure:**
   ```bash
   docker-compose up -d
   ```

2. **Download required JAR files for Spark:**
   ```bash
   ./airflow/download_jars.sh
   ```

3. **Create MinIO bucket for Iceberg warehouse:**
   - Access MinIO UI at http://localhost:9001
   - Login with admin/password
   - Create a bucket named "lakehouse"

4. **Access Airflow UI:**
   - URL: http://localhost:8080
   - Username: admin
   - Password: admin

5. **Verify setup:**
   - Run the "test_setup" DAG to verify all components are working

### DAGs Available

#### 1. NYC Taxi Iceberg ETL (`nyc_taxi_iceberg_etl`)
- **Purpose**: Incrementally loads NYC taxi data into Iceberg tables
- **Schedule**: Every 6 hours
- **Features**:
  - Downloads public NYC taxi data
  - Data validation and cleaning
  - Incremental processing with state management
  - Writes to partitioned Iceberg tables
  - Creates daily aggregation tables

#### 2. Test Setup (`test_setup`)
- **Purpose**: Validates the infrastructure setup
- **Schedule**: Manual trigger only
- **Tests**: Python, Bash, Spark connectivity, MinIO connectivity

### Data Flow

1. **Extract**: Downloads NYC taxi data from public dataset or generates sample data
2. **Validate**: Performs data quality checks
3. **Transform**: Cleans and enriches the data using Spark
4. **Load**: Writes data to Iceberg tables with partitioning
5. **Aggregate**: Creates summary tables for analytics

### Monitoring and Management

- **Airflow UI**: http://localhost:8080 - DAG management and monitoring
- **Spark UI**: http://localhost:8070 - Spark job monitoring
- **MinIO Console**: http://localhost:9001 - Object storage management
- **Kafka Control Center**: http://localhost:9021 - Kafka monitoring

### Configuration

#### Environment Variables
- `DATABASE_URL`: PostgreSQL connection for source data
- `BATCH_SIZE`: Number of records to process in each batch
- `INTERVAL_SECONDS`: Data generation interval

#### Spark Configuration
The DAG includes optimized Spark configurations for:
- Iceberg integration
- S3A file system for MinIO
- Compression and performance tuning

#### Iceberg Tables

**trips** table schema:
- `vendor_id`: Taxi vendor identifier
- `pickup_datetime`, `dropoff_datetime`: Trip timestamps
- `passenger_count`: Number of passengers
- `trip_distance`: Trip distance in miles
- `pickup_location_id`, `dropoff_location_id`: NYC location IDs
- `fare_amount`, `tip_amount`, `total_amount`: Monetary amounts
- `year`, `month`: Partition columns

**daily_summary** table:
- Aggregated daily statistics
- Trip counts, averages, totals by date

### Troubleshooting

#### Common Issues

1. **Container startup failures**:
   - Check available memory (need 8GB+)
   - Verify ports are not in use
   - Check Docker logs: `docker-compose logs <service-name>`

2. **Airflow DAG import errors**:
   - Check Python dependencies in entrypoint.sh
   - Verify DAG syntax
   - Check Airflow logs: `docker-compose logs airflow-scheduler`

3. **Spark job failures**:
   - Verify JAR files are downloaded
   - Check Spark master connectivity
   - Review Spark logs in Airflow task logs

4. **MinIO connection issues**:
   - Ensure bucket "lakehouse" exists
   - Verify credentials (admin/password)
   - Check network connectivity between containers

#### Useful Commands

```bash
# View logs for all services
docker-compose logs -f

# View logs for specific service
docker-compose logs -f airflow-webserver

# Restart a specific service
docker-compose restart airflow-scheduler

# Scale Spark workers
docker-compose up -d --scale spark-worker=3

# Clean up and restart everything
docker-compose down -v
docker-compose up -d
```

### Development

#### Adding New DAGs
1. Place DAG files in `./airflow/dags/`
2. Restart scheduler: `docker-compose restart airflow-scheduler`
3. DAGs will appear in Airflow UI within 30 seconds

#### Modifying Spark Jobs
1. Update files in `./airflow/dags/spark_jobs/`
2. Jobs are picked up automatically on next DAG run

#### Data Location
- **Raw data**: `./data/raw/`
- **Iceberg warehouse**: MinIO bucket `s3a://lakehouse/warehouse`
- **Logs**: `./airflow/logs/`

## Performance Tuning

### For Production Use

1. **Increase resources**:
   - Add more Spark workers
   - Increase memory allocation
   - Use SSD storage for MinIO

2. **Optimize Iceberg**:
   - Tune partition strategy
   - Configure compaction schedules
   - Enable file size optimization

3. **Scale Airflow**:
   - Use external PostgreSQL
   - Add more workers
   - Configure resource pools

4. **Monitoring**:
   - Set up Prometheus/Grafana
   - Configure alerts
   - Monitor data freshness

## License

This project is for demonstration purposes. NYC taxi data is provided by the NYC Taxi and Limousine Commission.
