# Comprehensive Data Pipeline with Kafka CDC and Iceberg

This project demonstrates a complete modern data pipeline integrating:
- **Kafka CDC (Change Data Capture)** with Debezium
- **Apache Iceberg** for lakehouse architecture
- **Apache Airflow** for orchestration
- **Apache Spark** for data processing
- **Real-time and batch analytics**

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │───▶│   Debezium      │───▶│     Kafka       │
│   (Taxi Data)   │    │   Connector     │    │   (CDC Stream)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐             ▼
│  Weather API    │───▶│    Airflow      │    ┌─────────────────┐
│  (Batch Data)   │    │     DAGs        │    │   Iceberg       │
└─────────────────┘    └─────────────────┘    │   Connector     │
                                │              └─────────────────┘
                                ▼                        │
                       ┌─────────────────┐              ▼
                       │     Spark       │    ┌─────────────────┐
                       │   Processing    │◀───│   Iceberg       │
                       └─────────────────┘    │   Tables        │
                                │              └─────────────────┘
                                ▼
                       ┌─────────────────┐
                       │   Analytics     │
                       │   & ML Tables   │
                       └─────────────────┘
```

## Data Sources and Processing

### 1. Real-time CDC Stream (Kafka + Debezium)
- **Source**: PostgreSQL taxi database with CDC enabled
- **Topics**: `lakehouse.trips`, `lakehouse.payment_types`, `lakehouse.vendors`
- **Processing**: Real-time stream processing with Spark Structured Streaming
- **Destination**: Iceberg tables in `taxi_cdc` namespace

### 2. Batch Weather Data (Airflow)
- **Source**: Weather API (or synthetic data for demo)
- **Schedule**: Hourly updates
- **Processing**: Batch processing with Spark
- **Destination**: Iceberg tables in `weather` namespace

### 3. NYC Taxi Trip Data (Airflow)
- **Source**: Public NYC taxi dataset (or generated data)
- **Schedule**: Every 6 hours
- **Processing**: Incremental batch processing
- **Destination**: Iceberg tables in `nyc_taxi` namespace

## Data Flow and Pipelines

### DAG 1: NYC Taxi ETL (`nyc_taxi_iceberg_etl`)
```python
# Extracts public NYC taxi data
extract_taxi_data → validate_data → process_to_iceberg → update_control_table
```

### DAG 2: Weather ETL (`nyc_weather_etl`) 
```python
# Fetches weather data and location references
[fetch_weather, create_locations] → validate → [weather_to_iceberg, location_to_iceberg]
```

### DAG 3: Comprehensive Analytics (`comprehensive_analytics_pipeline`)
```python
# Combines all data sources for analytics
wait_for_sources → check_availability → [analytics_transform, realtime_cdc] → ml_features
```

## Iceberg Table Structure

### Core Data Tables
- `iceberg.nyc_taxi.trips` - Partitioned by (year, month)
- `iceberg.weather.hourly_weather` - Partitioned by (year, month, day)
- `iceberg.reference.taxi_zones` - Reference data
- `iceberg.taxi_cdc.trips_cdc` - Real-time CDC data partitioned by hour

### Analytics Tables
- `iceberg.analytics.trip_weather_correlation` - Trip patterns vs weather
- `iceberg.analytics.zone_performance_metrics` - Zone-level KPIs
- `iceberg.analytics.demand_prediction_features` - ML features for demand prediction

### Real-time Tables
- `iceberg.realtime.trip_aggregations` - 5-minute windowed aggregations
- `iceberg.realtime.zone_activity` - Real-time zone activity scores

### ML Feature Tables
- `iceberg.ml.demand_prediction_features` - Features for demand forecasting
- `iceberg.ml.fare_prediction_features` - Features for fare prediction

## Key Features

### 1. **Change Data Capture (CDC)**
- Real-time capture of database changes using Debezium
- Transforms CDC events to Iceberg format
- Handles INSERT, UPDATE, DELETE operations
- Maintains data lineage and audit trail

### 2. **Lakehouse Architecture**
- Apache Iceberg for ACID transactions on data lakes
- Time travel and schema evolution capabilities
- Optimized for both batch and streaming workloads
- Efficient partitioning and compaction

### 3. **Multi-Source Data Integration**
- Combines real-time CDC streams with batch data
- Weather data correlation with taxi demand
- Location-based analytics and insights
- Cross-source data quality validation

### 4. **Advanced Analytics**
- Weather impact analysis on taxi demand
- Zone performance metrics and rankings
- Peak hour analysis and surge pricing insights
- Demand prediction feature engineering

### 5. **Real-time Processing**
- Spark Structured Streaming for low-latency processing
- Windowed aggregations (5-minute, hourly)
- Real-time alerting and monitoring capabilities
- Stream-batch unified processing

## Setup and Deployment

### Prerequisites
```bash
# System requirements
- Docker & Docker Compose
- 8GB+ RAM
- 10GB+ disk space
- jq (for JSON processing)
```

### Quick Start
```bash
# 1. Start all services
docker-compose up -d

# 2. Download required JAR files
./airflow/download_jars.sh

# 3. Setup Kafka connectors
./setup-connectors.sh

# 4. Create MinIO bucket
# Access http://localhost:9001 (admin/password)
# Create bucket: lakehouse

# 5. Access Airflow
# http://localhost:8080 (admin/admin)
```

### Service URLs
- **Airflow UI**: http://localhost:8080
- **Spark Master**: http://localhost:8070
- **MinIO Console**: http://localhost:9001
- **Kafka Control Center**: http://localhost:9021
- **Kafka Connect API**: http://localhost:8083
- **Analytics API**: http://localhost:8000
- **Analytics Dashboard**: http://localhost:8501

## Data Pipeline Execution

### 1. Initial Data Load
```bash
# Trigger initial data loads
airflow dags trigger nyc_taxi_iceberg_etl
airflow dags trigger nyc_weather_etl
```

### 2. Enable CDC
```bash
# Setup database CDC
./setup-connectors.sh

# Verify connectors
curl http://localhost:8083/connectors
```

### 3. Start Analytics Pipeline
```bash
# Run comprehensive analytics
airflow dags trigger comprehensive_analytics_pipeline
```

### 4. Monitor Real-time Processing
```bash
# Check Spark streaming jobs
# Access Spark UI at http://localhost:8070

# Monitor Kafka topics
./kafka-scripts.sh consume lakehouse.trips
```

## Analytics Use Cases

### 1. **Demand Forecasting**
```sql
-- Predict taxi demand by zone and hour
SELECT 
    location_id,
    prediction_hour,
    predicted_demand,
    weather_impact_factor,
    confidence_score
FROM iceberg.ml.demand_prediction_features
WHERE prediction_hour >= current_timestamp()
```

### 2. **Weather Impact Analysis**
```sql
-- Analyze weather impact on taxi demand
SELECT 
    weather_condition,
    AVG(total_trips) as avg_trips,
    AVG(avg_fare_amount) as avg_fare,
    CORR(temperature_celsius, total_trips) as temp_correlation
FROM iceberg.analytics.trip_weather_correlation
GROUP BY weather_condition
```

### 3. **Zone Performance Ranking**
```sql
-- Top performing zones by revenue
SELECT 
    zone_name,
    borough,
    SUM(total_pickups * avg_fare_per_pickup) as total_revenue,
    AVG(peak_hour_factor) as avg_peak_factor,
    AVG(weather_impact_score) as weather_sensitivity
FROM iceberg.analytics.zone_performance_metrics
WHERE trip_date >= current_date() - interval 7 days
GROUP BY zone_name, borough
ORDER BY total_revenue DESC
```

### 4. **Real-time Activity Monitoring**
```sql
-- Current zone activity levels
SELECT 
    zone_id,
    activity_timestamp,
    activity_score,
    revenue_last_hour,
    pickup_count
FROM iceberg.realtime.zone_activity
WHERE activity_timestamp >= current_timestamp() - interval 1 hour
ORDER BY activity_score DESC
```

## Data Quality and Monitoring

### Built-in Data Quality Checks
- **Schema validation** for all incoming data
- **Null value detection** and handling
- **Referential integrity** between tables
- **Time series continuity** checks
- **Outlier detection** for numerical fields

### Monitoring Dashboards
- Real-time data ingestion rates
- Pipeline execution status
- Data freshness metrics
- Error rates and alerts
- Resource utilization

## Scaling and Performance

### Horizontal Scaling
- **Kafka**: Add more brokers and partitions
- **Spark**: Scale workers for processing power
- **Airflow**: Add worker nodes for task parallelism
- **Iceberg**: Partition optimization and compaction

### Performance Optimization
- **Iceberg**: Columnar storage with compression
- **Spark**: Adaptive query execution and caching
- **Kafka**: Batch processing and compression
- **MinIO**: Multi-node deployment for throughput

## Troubleshooting

### Common Issues
1. **Memory Issues**: Increase Docker memory allocation
2. **Connector Failures**: Check database permissions and connectivity
3. **Spark Job Failures**: Verify JAR dependencies and configurations
4. **Data Lag**: Monitor Kafka consumer lag and processing times

### Debug Commands
```bash
# Check connector status
curl http://localhost:8083/connectors/taxi-postgres-connector/status

# Monitor Kafka topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# View Airflow logs
docker-compose logs airflow-scheduler

# Check Spark job logs
# Access through Spark UI or Airflow task logs
```

## Future Enhancements

### 1. **Machine Learning Models**
- Implement demand prediction models
- Fare optimization algorithms
- Route recommendation systems
- Anomaly detection for fraud

### 2. **Advanced Analytics**
- Geospatial analysis with PostGIS
- Time series forecasting
- Customer segmentation
- Revenue optimization

### 3. **Operational Improvements**
- Data lineage tracking
- Auto-scaling policies
- Cost optimization
- Security hardening

### 4. **Integration Extensions**
- REST APIs for analytics
- Real-time dashboards
- Mobile applications
- Third-party integrations

## Serving Layer APIs and Dashboards

### REST API (FastAPI)
The analytics API provides programmatic access to all pipeline data:

**Key Endpoints:**
- `GET /api/v1/dashboard/stats` - Real-time dashboard metrics
- `GET /api/v1/trips/recent` - Recent trip data
- `GET /api/v1/analytics/zones` - Zone performance analysis
- `GET /api/v1/analytics/weather-impact` - Weather correlation data
- `GET /api/v1/predictions/demand` - ML-powered demand forecasting
- `GET /api/v1/realtime/activity` - Live zone activity monitoring

**Access:** http://localhost:8000 (API), http://localhost:8000/docs (Documentation)

### Interactive Dashboard (Streamlit)
Comprehensive web dashboard for data exploration and visualization:

**Features:**
- Real-time KPIs and performance metrics
- Interactive time series charts
- Zone performance analysis with maps
- Weather impact visualization
- Demand prediction charts
- Real-time activity monitoring
- Data export capabilities

**Access:** http://localhost:8501

### Python Client Library
Easy-to-use Python SDK for API integration:

```python
from serving.client import LakehouseClient

client = LakehouseClient()
stats = client.get_dashboard_stats()
trips_df = client.get_recent_trips(limit=100)
predictions_df = client.get_demand_predictions(hours_ahead=24)
```

### Service URLs (Updated)
- **Airflow UI**: http://localhost:8080
- **Analytics API**: http://localhost:8000
- **Analytics Dashboard**: http://localhost:8501
- **Spark Master**: http://localhost:8070
- **MinIO Console**: http://localhost:9001
- **Kafka Control Center**: http://localhost:9021
