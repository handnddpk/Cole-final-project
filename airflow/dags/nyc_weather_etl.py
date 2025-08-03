"""
NYC Weather Data ETL DAG - Complementary data source for taxi analysis
This DAG extracts weather data and loads it to Iceberg tables for correlation with taxi trips
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    import pandas as pd
    import requests
    import json
except ImportError:
    print("pandas or requests not available")

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'nyc_weather_etl',
    default_args=default_args,
    description='ETL for NYC Weather data to complement taxi analysis',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'iceberg', 'spark', 'etl'],
)

def fetch_weather_data(**context):
    """Fetch weather data from OpenWeatherMap API or generate synthetic data"""
    import tempfile
    import random
    from datetime import datetime, timedelta
    
    # Create data directory if it doesn't exist
    data_dir = "/opt/airflow/data/weather"
    os.makedirs(data_dir, exist_ok=True)
    
    # For demo purposes, generate synthetic weather data
    # In production, you'd use actual weather API
    current_time = datetime.now()
    
    # Generate hourly weather data for the last 24 hours
    weather_data = []
    for i in range(24):
        timestamp = current_time - timedelta(hours=i)
        
        # Simulate realistic NYC weather patterns
        base_temp = 20 + random.uniform(-15, 15)  # Celsius
        humidity = random.uniform(30, 90)
        pressure = random.uniform(1000, 1030)
        wind_speed = random.uniform(0, 15)
        
        # Weather conditions with probabilities
        conditions = ['clear', 'cloudy', 'rain', 'snow', 'fog']
        probabilities = [0.4, 0.3, 0.15, 0.1, 0.05]
        weather_condition = random.choices(conditions, probabilities)[0]
        
        # Adjust temperature based on condition
        if weather_condition == 'snow':
            base_temp = min(base_temp, 0)
        elif weather_condition == 'rain':
            base_temp *= 0.9
            humidity = max(humidity, 70)
        
        weather_record = {
            'timestamp': timestamp.isoformat(),
            'location': 'NYC',
            'latitude': 40.7128,
            'longitude': -74.0060,
            'temperature_celsius': round(base_temp, 2),
            'temperature_fahrenheit': round(base_temp * 9/5 + 32, 2),
            'humidity_percent': round(humidity, 1),
            'pressure_hpa': round(pressure, 1),
            'wind_speed_kmh': round(wind_speed, 1),
            'weather_condition': weather_condition,
            'visibility_km': round(random.uniform(1, 15), 1),
            'uv_index': max(0, round(random.uniform(0, 8), 1)) if 6 <= timestamp.hour <= 18 else 0,
            'hour': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'is_weekend': timestamp.weekday() >= 5
        }
        weather_data.append(weather_record)
    
    # Save as JSON
    output_path = f"{data_dir}/weather_data_{current_time.strftime('%Y%m%d_%H')}.json"
    with open(output_path, 'w') as f:
        json.dump(weather_data, f, indent=2)
    
    print(f"Generated weather data: {len(weather_data)} records at {output_path}")
    
    # Store file path in XCom
    context['task_instance'].xcom_push(key='weather_data_path', value=output_path)
    return output_path

def validate_weather_data(**context):
    """Validate the weather data"""
    data_path = context['task_instance'].xcom_pull(task_ids='fetch_weather_data', key='weather_data_path')
    
    with open(data_path, 'r') as f:
        data = json.load(f)
    
    # Basic validation checks
    assert len(data) > 0, "Weather dataset is empty"
    assert all('timestamp' in record for record in data), "Missing timestamp in weather records"
    assert all('temperature_celsius' in record for record in data), "Missing temperature data"
    
    print(f"Weather data validation passed. {len(data)} records ready for processing.")
    
    # Store validated path
    context['task_instance'].xcom_push(key='validated_weather_path', value=data_path)

def create_location_data(**context):
    """Create NYC location/zone reference data"""
    data_dir = "/opt/airflow/data/locations"
    os.makedirs(data_dir, exist_ok=True)
    
    # NYC taxi zone data (simplified)
    zones = [
        {'location_id': 1, 'borough': 'Manhattan', 'zone': 'Financial District', 'lat': 40.7074, 'lon': -74.0113},
        {'location_id': 4, 'borough': 'Manhattan', 'zone': 'Times Square', 'lat': 40.7589, 'lon': -73.9851},
        {'location_id': 13, 'borough': 'Manhattan', 'zone': 'Central Park', 'lat': 40.7812, 'lon': -73.9665},
        {'location_id': 48, 'borough': 'Manhattan', 'zone': 'Penn Station', 'lat': 40.7505, 'lon': -73.9934},
        {'location_id': 79, 'borough': 'Manhattan', 'zone': 'East Village', 'lat': 40.7264, 'lon': -73.9818},
        {'location_id': 87, 'borough': 'Manhattan', 'zone': 'Upper East Side', 'lat': 40.7736, 'lon': -73.9566},
        {'location_id': 100, 'borough': 'Manhattan', 'zone': 'Upper West Side', 'lat': 40.7870, 'lon': -73.9754},
        {'location_id': 132, 'borough': 'Queens', 'zone': 'JFK Airport', 'lat': 40.6413, 'lon': -73.7781},
        {'location_id': 138, 'borough': 'Queens', 'zone': 'LaGuardia Airport', 'lat': 40.7769, 'lon': -73.8740},
        {'location_id': 161, 'borough': 'Manhattan', 'zone': 'Midtown East', 'lat': 40.7549, 'lon': -73.9707},
        {'location_id': 162, 'borough': 'Manhattan', 'zone': 'Midtown West', 'lat': 40.7590, 'lon': -73.9845},
        {'location_id': 186, 'borough': 'Manhattan', 'zone': 'Greenwich Village', 'lat': 40.7336, 'lon': -74.0027},
        {'location_id': 230, 'borough': 'Manhattan', 'zone': 'Lower East Side', 'lat': 40.7154, 'lon': -73.9840},
        {'location_id': 237, 'borough': 'Manhattan', 'zone': 'Union Square', 'lat': 40.7359, 'lon': -73.9911},
        {'location_id': 244, 'borough': 'Brooklyn', 'zone': 'Williamsburg', 'lat': 40.7081, 'lon': -73.9571},
        {'location_id': 263, 'borough': 'Manhattan', 'zone': 'Yorkville East', 'lat': 40.7736, 'lon': -73.9566},
    ]
    
    # Add additional attributes
    for zone in zones:
        zone['zone_type'] = 'airport' if 'airport' in zone['zone'].lower() else 'neighborhood'
        zone['is_tourist_area'] = zone['zone'] in ['Times Square', 'Central Park', 'Greenwich Village', 'Union Square']
        zone['is_business_district'] = zone['zone'] in ['Financial District', 'Midtown East', 'Midtown West']
        zone['created_at'] = datetime.now().isoformat()
    
    output_path = f"{data_dir}/nyc_taxi_zones.json"
    with open(output_path, 'w') as f:
        json.dump(zones, f, indent=2)
    
    print(f"Created location data: {len(zones)} zones at {output_path}")
    
    context['task_instance'].xcom_push(key='location_data_path', value=output_path)
    return output_path

# Task definitions
fetch_weather_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

validate_weather_task = PythonOperator(
    task_id='validate_weather_data',
    python_callable=validate_weather_data,
    dag=dag,
)

create_location_task = PythonOperator(
    task_id='create_location_data',
    python_callable=create_location_data,
    dag=dag,
)

# Spark job for processing weather data to Iceberg
spark_weather_etl_task = SparkSubmitOperator(
    task_id='spark_process_weather_to_iceberg',
    application='/opt/airflow/dags/spark_jobs/weather_to_iceberg.py',
    conn_id='spark_default',
    conf={
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hive',
        'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.iceberg.type': 'hadoop',
        'spark.sql.catalog.iceberg.warehouse': 's3a://lakehouse/warehouse',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'admin',
        'spark.hadoop.fs.s3a.secret.key': 'password',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    jars='/opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar',
    dag=dag,
)

# Spark job for processing location data to Iceberg
spark_location_etl_task = SparkSubmitOperator(
    task_id='spark_process_location_to_iceberg',
    application='/opt/airflow/dags/spark_jobs/location_to_iceberg.py',
    conn_id='spark_default',
    conf={
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hive',
        'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.iceberg.type': 'hadoop',
        'spark.sql.catalog.iceberg.warehouse': 's3a://lakehouse/warehouse',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'admin',
        'spark.hadoop.fs.s3a.secret.key': 'password',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    jars='/opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar',
    dag=dag,
)

# Define task dependencies
[fetch_weather_task, create_location_task] >> validate_weather_task
validate_weather_task >> [spark_weather_etl_task, spark_location_etl_task]
