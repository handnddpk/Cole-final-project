"""
Comprehensive Data Transformation Pipeline
This DAG combines taxi trips, weather data, and CDC data from Kafka to create analytics tables
"""

from datetime import datetime, timedelta
import os

# Handle Airflow imports gracefully for development
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    from airflow.sensors.external_task import ExternalTaskSensor
except ImportError:
    print("Airflow not available in this environment")

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
    'comprehensive_analytics_pipeline',
    default_args=default_args,
    description='Comprehensive analytics pipeline combining taxi, weather, and CDC data',
    schedule_interval=timedelta(hours=2),  # Run every 2 hours
    catchup=False,
    max_active_runs=1,
    tags=['analytics', 'iceberg', 'spark', 'transformation'],
)

def check_data_availability(**context):
    """Check if all required data sources are available"""
    print("Checking data availability...")
    
    # This would normally check for:
    # 1. Latest taxi trip data in Iceberg
    # 2. Latest weather data in Iceberg  
    # 3. Latest CDC data from Kafka in Iceberg
    
    # For demo, we'll assume data is available
    print("All data sources are available for processing")
    return True

# Sensor to wait for taxi data ETL to complete
wait_for_taxi_data = ExternalTaskSensor(
    task_id='wait_for_taxi_data',
    external_dag_id='nyc_taxi_iceberg_etl',
    external_task_id='spark_process_to_iceberg',
    timeout=300,
    allowed_states=['success'],
    failed_states=['failed', 'upstream_failed'],
    dag=dag,
)

# Sensor to wait for weather data ETL to complete
wait_for_weather_data = ExternalTaskSensor(
    task_id='wait_for_weather_data',
    external_dag_id='nyc_weather_etl',
    external_task_id='spark_process_weather_to_iceberg',
    timeout=300,
    allowed_states=['success'],
    failed_states=['failed', 'upstream_failed'],
    dag=dag,
)

# Check data availability
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag,
)

# Comprehensive analytics Spark job
comprehensive_analytics_task = BashOperator(
    task_id='comprehensive_analytics_transformation',
    bash_command='''
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
        --conf spark.sql.catalog.spark_catalog.type=hive \
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg.type=hadoop \
        --conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse/warehouse \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=admin \
        --conf spark.hadoop.fs.s3a.secret.key=password \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --jars /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
        /opt/airflow/dags/spark_jobs/comprehensive_analytics.py
    ''',
    dag=dag,
)

# Real-time CDC processing Spark job
realtime_cdc_task = BashOperator(
    task_id='realtime_cdc_processing',
    bash_command='''
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
        --conf spark.sql.catalog.spark_catalog.type=hive \
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg.type=hadoop \
        --conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse/warehouse \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=admin \
        --conf spark.hadoop.fs.s3a.secret.key=password \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.streaming.checkpointLocation=s3a://lakehouse/checkpoints/cdc \
        --jars /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
        /opt/airflow/dags/spark_jobs/realtime_cdc_processor.py
    ''',
    dag=dag,
)

# ML feature engineering task
ml_feature_engineering_task = BashOperator(
    task_id='ml_feature_engineering',
    bash_command='''
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
        --conf spark.sql.catalog.spark_catalog.type=hive \
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg.type=hadoop \
        --conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse/warehouse \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=admin \
        --conf spark.hadoop.fs.s3a.secret.key=password \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --jars /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
        /opt/airflow/dags/spark_jobs/ml_feature_engineering.py
    ''',
    dag=dag,
)

# Define task dependencies
[wait_for_taxi_data, wait_for_weather_data] >> check_data_task
check_data_task >> [comprehensive_analytics_task, realtime_cdc_task]
comprehensive_analytics_task >> ml_feature_engineering_task
