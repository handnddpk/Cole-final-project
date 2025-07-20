"""
NYC Taxi Data ETL DAG - Incremental Load to Iceberg
This DAG extracts NYC taxi data from the public dataset and loads it to Iceberg tables using Spark
"""

from datetime import datetime, timedelta
import os

# Handle Airflow imports gracefully for development
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    print("Airflow not available in this environment")

try:
    import pandas as pd
    import requests
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
    'nyc_taxi_iceberg_etl',
    default_args=default_args,
    description='Incremental ETL for NYC Taxi data to Iceberg',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['nyc-taxi', 'iceberg', 'spark', 'etl'],
)

def get_latest_processed_date(**context):
    """Get the latest date processed from the control table"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Create control table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.etl_control (
        table_name VARCHAR(255) PRIMARY KEY,
        last_processed_date TIMESTAMP,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    pg_hook.run(create_table_sql)
    
    # Get last processed date
    result = pg_hook.get_first("""
        SELECT last_processed_date 
        FROM public.etl_control 
        WHERE table_name = 'nyc_taxi_trips'
    """)
    
    if result and result[0]:
        last_date = result[0]
    else:
        # If no previous run, start from 30 days ago
        last_date = datetime.now() - timedelta(days=30)
        # Insert initial record
        pg_hook.run("""
            INSERT INTO public.etl_control (table_name, last_processed_date)
            VALUES ('nyc_taxi_trips', %s)
            ON CONFLICT (table_name) 
            DO UPDATE SET last_processed_date = EXCLUDED.last_processed_date
        """, parameters=(last_date,))
    
    # Store in XCom for next task
    context['task_instance'].xcom_push(key='last_processed_date', value=last_date.isoformat())
    return last_date.isoformat()

def download_nyc_taxi_data(**context):
    """Download incremental NYC taxi data"""
    import tempfile
    
    # Get the last processed date
    last_date_str = context['task_instance'].xcom_pull(task_ids='get_latest_date', key='last_processed_date')
    last_date = datetime.fromisoformat(last_date_str)
    
    # For demo purposes, we'll use the current month's data
    # In production, you'd implement proper incremental logic
    current_date = datetime.now()
    year = current_date.year
    month = current_date.month
    
    # NYC Taxi data URL (Yellow taxi trips)
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    
    # Create data directory if it doesn't exist
    data_dir = "/opt/airflow/data/raw"
    os.makedirs(data_dir, exist_ok=True)
    
    # Download file
    output_path = f"{data_dir}/yellow_tripdata_{year}-{month:02d}.parquet"
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Downloaded taxi data to {output_path}")
        
        # Store file path in XCom
        context['task_instance'].xcom_push(key='raw_data_path', value=output_path)
        
    except Exception as e:
        print(f"Error downloading data: {e}")
        # For demo, create sample data if download fails
        create_sample_data(output_path, last_date)
        context['task_instance'].xcom_push(key='raw_data_path', value=output_path)

def create_sample_data(output_path, last_date):
    """Create sample NYC taxi data for demo purposes"""
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    # Generate sample data
    n_records = 10000
    start_date = last_date
    end_date = datetime.now()
    
    data = {
        'VendorID': np.random.choice([1, 2], n_records),
        'tpep_pickup_datetime': pd.date_range(start_date, end_date, periods=n_records),
        'tpep_dropoff_datetime': pd.date_range(start_date + timedelta(minutes=5), 
                                             end_date + timedelta(minutes=30), periods=n_records),
        'passenger_count': np.random.choice([1, 2, 3, 4, 5], n_records, p=[0.6, 0.2, 0.1, 0.07, 0.03]),
        'trip_distance': np.random.exponential(2.0, n_records),
        'RatecodeID': np.random.choice([1, 2, 3, 4, 5], n_records, p=[0.9, 0.03, 0.03, 0.02, 0.02]),
        'store_and_fwd_flag': np.random.choice(['N', 'Y'], n_records, p=[0.95, 0.05]),
        'PULocationID': np.random.randint(1, 266, n_records),
        'DOLocationID': np.random.randint(1, 266, n_records),
        'payment_type': np.random.choice([1, 2, 3, 4], n_records, p=[0.7, 0.25, 0.03, 0.02]),
        'fare_amount': np.random.exponential(10.0, n_records),
        'extra': np.random.choice([0, 0.5, 1.0], n_records, p=[0.7, 0.2, 0.1]),
        'mta_tax': np.full(n_records, 0.5),
        'tip_amount': np.random.exponential(2.0, n_records),
        'tolls_amount': np.random.exponential(1.0, n_records) * np.random.choice([0, 1], n_records, p=[0.9, 0.1]),
        'improvement_surcharge': np.full(n_records, 0.3),
        'total_amount': None,
        'congestion_surcharge': np.random.choice([0, 2.5], n_records, p=[0.5, 0.5]),
        'airport_fee': np.random.choice([0, 1.25], n_records, p=[0.95, 0.05])
    }
    
    df = pd.DataFrame(data)
    
    # Calculate total_amount
    df['total_amount'] = (df['fare_amount'] + df['extra'] + df['mta_tax'] + 
                         df['tip_amount'] + df['tolls_amount'] + 
                         df['improvement_surcharge'] + df['congestion_surcharge'] + 
                         df['airport_fee'])
    
    # Save as parquet
    df.to_parquet(output_path, index=False)
    print(f"Created sample data at {output_path}")

def validate_data(**context):
    """Validate the downloaded data"""
    data_path = context['task_instance'].xcom_pull(task_ids='download_data', key='raw_data_path')
    
    df = pd.read_parquet(data_path)
    
    # Basic validation checks
    assert len(df) > 0, "Dataset is empty"
    assert 'tpep_pickup_datetime' in df.columns, "Missing pickup datetime column"
    assert 'tpep_dropoff_datetime' in df.columns, "Missing dropoff datetime column"
    
    # Data quality checks
    null_percentage = df.isnull().sum() / len(df) * 100
    print("Null percentage by column:")
    print(null_percentage)
    
    print(f"Dataset validation passed. {len(df)} records ready for processing.")
    
    # Store validated path
    context['task_instance'].xcom_push(key='validated_data_path', value=data_path)

def update_control_table(**context):
    """Update the control table with the latest processed date"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    current_time = datetime.now()
    
    pg_hook.run("""
        UPDATE public.etl_control 
        SET last_processed_date = %s, last_updated = %s
        WHERE table_name = 'nyc_taxi_trips'
    """, parameters=(current_time, current_time))
    
    print(f"Updated control table with date: {current_time}")

# Task definitions
get_latest_date_task = PythonOperator(
    task_id='get_latest_date',
    python_callable=get_latest_processed_date,
    dag=dag,
)

download_data_task = PythonOperator(
    task_id='download_data',
    python_callable=download_nyc_taxi_data,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

# Spark job for processing data to Iceberg
spark_etl_task = BashOperator(
    task_id='spark_process_to_iceberg',
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
        /opt/airflow/dags/spark_jobs/nyc_taxi_to_iceberg.py {{ task_instance.xcom_pull(task_ids='validate_data', key='validated_data_path') }}
    ''',
    dag=dag,
)

update_control_task = PythonOperator(
    task_id='update_control_table',
    python_callable=update_control_table,
    dag=dag,
)

# Define task dependencies
get_latest_date_task >> download_data_task >> validate_data_task >> spark_etl_task >> update_control_task
