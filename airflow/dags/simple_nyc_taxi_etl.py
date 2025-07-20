"""
Simple NYC Taxi Data ETL DAG using BashOperator
This DAG processes NYC taxi data using remote Spark submit to the Spark cluster
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
    import json
except ImportError:
    print("pandas not available")

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
    'simple_nyc_taxi_etl',
    default_args=default_args,
    description='Simple ETL for NYC Taxi data using Bash operators',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['nyc-taxi', 'bash', 'etl'],
)

def generate_sample_data(**context):
    """Generate sample NYC taxi data for demo purposes"""
    import random
    from datetime import datetime, timedelta
    
    # Create data directory if it doesn't exist
    data_dir = "/opt/airflow/data/raw"
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate sample data
    n_records = 1000
    current_time = datetime.now()
    
    data = []
    for i in range(n_records):
        pickup_time = current_time - timedelta(hours=random.randint(1, 24))
        dropoff_time = pickup_time + timedelta(minutes=random.randint(5, 60))
        
        record = {
            'vendor_id': random.choice([1, 2]),
            'pickup_datetime': pickup_time.isoformat(),
            'dropoff_datetime': dropoff_time.isoformat(),
            'passenger_count': random.choice([1, 2, 3, 4]),
            'trip_distance': round(random.uniform(0.5, 20.0), 2),
            'pickup_location_id': random.randint(1, 265),
            'dropoff_location_id': random.randint(1, 265),
            'fare_amount': round(random.uniform(5.0, 50.0), 2),
            'tip_amount': round(random.uniform(0.0, 10.0), 2),
            'total_amount': 0.0  # Will be calculated
        }
        
        # Calculate total amount
        record['total_amount'] = round(
            record['fare_amount'] + record['tip_amount'] + 
            random.uniform(0.5, 3.0), 2  # taxes and fees
        )
        
        data.append(record)
    
    # Save as JSON
    output_path = f"{data_dir}/taxi_data_{current_time.strftime('%Y%m%d_%H')}.json"
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Generated sample data: {len(data)} records at {output_path}")
    
    # Store file path in XCom
    context['task_instance'].xcom_push(key='data_file_path', value=output_path)
    return output_path

def validate_data(**context):
    """Validate the generated data"""
    data_path = context['task_instance'].xcom_pull(task_ids='generate_sample_data', key='data_file_path')
    
    with open(data_path, 'r') as f:
        data = json.load(f)
    
    # Basic validation checks
    assert len(data) > 0, "Dataset is empty"
    assert all('pickup_datetime' in record for record in data), "Missing pickup datetime"
    assert all('fare_amount' in record for record in data), "Missing fare amount"
    
    print(f"Data validation passed. {len(data)} records ready for processing.")
    return True

# Task definitions
generate_data_task = PythonOperator(
    task_id='generate_sample_data',
    python_callable=generate_sample_data,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

# Simple processing task using bash
process_data_task = BashOperator(
    task_id='process_data_simple',
    bash_command="""
    echo "Processing taxi data..."
    DATA_FILE="/opt/airflow/data/raw/taxi_data_$(date +%Y%m%d_%H).json"
    OUTPUT_DIR="/opt/airflow/data/processed"
    mkdir -p $OUTPUT_DIR
    
    # Simple data processing using jq
    if [ -f "$DATA_FILE" ]; then
        echo "Processing file: $DATA_FILE"
        
        # Extract summary statistics
        jq '[.[] | .fare_amount] | add / length' $DATA_FILE > $OUTPUT_DIR/avg_fare.txt
        jq '[.[] | .trip_distance] | add / length' $DATA_FILE > $OUTPUT_DIR/avg_distance.txt
        jq 'length' $DATA_FILE > $OUTPUT_DIR/total_trips.txt
        
        echo "Summary statistics generated:"
        echo "Average fare: $(cat $OUTPUT_DIR/avg_fare.txt)"
        echo "Average distance: $(cat $OUTPUT_DIR/avg_distance.txt)"
        echo "Total trips: $(cat $OUTPUT_DIR/total_trips.txt)"
    else
        echo "Data file not found: $DATA_FILE"
        exit 1
    fi
    """,
    dag=dag,
)

# Test Spark connectivity
test_spark_task = BashOperator(
    task_id='test_spark_connectivity',
    bash_command="""
    echo "Testing Spark cluster connectivity..."
    curl -f http://spark-master:8080 && echo "Spark Master is accessible" || echo "Spark Master not accessible"
    echo "Spark cluster test completed"
    """,
    dag=dag,
)

# Define task dependencies
generate_data_task >> validate_data_task >> [process_data_task, test_spark_task]
