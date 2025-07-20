"""
Simple test DAG to verify Airflow setup
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_setup',
    default_args=default_args,
    description='Test Airflow and Spark setup',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test'],
)

def test_python():
    """Test Python functionality"""
    print("Python test successful!")
    return "success"

# Test tasks
test_python_task = PythonOperator(
    task_id='test_python',
    python_callable=test_python,
    dag=dag,
)

test_bash_task = BashOperator(
    task_id='test_bash',
    bash_command='echo "Bash test successful!"',
    dag=dag,
)

test_spark_task = BashOperator(
    task_id='test_spark_connection',
    bash_command='curl -f http://spark-master:8080 || echo "Spark master not reachable"',
    dag=dag,
)

test_minio_task = BashOperator(
    task_id='test_minio_connection',
    bash_command='curl -f http://minio:9000/minio/health/live || echo "MinIO not reachable"',
    dag=dag,
)

# Task dependencies
test_python_task >> test_bash_task >> [test_spark_task, test_minio_task]
