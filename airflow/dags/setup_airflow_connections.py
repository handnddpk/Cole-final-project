"""
Setup Airflow connections programmatically
Run this inside Airflow container: docker exec airflow-webserver python /opt/airflow/dags/setup_airflow_connections.py
"""

from airflow.models import Connection
from airflow.settings import Session
import json

def create_or_update_connection(conn_id, conn_type, host, schema=None, login=None, password=None, port=None, extra=None):
    """Create or update an Airflow connection"""
    session = Session()
    
    # Check if connection exists
    conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if conn:
        print(f"Updating existing connection: {conn_id}")
        conn.conn_type = conn_type
        conn.host = host
        conn.schema = schema
        conn.login = login
        conn.password = password
        conn.port = port
        if extra:
            conn.extra = json.dumps(extra) if isinstance(extra, dict) else extra
    else:
        print(f"Creating new connection: {conn_id}")
        conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            schema=schema,
            login=login,
            password=password,
            port=port,
            extra=json.dumps(extra) if extra and isinstance(extra, dict) else extra
        )
        session.add(conn)
    
    session.commit()
    session.close()
    print(f"✓ Connection '{conn_id}' configured successfully")

def setup_connections():
    """Setup all required connections for the NYC Weather ETL pipeline"""
    
    print("=" * 60)
    print("Setting up Airflow Connections")
    print("=" * 60)
    
    # Spark connection - configured to use remote cluster, not local PySpark
    create_or_update_connection(
        conn_id='spark_default',
        conn_type='spark',
        host='spark://spark-master',
        port=7077,
        extra={
            'queue': 'root.default',
            'deploy-mode': 'client',
            'spark-home': '/opt/bitnami/spark',
            'spark-binary': 'spark-submit'
        }
    )
    
    # PostgreSQL connection
    create_or_update_connection(
        conn_id='postgres_default',
        conn_type='postgres',
        host='postgres',
        schema='taxi_db',
        login='postgres',
        password='postgres',
        port=5432
    )
    
    # HTTP connection for APIs (optional, for future use)
    create_or_update_connection(
        conn_id='http_default',
        conn_type='http',
        host='http://localhost',
        port=8080
    )
    
    print("\n" + "=" * 60)
    print("All connections configured successfully!")
    print("=" * 60)
    
    # List all connections
    print("\nConfigured connections:")
    session = Session()
    connections = session.query(Connection).all()
    for conn in connections:
        print(f"  - {conn.conn_id} ({conn.conn_type}): {conn.host}")
    session.close()

if __name__ == "__main__":
    try:
        setup_connections()
    except Exception as e:
        print(f"Error setting up connections: {e}")
        import traceback
        traceback.print_exc()
