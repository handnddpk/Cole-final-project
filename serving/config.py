import os
from typing import Optional

try:
    from pydantic import BaseSettings
except ImportError:
    try:
        from pydantic_settings import BaseSettings
    except ImportError:
        # Fallback to a simple class if pydantic/pydantic-settings not available
        class BaseSettings:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)
            
            class Config:
                env_file = ".env"

class Settings(BaseSettings):
    # API Configuration
    api_title: str = "Lakehouse Analytics API"
    api_description: str = "REST API for real-time and historical analytics"
    api_version: str = "1.0.0"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    # Database Configuration
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "taxi_db"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    
    # Airflow Database Configuration
    airflow_postgres_host: str = "localhost"
    airflow_postgres_port: int = 5433
    airflow_postgres_db: str = "airflow"
    airflow_postgres_user: str = "airflow"
    airflow_postgres_password: str = "airflow"
    
    # MinIO Configuration
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "password"
    minio_bucket: str = "lakehouse"
    minio_secure: bool = False
    
    # Redis Configuration
    redis_url: str = "redis://localhost:6379"
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = "localhost:9092"
    
    # Cache Configuration
    cache_ttl: int = 300  # 5 minutes
    
    # Authentication (simple token for demo)
    api_key: str = "demo-api-key-2024"
    
    @property
    def database_url(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    @property
    def airflow_database_url(self) -> str:
        return f"postgresql://{self.airflow_postgres_user}:{self.airflow_postgres_password}@{self.airflow_postgres_host}:{self.airflow_postgres_port}/{self.airflow_postgres_db}"
    
    class Config:
        env_file = ".env"

settings = Settings()
