"""
Spark job to process weather data and write to Iceberg tables
"""

import sys
import os
import json

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp, year, month, dayofmonth, hour, when, lit
    from pyspark.sql.types import *
except ImportError:
    print("PySpark not available in this environment")

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("Weather to Iceberg ETL") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://lakehouse/warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def create_weather_tables(spark):
    """Create weather tables if they don't exist"""
    
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS weather")
    
    # Create hourly weather table
    weather_table_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.weather.hourly_weather (
        timestamp timestamp,
        location string,
        latitude double,
        longitude double,
        temperature_celsius double,
        temperature_fahrenheit double,
        humidity_percent double,
        pressure_hpa double,
        wind_speed_kmh double,
        weather_condition string,
        visibility_km double,
        uv_index double,
        hour int,
        day_of_week int,
        is_weekend boolean,
        load_date timestamp,
        year int,
        month int,
        day int
    ) USING iceberg
    PARTITIONED BY (year, month, day)
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    """
    
    try:
        spark.sql(weather_table_sql)
        print("Created/verified weather table")
    except Exception as e:
        print(f"Error creating weather table: {e}")

def process_weather_data(spark, input_path):
    """Process weather JSON data and write to Iceberg"""
    
    print(f"Reading weather data from: {input_path}")
    
    # Read JSON data
    with open(input_path, 'r') as f:
        weather_data = json.load(f)
    
    # Convert to Spark DataFrame
    df = spark.createDataFrame(weather_data)
    
    print(f"Raw weather data count: {df.count()}")
    df.printSchema()
    
    # Data transformations
    processed_df = df \
        .withColumn("timestamp", col("timestamp").cast("timestamp")) \
        .withColumn("load_date", current_timestamp()) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp"))) \
        .filter(col("timestamp").isNotNull())
    
    print(f"Processed weather data count: {processed_df.count()}")
    
    # Write to Iceberg table
    processed_df.writeTo("iceberg.weather.hourly_weather") \
        .option("merge-schema", "true") \
        .append()
    
    print("Successfully wrote weather data to Iceberg table")
    
    # Show some statistics
    spark.sql("""
        SELECT 
            date(timestamp) as weather_date,
            avg(temperature_celsius) as avg_temp_c,
            avg(humidity_percent) as avg_humidity,
            avg(wind_speed_kmh) as avg_wind_speed,
            collect_set(weather_condition) as conditions
        FROM iceberg.weather.hourly_weather 
        WHERE load_date >= current_date()
        GROUP BY date(timestamp)
        ORDER BY weather_date DESC
    """).show()

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: weather_to_iceberg.py <input_json_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create tables
        create_weather_tables(spark)
        
        # Process data
        process_weather_data(spark, input_path)
        
        print("Weather ETL job completed successfully!")
        
    except Exception as e:
        print(f"Error in weather ETL job: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
