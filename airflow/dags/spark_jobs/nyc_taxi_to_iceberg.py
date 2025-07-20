"""
Spark job to process NYC taxi data and write to Iceberg tables
"""

import sys
import os

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp, year, month, dayofmonth
    from pyspark.sql.types import *
except ImportError:
    print("PySpark not available in this environment")
    # For linting purposes when PySpark is not installed

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("NYC Taxi to Iceberg ETL") \
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

def create_iceberg_tables(spark):
    """Create Iceberg tables if they don't exist"""
    
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS nyc_taxi")
    
    # Create trips table
    trips_table_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.nyc_taxi.trips (
        vendor_id int,
        pickup_datetime timestamp,
        dropoff_datetime timestamp,
        passenger_count int,
        trip_distance double,
        rate_code_id int,
        store_and_fwd_flag string,
        pickup_location_id int,
        dropoff_location_id int,
        payment_type int,
        fare_amount double,
        extra double,
        mta_tax double,
        tip_amount double,
        tolls_amount double,
        improvement_surcharge double,
        total_amount double,
        congestion_surcharge double,
        airport_fee double,
        load_date timestamp,
        year int,
        month int,
        day int
    ) USING iceberg
    PARTITIONED BY (year, month)
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    """
    
    try:
        spark.sql(trips_table_sql)
        print("Created/verified trips table")
    except Exception as e:
        print(f"Error creating trips table: {e}")

def process_taxi_data(spark, input_path):
    """Process raw taxi data and write to Iceberg"""
    
    # Read raw parquet data
    print(f"Reading data from: {input_path}")
    df = spark.read.parquet(input_path)
    
    print(f"Raw data count: {df.count()}")
    df.printSchema()
    
    # Data cleaning and transformation
    cleaned_df = df \
        .filter(col("tpep_pickup_datetime").isNotNull()) \
        .filter(col("tpep_dropoff_datetime").isNotNull()) \
        .filter(col("trip_distance") > 0) \
        .filter(col("fare_amount") > 0) \
        .filter(col("total_amount") > 0) \
        .filter(col("passenger_count").between(1, 6))
    
    # Rename columns to match our schema
    processed_df = cleaned_df \
        .withColumnRenamed("VendorID", "vendor_id") \
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
        .withColumnRenamed("RatecodeID", "rate_code_id") \
        .withColumnRenamed("PULocationID", "pickup_location_id") \
        .withColumnRenamed("DOLocationID", "dropoff_location_id") \
        .withColumn("load_date", current_timestamp()) \
        .withColumn("year", year(col("pickup_datetime"))) \
        .withColumn("month", month(col("pickup_datetime"))) \
        .withColumn("day", dayofmonth(col("pickup_datetime")))
    
    # Select only the columns we need
    final_df = processed_df.select(
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime", 
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "load_date",
        "year",
        "month",
        "day"
    )
    
    print(f"Processed data count: {final_df.count()}")
    
    # Write to Iceberg table (merge mode for incremental updates)
    final_df.writeTo("iceberg.nyc_taxi.trips") \
        .option("merge-schema", "true") \
        .append()
    
    print("Successfully wrote data to Iceberg table")
    
    # Show some statistics
    spark.sql("""
        SELECT 
            year,
            month,
            count(*) as trip_count,
            avg(trip_distance) as avg_distance,
            avg(total_amount) as avg_amount
        FROM iceberg.nyc_taxi.trips 
        WHERE load_date >= current_date()
        GROUP BY year, month
        ORDER BY year DESC, month DESC
    """).show()

def create_aggregated_tables(spark):
    """Create aggregated tables for analytics"""
    
    # Daily summary table
    daily_summary_sql = """
    CREATE OR REPLACE TABLE iceberg.nyc_taxi.daily_summary
    USING iceberg
    PARTITIONED BY (year, month)
    AS
    SELECT 
        date(pickup_datetime) as trip_date,
        year(pickup_datetime) as year,
        month(pickup_datetime) as month,
        count(*) as total_trips,
        sum(passenger_count) as total_passengers,
        avg(trip_distance) as avg_trip_distance,
        avg(fare_amount) as avg_fare_amount,
        avg(tip_amount) as avg_tip_amount,
        avg(total_amount) as avg_total_amount,
        max(load_date) as last_updated
    FROM iceberg.nyc_taxi.trips
    WHERE date(pickup_datetime) = current_date()
    GROUP BY date(pickup_datetime), year(pickup_datetime), month(pickup_datetime)
    """
    
    try:
        spark.sql(daily_summary_sql)
        print("Created daily summary table")
    except Exception as e:
        print(f"Error creating daily summary: {e}")

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: spark_job.py <input_parquet_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create tables
        create_iceberg_tables(spark)
        
        # Process data
        process_taxi_data(spark, input_path)
        
        # Create aggregated tables
        create_aggregated_tables(spark)
        
        print("ETL job completed successfully!")
        
    except Exception as e:
        print(f"Error in ETL job: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
