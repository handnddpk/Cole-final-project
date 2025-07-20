"""
Spark job to process location/zone data and write to Iceberg tables
"""

import sys
import os
import json

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp, lit
    from pyspark.sql.types import *
except ImportError:
    print("PySpark not available in this environment")

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("Location to Iceberg ETL") \
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

def create_location_tables(spark):
    """Create location reference tables if they don't exist"""
    
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS reference")
    
    # Create taxi zones table
    zones_table_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.reference.taxi_zones (
        location_id int,
        borough string,
        zone string,
        latitude double,
        longitude double,
        zone_type string,
        is_tourist_area boolean,
        is_business_district boolean,
        created_at timestamp,
        load_date timestamp
    ) USING iceberg
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    """
    
    try:
        spark.sql(zones_table_sql)
        print("Created/verified taxi zones table")
    except Exception as e:
        print(f"Error creating zones table: {e}")

def process_location_data(spark, input_path):
    """Process location JSON data and write to Iceberg"""
    
    print(f"Reading location data from: {input_path}")
    
    # Read JSON data
    with open(input_path, 'r') as f:
        location_data = json.load(f)
    
    # Convert to Spark DataFrame
    df = spark.createDataFrame(location_data)
    
    print(f"Raw location data count: {df.count()}")
    df.printSchema()
    
    # Data transformations
    processed_df = df \
        .withColumn("created_at", col("created_at").cast("timestamp")) \
        .withColumn("load_date", current_timestamp()) \
        .withColumnRenamed("lat", "latitude") \
        .withColumnRenamed("lon", "longitude")
    
    print(f"Processed location data count: {processed_df.count()}")
    
    # Truncate and reload reference data (since it's reference data, not incremental)
    spark.sql("DELETE FROM iceberg.reference.taxi_zones")
    
    # Write to Iceberg table
    processed_df.writeTo("iceberg.reference.taxi_zones") \
        .option("merge-schema", "true") \
        .append()
    
    print("Successfully wrote location data to Iceberg table")
    
    # Show some statistics
    spark.sql("""
        SELECT 
            borough,
            count(*) as zone_count,
            sum(case when is_tourist_area then 1 else 0 end) as tourist_zones,
            sum(case when is_business_district then 1 else 0 end) as business_zones
        FROM iceberg.reference.taxi_zones 
        GROUP BY borough
        ORDER BY zone_count DESC
    """).show()

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: location_to_iceberg.py <input_json_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create tables
        create_location_tables(spark)
        
        # Process data
        process_location_data(spark, input_path)
        
        print("Location ETL job completed successfully!")
        
    except Exception as e:
        print(f"Error in location ETL job: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
