"""
Real-time CDC Processor using Spark Structured Streaming
Processes CDC events from Kafka and updates Iceberg tables in real-time
"""

import sys
import os

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
except ImportError:
    print("PySpark not available in this environment")

def create_spark_session():
    """Create Spark session with streaming and Iceberg configuration"""
    return SparkSession.builder \
        .appName("Real-time CDC Processor") \
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
        .config("spark.sql.streaming.checkpointLocation", "s3a://lakehouse/checkpoints/cdc") \
        .getOrCreate()

def create_realtime_tables(spark):
    """Create real-time analytics tables"""
    
    # Create real-time database
    spark.sql("CREATE DATABASE IF NOT EXISTS realtime")
    
    # Real-time trip aggregations table
    realtime_trip_aggs_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.realtime.trip_aggregations (
        window_start timestamp,
        window_end timestamp,
        pickup_zone_id int,
        total_trips bigint,
        total_revenue double,
        avg_trip_distance double,
        avg_fare_amount double,
        unique_vendors bigint,
        processed_at timestamp
    ) USING iceberg
    PARTITIONED BY (days(window_start))
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    """
    
    # Real-time zone activity table
    realtime_zone_activity_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.realtime.zone_activity (
        zone_id int,
        activity_timestamp timestamp,
        pickup_count bigint,
        dropoff_count bigint,
        revenue_last_hour double,
        avg_fare_last_hour double,
        top_destination_zone int,
        activity_score double,
        processed_at timestamp
    ) USING iceberg
    PARTITIONED BY (hours(activity_timestamp))
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    """
    
    try:
        spark.sql(realtime_trip_aggs_sql)
        spark.sql(realtime_zone_activity_sql)
        print("Created/verified real-time tables")
    except Exception as e:
        print(f"Error creating real-time tables: {e}")

def process_trip_cdc_stream(spark):
    """Process CDC stream for trip data"""
    
    # Read from Kafka CDC topic
    cdc_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "lakehouse.trips") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse CDC JSON data
    cdc_parsed = cdc_stream.select(
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), 
                 StructType([
                     StructField("op", StringType(), True),
                     StructField("ts_ms", LongType(), True),
                     StructField("before", StringType(), True),
                     StructField("after", StringType(), True),
                     StructField("source", StructType([
                         StructField("db", StringType(), True),
                         StructField("table", StringType(), True),
                         StructField("ts_ms", LongType(), True)
                     ]), True)
                 ])).alias("data")
    ).select(
        col("kafka_timestamp"),
        col("data.op").alias("operation"),
        col("data.ts_ms").alias("source_ts_ms"),
        col("data.after").alias("after_data"),
        col("data.before").alias("before_data"),
        col("data.source.db").alias("source_db"),
        col("data.source.table").alias("source_table")
    )
    
    # Filter for INSERT and UPDATE operations
    active_trips = cdc_parsed.filter(
        col("operation").isin("c", "u")  # create, update
    )
    
    # Parse the after_data JSON for trip details
    trip_details = active_trips.select(
        col("kafka_timestamp"),
        col("operation"),
        col("source_ts_ms"),
        from_json(col("after_data"), 
                 StructType([
                     StructField("id", IntegerType(), True),
                     StructField("vendor_id", IntegerType(), True),
                     StructField("pickup_datetime", StringType(), True),
                     StructField("dropoff_datetime", StringType(), True),
                     StructField("pickup_location_id", IntegerType(), True),
                     StructField("dropoff_location_id", IntegerType(), True),
                     StructField("trip_distance", DoubleType(), True),
                     StructField("fare_amount", DoubleType(), True),
                     StructField("total_amount", DoubleType(), True)
                 ])).alias("trip")
    ).select(
        col("kafka_timestamp"),
        col("operation"),
        col("source_ts_ms"),
        col("trip.*")
    ).filter(
        col("pickup_location_id").isNotNull() &
        col("fare_amount").isNotNull() &
        col("fare_amount") > 0
    )
    
    # Create windowed aggregations (5-minute windows)
    windowed_aggs = trip_details \
        .withWatermark("kafka_timestamp", "10 minutes") \
        .groupBy(
            window(col("kafka_timestamp"), "5 minutes"),
            col("pickup_location_id")
        ) \
        .agg(
            count("*").alias("total_trips"),
            sum("total_amount").alias("total_revenue"),
            avg("trip_distance").alias("avg_trip_distance"),
            avg("fare_amount").alias("avg_fare_amount"),
            countDistinct("vendor_id").alias("unique_vendors")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("pickup_location_id").alias("pickup_zone_id"),
            col("total_trips"),
            col("total_revenue"),
            col("avg_trip_distance"),
            col("avg_fare_amount"),
            col("unique_vendors"),
            current_timestamp().alias("processed_at")
        )
    
    # Write to Iceberg table
    query = windowed_aggs.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("table", "iceberg.realtime.trip_aggregations") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/cdc/trip_aggs") \
        .option("fanout-enabled", "true") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query

def process_zone_activity_stream(spark):
    """Process zone activity in real-time"""
    
    # Read from the trip aggregations table as a stream source
    trip_stream = spark \
        .readStream \
        .format("iceberg") \
        .table("iceberg.realtime.trip_aggregations")
    
    # Calculate zone activity scores
    zone_activity = trip_stream \
        .withColumn("activity_timestamp", col("window_end")) \
        .withColumn("revenue_last_hour", col("total_revenue")) \
        .withColumn("avg_fare_last_hour", col("avg_fare_amount")) \
        .withColumn("pickup_count", col("total_trips")) \
        .withColumn("dropoff_count", lit(0))  # Would need separate CDC processing for dropoffs
        .withColumn("top_destination_zone", lit(null).cast("int"))  # Would need more complex logic
        .withColumn("activity_score", 
                   col("total_trips") * 0.4 + 
                   (col("total_revenue") / 100) * 0.4 + 
                   col("unique_vendors") * 0.2) \
        .select(
            col("pickup_zone_id").alias("zone_id"),
            col("activity_timestamp"),
            col("pickup_count"),
            col("dropoff_count"),
            col("revenue_last_hour"),
            col("avg_fare_last_hour"),
            col("top_destination_zone"),
            col("activity_score"),
            current_timestamp().alias("processed_at")
        )
    
    # Write to zone activity table
    query = zone_activity.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("table", "iceberg.realtime.zone_activity") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/cdc/zone_activity") \
        .trigger(processingTime="60 seconds") \
        .start()
    
    return query

def monitor_streaming_queries(queries):
    """Monitor streaming queries and handle errors"""
    try:
        # Wait for queries to finish (they run indefinitely)
        for query in queries:
            query.awaitTermination(timeout=300)  # 5 minutes timeout for demo
            
    except Exception as e:
        print(f"Streaming query error: {e}")
        for query in queries:
            if query.isActive:
                query.stop()
        raise

def main():
    """Main function"""
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("Starting real-time CDC processor...")
        
        # Create real-time tables
        create_realtime_tables(spark)
        
        # Start streaming queries
        trip_agg_query = process_trip_cdc_stream(spark)
        zone_activity_query = process_zone_activity_stream(spark)
        
        queries = [trip_agg_query, zone_activity_query]
        
        print("Real-time streaming queries started successfully!")
        print("Processing CDC events...")
        
        # Monitor queries
        monitor_streaming_queries(queries)
        
    except Exception as e:
        print(f"Error in real-time CDC processor: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
