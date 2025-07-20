"""
Comprehensive Analytics Spark Job
Combines taxi trips, weather data, and CDC data to create analytics tables
"""

import sys
import os

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.sql.window import Window
except ImportError:
    print("PySpark not available in this environment")

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("Comprehensive Analytics Pipeline") \
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

def create_analytics_tables(spark):
    """Create analytics tables if they don't exist"""
    
    # Create analytics database
    spark.sql("CREATE DATABASE IF NOT EXISTS analytics")
    
    # Trip weather correlation table
    trip_weather_correlation_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.analytics.trip_weather_correlation (
        trip_date date,
        hour int,
        weather_condition string,
        temperature_celsius double,
        humidity_percent double,
        wind_speed_kmh double,
        total_trips bigint,
        avg_trip_distance double,
        avg_fare_amount double,
        avg_tip_amount double,
        avg_trip_duration_minutes double,
        pickup_zone_diversity int,
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
    
    # Zone performance metrics table
    zone_performance_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.analytics.zone_performance_metrics (
        location_id int,
        zone_name string,
        borough string,
        zone_type string,
        is_tourist_area boolean,
        is_business_district boolean,
        trip_date date,
        hour int,
        weather_condition string,
        total_pickups bigint,
        total_dropoffs bigint,
        avg_fare_per_pickup double,
        avg_tip_percentage double,
        avg_trip_distance double,
        peak_hour_factor double,
        weather_impact_score double,
        load_date timestamp,
        year int,
        month int
    ) USING iceberg
    PARTITIONED BY (year, month, borough)
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    """
    
    # Demand prediction features table
    demand_prediction_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.analytics.demand_prediction_features (
        location_id int,
        prediction_datetime timestamp,
        hour int,
        day_of_week int,
        is_weekend boolean,
        is_holiday boolean,
        temperature_celsius double,
        weather_condition string,
        historical_demand_1h_ago bigint,
        historical_demand_24h_ago bigint,
        historical_demand_168h_ago bigint,
        rolling_avg_demand_7d double,
        rolling_avg_demand_30d double,
        zone_type string,
        is_tourist_area boolean,
        is_business_district boolean,
        nearby_zones_demand bigint,
        weather_impact_factor double,
        event_factor double,
        load_date timestamp,
        year int,
        month int
    ) USING iceberg
    PARTITIONED BY (year, month)
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    """
    
    try:
        spark.sql(trip_weather_correlation_sql)
        spark.sql(zone_performance_sql)
        spark.sql(demand_prediction_sql)
        print("Created/verified analytics tables")
    except Exception as e:
        print(f"Error creating analytics tables: {e}")

def create_trip_weather_correlation(spark):
    """Create trip weather correlation analytics"""
    
    correlation_sql = """
    WITH trip_hourly AS (
        SELECT 
            date(pickup_datetime) as trip_date,
            hour(pickup_datetime) as hour,
            pickup_location_id,
            dropoff_location_id,
            trip_distance,
            fare_amount,
            tip_amount,
            (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60 as trip_duration_minutes
        FROM iceberg.nyc_taxi.trips
        WHERE pickup_datetime >= current_date() - interval 1 day
    ),
    weather_hourly AS (
        SELECT 
            date(timestamp) as weather_date,
            hour(timestamp) as hour,
            weather_condition,
            temperature_celsius,
            humidity_percent,
            wind_speed_kmh
        FROM iceberg.weather.hourly_weather
        WHERE timestamp >= current_date() - interval 1 day
    ),
    combined_data AS (
        SELECT 
            t.trip_date,
            t.hour,
            w.weather_condition,
            w.temperature_celsius,
            w.humidity_percent,
            w.wind_speed_kmh,
            t.trip_distance,
            t.fare_amount,
            t.tip_amount,
            t.trip_duration_minutes,
            t.pickup_location_id
        FROM trip_hourly t
        LEFT JOIN weather_hourly w ON t.trip_date = w.weather_date AND t.hour = w.hour
    )
    SELECT 
        trip_date,
        hour,
        COALESCE(weather_condition, 'unknown') as weather_condition,
        AVG(temperature_celsius) as temperature_celsius,
        AVG(humidity_percent) as humidity_percent,
        AVG(wind_speed_kmh) as wind_speed_kmh,
        COUNT(*) as total_trips,
        AVG(trip_distance) as avg_trip_distance,
        AVG(fare_amount) as avg_fare_amount,
        AVG(tip_amount) as avg_tip_amount,
        AVG(trip_duration_minutes) as avg_trip_duration_minutes,
        COUNT(DISTINCT pickup_location_id) as pickup_zone_diversity,
        current_timestamp() as load_date,
        year(trip_date) as year,
        month(trip_date) as month,
        day(trip_date) as day
    FROM combined_data
    GROUP BY trip_date, hour, weather_condition
    """
    
    print("Creating trip weather correlation data...")
    correlation_df = spark.sql(correlation_sql)
    
    # Delete existing data for the same date range to avoid duplicates
    spark.sql("""
        DELETE FROM iceberg.analytics.trip_weather_correlation 
        WHERE trip_date >= current_date() - interval 1 day
    """)
    
    # Insert new data
    correlation_df.writeTo("iceberg.analytics.trip_weather_correlation").append()
    print("Trip weather correlation data created successfully")

def create_zone_performance_metrics(spark):
    """Create zone performance metrics"""
    
    zone_metrics_sql = """
    WITH trip_data AS (
        SELECT 
            pickup_location_id,
            dropoff_location_id,
            date(pickup_datetime) as trip_date,
            hour(pickup_datetime) as hour,
            fare_amount,
            tip_amount,
            trip_distance
        FROM iceberg.nyc_taxi.trips
        WHERE pickup_datetime >= current_date() - interval 1 day
    ),
    weather_data AS (
        SELECT 
            date(timestamp) as weather_date,
            hour(timestamp) as hour,
            weather_condition
        FROM iceberg.weather.hourly_weather
        WHERE timestamp >= current_date() - interval 1 day
    ),
    zone_reference AS (
        SELECT 
            location_id,
            zone,
            borough,
            zone_type,
            is_tourist_area,
            is_business_district
        FROM iceberg.reference.taxi_zones
    ),
    pickup_metrics AS (
        SELECT 
            t.pickup_location_id as location_id,
            t.trip_date,
            t.hour,
            COALESCE(w.weather_condition, 'unknown') as weather_condition,
            COUNT(*) as total_pickups,
            AVG(t.fare_amount) as avg_fare_per_pickup,
            AVG(CASE WHEN t.fare_amount > 0 THEN t.tip_amount / t.fare_amount * 100 ELSE 0 END) as avg_tip_percentage,
            AVG(t.trip_distance) as avg_trip_distance
        FROM trip_data t
        LEFT JOIN weather_data w ON t.trip_date = w.weather_date AND t.hour = w.hour
        GROUP BY t.pickup_location_id, t.trip_date, t.hour, w.weather_condition
    ),
    dropoff_metrics AS (
        SELECT 
            dropoff_location_id as location_id,
            trip_date,
            hour,
            COUNT(*) as total_dropoffs
        FROM trip_data
        GROUP BY dropoff_location_id, trip_date, hour
    ),
    hourly_pickup_stats AS (
        SELECT 
            location_id,
            trip_date,
            AVG(total_pickups) as avg_hourly_pickups,
            MAX(total_pickups) as max_hourly_pickups
        FROM pickup_metrics
        GROUP BY location_id, trip_date
    )
    SELECT 
        pm.location_id,
        zr.zone as zone_name,
        zr.borough,
        zr.zone_type,
        zr.is_tourist_area,
        zr.is_business_district,
        pm.trip_date,
        pm.hour,
        pm.weather_condition,
        pm.total_pickups,
        COALESCE(dm.total_dropoffs, 0) as total_dropoffs,
        pm.avg_fare_per_pickup,
        pm.avg_tip_percentage,
        pm.avg_trip_distance,
        CASE 
            WHEN hps.avg_hourly_pickups > 0 
            THEN pm.total_pickups / hps.avg_hourly_pickups 
            ELSE 1.0 
        END as peak_hour_factor,
        CASE 
            WHEN pm.weather_condition IN ('rain', 'snow') THEN 1.2
            WHEN pm.weather_condition = 'fog' THEN 1.1
            ELSE 1.0 
        END as weather_impact_score,
        current_timestamp() as load_date,
        year(pm.trip_date) as year,
        month(pm.trip_date) as month
    FROM pickup_metrics pm
    LEFT JOIN dropoff_metrics dm ON pm.location_id = dm.location_id 
        AND pm.trip_date = dm.trip_date AND pm.hour = dm.hour
    LEFT JOIN zone_reference zr ON pm.location_id = zr.location_id
    LEFT JOIN hourly_pickup_stats hps ON pm.location_id = hps.location_id 
        AND pm.trip_date = hps.trip_date
    WHERE zr.location_id IS NOT NULL
    """
    
    print("Creating zone performance metrics...")
    metrics_df = spark.sql(zone_metrics_sql)
    
    # Delete existing data for the same date range
    spark.sql("""
        DELETE FROM iceberg.analytics.zone_performance_metrics 
        WHERE trip_date >= current_date() - interval 1 day
    """)
    
    # Insert new data
    metrics_df.writeTo("iceberg.analytics.zone_performance_metrics").append()
    print("Zone performance metrics created successfully")

def create_demand_prediction_features(spark):
    """Create features for demand prediction ML models"""
    
    features_sql = """
    WITH current_demand AS (
        SELECT 
            pickup_location_id as location_id,
            date_trunc('hour', pickup_datetime) as hour_timestamp,
            COUNT(*) as demand_count
        FROM iceberg.nyc_taxi.trips
        WHERE pickup_datetime >= current_date() - interval 7 days
        GROUP BY pickup_location_id, date_trunc('hour', pickup_datetime)
    ),
    weather_features AS (
        SELECT 
            date_trunc('hour', timestamp) as hour_timestamp,
            temperature_celsius,
            weather_condition
        FROM iceberg.weather.hourly_weather
        WHERE timestamp >= current_date() - interval 7 days
    ),
    zone_reference AS (
        SELECT 
            location_id,
            zone_type,
            is_tourist_area,
            is_business_district
        FROM iceberg.reference.taxi_zones
    )
    SELECT 
        cd.location_id,
        cd.hour_timestamp as prediction_datetime,
        hour(cd.hour_timestamp) as hour,
        dayofweek(cd.hour_timestamp) as day_of_week,
        CASE WHEN dayofweek(cd.hour_timestamp) IN (1, 7) THEN true ELSE false END as is_weekend,
        false as is_holiday, -- Would need holiday calendar integration
        wf.temperature_celsius,
        COALESCE(wf.weather_condition, 'unknown') as weather_condition,
        
        -- Historical demand features
        LAG(cd.demand_count, 1) OVER (
            PARTITION BY cd.location_id 
            ORDER BY cd.hour_timestamp
        ) as historical_demand_1h_ago,
        
        LAG(cd.demand_count, 24) OVER (
            PARTITION BY cd.location_id 
            ORDER BY cd.hour_timestamp
        ) as historical_demand_24h_ago,
        
        LAG(cd.demand_count, 168) OVER (
            PARTITION BY cd.location_id 
            ORDER BY cd.hour_timestamp
        ) as historical_demand_168h_ago,
        
        -- Rolling averages
        AVG(cd.demand_count) OVER (
            PARTITION BY cd.location_id 
            ORDER BY cd.hour_timestamp 
            ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
        ) as rolling_avg_demand_7d,
        
        AVG(cd.demand_count) OVER (
            PARTITION BY cd.location_id 
            ORDER BY cd.hour_timestamp 
            ROWS BETWEEN 720 PRECEDING AND 1 PRECEDING
        ) as rolling_avg_demand_30d,
        
        zr.zone_type,
        zr.is_tourist_area,
        zr.is_business_district,
        
        -- Nearby zones demand (simplified)
        cd.demand_count as nearby_zones_demand,
        
        -- Weather impact factor
        CASE 
            WHEN wf.weather_condition IN ('rain', 'snow') THEN 1.3
            WHEN wf.weather_condition = 'fog' THEN 1.1
            WHEN wf.temperature_celsius < 0 THEN 1.2
            WHEN wf.temperature_celsius > 30 THEN 1.1
            ELSE 1.0 
        END as weather_impact_factor,
        
        1.0 as event_factor, -- Would need events calendar integration
        
        current_timestamp() as load_date,
        year(cd.hour_timestamp) as year,
        month(cd.hour_timestamp) as month
        
    FROM current_demand cd
    LEFT JOIN weather_features wf ON cd.hour_timestamp = wf.hour_timestamp
    LEFT JOIN zone_reference zr ON cd.location_id = zr.location_id
    WHERE cd.hour_timestamp >= current_date() - interval 1 day
    """
    
    print("Creating demand prediction features...")
    features_df = spark.sql(features_sql)
    
    # Delete existing data for the same date range
    spark.sql("""
        DELETE FROM iceberg.analytics.demand_prediction_features 
        WHERE prediction_datetime >= current_date() - interval 1 day
    """)
    
    # Insert new data
    features_df.writeTo("iceberg.analytics.demand_prediction_features").append()
    print("Demand prediction features created successfully")

def generate_summary_reports(spark):
    """Generate summary reports"""
    
    print("\n=== TRIP WEATHER CORRELATION SUMMARY ===")
    spark.sql("""
        SELECT 
            weather_condition,
            COUNT(*) as hours_count,
            AVG(total_trips) as avg_trips_per_hour,
            AVG(avg_trip_distance) as avg_distance,
            AVG(avg_fare_amount) as avg_fare
        FROM iceberg.analytics.trip_weather_correlation
        WHERE trip_date >= current_date() - interval 1 day
        GROUP BY weather_condition
        ORDER BY avg_trips_per_hour DESC
    """).show()
    
    print("\n=== TOP PERFORMING ZONES ===")
    spark.sql("""
        SELECT 
            zone_name,
            borough,
            SUM(total_pickups) as total_pickups,
            AVG(avg_fare_per_pickup) as avg_fare,
            AVG(peak_hour_factor) as avg_peak_factor
        FROM iceberg.analytics.zone_performance_metrics
        WHERE trip_date >= current_date() - interval 1 day
        GROUP BY zone_name, borough
        ORDER BY total_pickups DESC
        LIMIT 10
    """).show()
    
    print("\n=== WEATHER IMPACT ON DEMAND ===")
    spark.sql("""
        SELECT 
            weather_condition,
            AVG(weather_impact_factor) as avg_impact_factor,
            COUNT(DISTINCT location_id) as zones_affected,
            AVG(rolling_avg_demand_7d) as avg_weekly_demand
        FROM iceberg.analytics.demand_prediction_features
        WHERE prediction_datetime >= current_date() - interval 1 day
        GROUP BY weather_condition
        ORDER BY avg_impact_factor DESC
    """).show()

def main():
    """Main function"""
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("Starting comprehensive analytics pipeline...")
        
        # Create analytics tables
        create_analytics_tables(spark)
        
        # Create analytics
        create_trip_weather_correlation(spark)
        create_zone_performance_metrics(spark)
        create_demand_prediction_features(spark)
        
        # Generate summary reports
        generate_summary_reports(spark)
        
        print("Comprehensive analytics pipeline completed successfully!")
        
    except Exception as e:
        print(f"Error in comprehensive analytics pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
