"""
ML Feature Engineering Pipeline
Creates features for machine learning models using taxi, weather, and CDC data
"""

import sys
import os

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.sql.window import Window
    from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
    from pyspark.ml import Pipeline
except ImportError:
    print("PySpark not available in this environment")

def create_spark_session():
    """Create Spark session with ML and Iceberg configuration"""
    return SparkSession.builder \
        .appName("ML Feature Engineering Pipeline") \
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

def create_ml_tables(spark):
    """Create ML feature tables"""
    
    # Create ML database
    spark.sql("CREATE DATABASE IF NOT EXISTS ml")
    
    # Demand prediction features table
    demand_features_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.ml.demand_prediction_features (
        location_id int,
        prediction_hour timestamp,
        target_demand bigint,
        
        -- Time features
        hour_of_day int,
        day_of_week int,
        day_of_month int,
        month int,
        is_weekend boolean,
        is_holiday boolean,
        
        -- Weather features
        temperature_celsius double,
        humidity_percent double,
        wind_speed_kmh double,
        weather_condition_encoded double,
        
        -- Historical demand features
        demand_1h_ago bigint,
        demand_24h_ago bigint,
        demand_168h_ago bigint,
        rolling_avg_7d double,
        rolling_avg_30d double,
        rolling_std_7d double,
        
        -- Zone features
        zone_type_encoded double,
        is_tourist_area_flag int,
        is_business_district_flag int,
        zone_popularity_score double,
        
        -- Interaction features
        temp_hour_interaction double,
        weather_weekend_interaction double,
        tourist_weekend_interaction double,
        
        -- Lag features for different time periods
        demand_lag_2h bigint,
        demand_lag_3h bigint,
        demand_same_hour_last_week bigint,
        
        -- Feature set metadata
        feature_date date,
        created_at timestamp
    ) USING iceberg
    PARTITIONED BY (feature_date)
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    """
    
    # Fare prediction features table
    fare_features_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.ml.fare_prediction_features (
        trip_id string,
        
        -- Target variable
        fare_amount double,
        
        -- Trip features
        trip_distance double,
        trip_duration_minutes double,
        passenger_count int,
        
        -- Location features
        pickup_zone_id int,
        dropoff_zone_id int,
        pickup_borough_encoded double,
        dropoff_borough_encoded double,
        zone_distance_km double,
        is_airport_trip boolean,
        is_cross_borough boolean,
        
        -- Time features
        pickup_hour int,
        pickup_day_of_week int,
        is_rush_hour boolean,
        is_weekend boolean,
        
        -- Weather features
        weather_condition_encoded double,
        temperature_celsius double,
        is_bad_weather boolean,
        
        -- Historical pricing features
        avg_fare_same_route_7d double,
        avg_fare_pickup_zone_1h double,
        surge_factor double,
        
        -- Interaction features
        distance_duration_ratio double,
        weather_distance_interaction double,
        rush_hour_distance_interaction double,
        
        -- Feature set metadata
        feature_date date,
        created_at timestamp
    ) USING iceberg
    PARTITIONED BY (feature_date)
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    """
    
    try:
        spark.sql(demand_features_sql)
        spark.sql(fare_features_sql)
        print("Created/verified ML feature tables")
    except Exception as e:
        print(f"Error creating ML feature tables: {e}")

def create_demand_prediction_features(spark):
    """Create features for demand prediction model"""
    
    features_sql = """
    WITH hourly_demand AS (
        SELECT 
            pickup_location_id as location_id,
            date_trunc('hour', pickup_datetime) as hour_timestamp,
            COUNT(*) as demand_count
        FROM iceberg.nyc_taxi.trips
        WHERE pickup_datetime >= current_date() - interval 30 days
        GROUP BY pickup_location_id, date_trunc('hour', pickup_datetime)
    ),
    weather_hourly AS (
        SELECT 
            date_trunc('hour', timestamp) as hour_timestamp,
            temperature_celsius,
            humidity_percent,
            wind_speed_kmh,
            weather_condition,
            CASE 
                WHEN weather_condition = 'clear' THEN 1.0
                WHEN weather_condition = 'cloudy' THEN 2.0
                WHEN weather_condition = 'rain' THEN 3.0
                WHEN weather_condition = 'snow' THEN 4.0
                WHEN weather_condition = 'fog' THEN 5.0
                ELSE 0.0
            END as weather_condition_encoded
        FROM iceberg.weather.hourly_weather
        WHERE timestamp >= current_date() - interval 30 days
    ),
    zone_info AS (
        SELECT 
            location_id,
            zone_type,
            is_tourist_area,
            is_business_district,
            CASE 
                WHEN zone_type = 'airport' THEN 3.0
                WHEN zone_type = 'neighborhood' THEN 1.0
                ELSE 2.0
            END as zone_type_encoded
        FROM iceberg.reference.taxi_zones
    ),
    zone_popularity AS (
        SELECT 
            location_id,
            AVG(demand_count) as avg_demand,
            NTILE(10) OVER (ORDER BY AVG(demand_count)) as popularity_decile
        FROM hourly_demand
        GROUP BY location_id
    ),
    feature_base AS (
        SELECT 
            hd.location_id,
            hd.hour_timestamp as prediction_hour,
            hd.demand_count as target_demand,
            
            -- Time features
            hour(hd.hour_timestamp) as hour_of_day,
            dayofweek(hd.hour_timestamp) as day_of_week,
            dayofmonth(hd.hour_timestamp) as day_of_month,
            month(hd.hour_timestamp) as month,
            CASE WHEN dayofweek(hd.hour_timestamp) IN (1, 7) THEN true ELSE false END as is_weekend,
            false as is_holiday, -- Would need holiday calendar
            
            -- Weather features
            COALESCE(wh.temperature_celsius, 20.0) as temperature_celsius,
            COALESCE(wh.humidity_percent, 50.0) as humidity_percent,
            COALESCE(wh.wind_speed_kmh, 5.0) as wind_speed_kmh,
            COALESCE(wh.weather_condition_encoded, 1.0) as weather_condition_encoded,
            
            -- Zone features
            zi.zone_type_encoded,
            CASE WHEN zi.is_tourist_area THEN 1 ELSE 0 END as is_tourist_area_flag,
            CASE WHEN zi.is_business_district THEN 1 ELSE 0 END as is_business_district_flag,
            COALESCE(zp.popularity_decile, 5.0) as zone_popularity_score
            
        FROM hourly_demand hd
        LEFT JOIN weather_hourly wh ON hd.hour_timestamp = wh.hour_timestamp
        LEFT JOIN zone_info zi ON hd.location_id = zi.location_id
        LEFT JOIN zone_popularity zp ON hd.location_id = zp.location_id
        WHERE hd.hour_timestamp >= current_date() - interval 7 days
    ),
    features_with_lags AS (
        SELECT 
            *,
            -- Historical demand features
            LAG(target_demand, 1) OVER (
                PARTITION BY location_id ORDER BY prediction_hour
            ) as demand_1h_ago,
            
            LAG(target_demand, 24) OVER (
                PARTITION BY location_id ORDER BY prediction_hour
            ) as demand_24h_ago,
            
            LAG(target_demand, 168) OVER (
                PARTITION BY location_id ORDER BY prediction_hour
            ) as demand_168h_ago,
            
            LAG(target_demand, 2) OVER (
                PARTITION BY location_id ORDER BY prediction_hour
            ) as demand_lag_2h,
            
            LAG(target_demand, 3) OVER (
                PARTITION BY location_id ORDER BY prediction_hour
            ) as demand_lag_3h,
            
            -- Rolling statistics
            AVG(target_demand) OVER (
                PARTITION BY location_id 
                ORDER BY prediction_hour 
                ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
            ) as rolling_avg_7d,
            
            AVG(target_demand) OVER (
                PARTITION BY location_id 
                ORDER BY prediction_hour 
                ROWS BETWEEN 720 PRECEDING AND 1 PRECEDING
            ) as rolling_avg_30d,
            
            STDDEV(target_demand) OVER (
                PARTITION BY location_id 
                ORDER BY prediction_hour 
                ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
            ) as rolling_std_7d
            
        FROM feature_base
    )
    SELECT 
        location_id,
        prediction_hour,
        target_demand,
        hour_of_day,
        day_of_week,
        day_of_month,
        month,
        is_weekend,
        is_holiday,
        temperature_celsius,
        humidity_percent,
        wind_speed_kmh,
        weather_condition_encoded,
        COALESCE(demand_1h_ago, 0) as demand_1h_ago,
        COALESCE(demand_24h_ago, 0) as demand_24h_ago,
        COALESCE(demand_168h_ago, 0) as demand_168h_ago,
        COALESCE(rolling_avg_7d, 0.0) as rolling_avg_7d,
        COALESCE(rolling_avg_30d, 0.0) as rolling_avg_30d,
        COALESCE(rolling_std_7d, 1.0) as rolling_std_7d,
        zone_type_encoded,
        is_tourist_area_flag,
        is_business_district_flag,
        zone_popularity_score,
        
        -- Interaction features
        temperature_celsius * hour_of_day as temp_hour_interaction,
        weather_condition_encoded * CASE WHEN is_weekend THEN 1.0 ELSE 0.0 END as weather_weekend_interaction,
        is_tourist_area_flag * CASE WHEN is_weekend THEN 1.0 ELSE 0.0 END as tourist_weekend_interaction,
        
        COALESCE(demand_lag_2h, 0) as demand_lag_2h,
        COALESCE(demand_lag_3h, 0) as demand_lag_3h,
        COALESCE(demand_168h_ago, 0) as demand_same_hour_last_week,
        
        date(prediction_hour) as feature_date,
        current_timestamp() as created_at
        
    FROM features_with_lags
    WHERE prediction_hour >= current_date() - interval 1 day
    """
    
    print("Creating demand prediction features...")
    features_df = spark.sql(features_sql)
    
    # Delete existing data for the same date range
    spark.sql("""
        DELETE FROM iceberg.ml.demand_prediction_features 
        WHERE feature_date >= current_date() - interval 1 day
    """)
    
    # Insert new features
    features_df.writeTo("iceberg.ml.demand_prediction_features").append()
    print("Demand prediction features created successfully")

def create_fare_prediction_features(spark):
    """Create features for fare prediction model"""
    
    fare_features_sql = """
    WITH trip_base AS (
        SELECT 
            CAST(vendor_id as string) || '_' || CAST(unix_timestamp(pickup_datetime) as string) as trip_id,
            fare_amount,
            trip_distance,
            (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60 as trip_duration_minutes,
            passenger_count,
            pickup_location_id,
            dropoff_location_id,
            pickup_datetime,
            hour(pickup_datetime) as pickup_hour,
            dayofweek(pickup_datetime) as pickup_day_of_week,
            CASE WHEN dayofweek(pickup_datetime) IN (1, 7) THEN true ELSE false END as is_weekend,
            CASE WHEN hour(pickup_datetime) BETWEEN 7 AND 9 OR hour(pickup_datetime) BETWEEN 17 AND 19 
                 THEN true ELSE false END as is_rush_hour
        FROM iceberg.nyc_taxi.trips
        WHERE pickup_datetime >= current_date() - interval 7 days
        AND fare_amount > 0 
        AND trip_distance > 0
    ),
    zone_info AS (
        SELECT 
            location_id,
            borough,
            zone_type,
            is_tourist_area,
            is_business_district,
            latitude,
            longitude,
            CASE 
                WHEN borough = 'Manhattan' THEN 1.0
                WHEN borough = 'Brooklyn' THEN 2.0
                WHEN borough = 'Queens' THEN 3.0
                WHEN borough = 'Bronx' THEN 4.0
                WHEN borough = 'Staten Island' THEN 5.0
                ELSE 0.0
            END as borough_encoded
        FROM iceberg.reference.taxi_zones
    ),
    weather_features AS (
        SELECT 
            date_trunc('hour', timestamp) as hour_timestamp,
            weather_condition,
            temperature_celsius,
            CASE 
                WHEN weather_condition = 'clear' THEN 1.0
                WHEN weather_condition = 'cloudy' THEN 2.0
                WHEN weather_condition = 'rain' THEN 3.0
                WHEN weather_condition = 'snow' THEN 4.0
                WHEN weather_condition = 'fog' THEN 5.0
                ELSE 1.0
            END as weather_condition_encoded,
            CASE WHEN weather_condition IN ('rain', 'snow', 'fog') THEN true ELSE false END as is_bad_weather
        FROM iceberg.weather.hourly_weather
        WHERE timestamp >= current_date() - interval 7 days
    ),
    route_history AS (
        SELECT 
            pickup_location_id,
            dropoff_location_id,
            AVG(fare_amount) as avg_fare_same_route_7d,
            COUNT(*) as route_frequency
        FROM iceberg.nyc_taxi.trips
        WHERE pickup_datetime >= current_date() - interval 7 days
        GROUP BY pickup_location_id, dropoff_location_id
    ),
    zone_hourly_stats AS (
        SELECT 
            pickup_location_id,
            date_trunc('hour', pickup_datetime) as hour_timestamp,
            AVG(fare_amount) as avg_fare_pickup_zone_1h,
            COUNT(*) as trips_in_hour,
            CASE 
                WHEN COUNT(*) > AVG(COUNT(*)) OVER (PARTITION BY pickup_location_id) * 1.5 THEN 1.2
                WHEN COUNT(*) < AVG(COUNT(*)) OVER (PARTITION BY pickup_location_id) * 0.5 THEN 0.8
                ELSE 1.0
            END as surge_factor
        FROM iceberg.nyc_taxi.trips
        WHERE pickup_datetime >= current_date() - interval 7 days
        GROUP BY pickup_location_id, date_trunc('hour', pickup_datetime)
    )
    SELECT 
        tb.trip_id,
        tb.fare_amount,
        tb.trip_distance,
        tb.trip_duration_minutes,
        tb.passenger_count,
        tb.pickup_location_id as pickup_zone_id,
        tb.dropoff_location_id as dropoff_zone_id,
        
        pickup_zone.borough_encoded as pickup_borough_encoded,
        dropoff_zone.borough_encoded as dropoff_borough_encoded,
        
        -- Calculate zone distance using simplified formula
        SQRT(POW(pickup_zone.latitude - dropoff_zone.latitude, 2) + 
             POW(pickup_zone.longitude - dropoff_zone.longitude, 2)) * 111 as zone_distance_km,
        
        CASE WHEN pickup_zone.zone_type = 'airport' OR dropoff_zone.zone_type = 'airport' 
             THEN true ELSE false END as is_airport_trip,
        
        CASE WHEN pickup_zone.borough != dropoff_zone.borough 
             THEN true ELSE false END as is_cross_borough,
        
        tb.pickup_hour,
        tb.pickup_day_of_week,
        tb.is_rush_hour,
        tb.is_weekend,
        
        COALESCE(wf.weather_condition_encoded, 1.0) as weather_condition_encoded,
        COALESCE(wf.temperature_celsius, 20.0) as temperature_celsius,
        COALESCE(wf.is_bad_weather, false) as is_bad_weather,
        
        COALESCE(rh.avg_fare_same_route_7d, tb.fare_amount) as avg_fare_same_route_7d,
        COALESCE(zhs.avg_fare_pickup_zone_1h, tb.fare_amount) as avg_fare_pickup_zone_1h,
        COALESCE(zhs.surge_factor, 1.0) as surge_factor,
        
        -- Interaction features
        CASE WHEN tb.trip_duration_minutes > 0 
             THEN tb.trip_distance / (tb.trip_duration_minutes / 60.0) 
             ELSE 0.0 END as distance_duration_ratio,
        
        tb.trip_distance * COALESCE(wf.weather_condition_encoded, 1.0) as weather_distance_interaction,
        
        tb.trip_distance * CASE WHEN tb.is_rush_hour THEN 1.0 ELSE 0.0 END as rush_hour_distance_interaction,
        
        date(tb.pickup_datetime) as feature_date,
        current_timestamp() as created_at
        
    FROM trip_base tb
    LEFT JOIN zone_info pickup_zone ON tb.pickup_location_id = pickup_zone.location_id
    LEFT JOIN zone_info dropoff_zone ON tb.dropoff_location_id = dropoff_zone.location_id
    LEFT JOIN weather_features wf ON date_trunc('hour', tb.pickup_datetime) = wf.hour_timestamp
    LEFT JOIN route_history rh ON tb.pickup_location_id = rh.pickup_location_id 
        AND tb.dropoff_location_id = rh.dropoff_location_id
    LEFT JOIN zone_hourly_stats zhs ON tb.pickup_location_id = zhs.pickup_location_id 
        AND date_trunc('hour', tb.pickup_datetime) = zhs.hour_timestamp
    WHERE pickup_zone.location_id IS NOT NULL 
    AND dropoff_zone.location_id IS NOT NULL
    """
    
    print("Creating fare prediction features...")
    features_df = spark.sql(fare_features_sql)
    
    # Delete existing data for the same date range
    spark.sql("""
        DELETE FROM iceberg.ml.fare_prediction_features 
        WHERE feature_date >= current_date() - interval 1 day
    """)
    
    # Insert new features
    features_df.writeTo("iceberg.ml.fare_prediction_features").append()
    print("Fare prediction features created successfully")

def generate_feature_statistics(spark):
    """Generate feature statistics and data quality reports"""
    
    print("\n=== DEMAND PREDICTION FEATURES STATISTICS ===")
    spark.sql("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT location_id) as unique_locations,
            AVG(target_demand) as avg_demand,
            STDDEV(target_demand) as stddev_demand,
            MIN(prediction_hour) as earliest_hour,
            MAX(prediction_hour) as latest_hour
        FROM iceberg.ml.demand_prediction_features
        WHERE feature_date >= current_date() - interval 1 day
    """).show()
    
    print("\n=== FARE PREDICTION FEATURES STATISTICS ===")
    spark.sql("""
        SELECT 
            COUNT(*) as total_trips,
            AVG(fare_amount) as avg_fare,
            STDDEV(fare_amount) as stddev_fare,
            AVG(trip_distance) as avg_distance,
            AVG(trip_duration_minutes) as avg_duration,
            COUNT(DISTINCT pickup_zone_id) as unique_pickup_zones,
            COUNT(DISTINCT dropoff_zone_id) as unique_dropoff_zones
        FROM iceberg.ml.fare_prediction_features
        WHERE feature_date >= current_date() - interval 1 day
    """).show()
    
    print("\n=== FEATURE CORRELATION ANALYSIS ===")
    spark.sql("""
        SELECT 
            CORR(target_demand, temperature_celsius) as demand_temp_corr,
            CORR(target_demand, demand_24h_ago) as demand_seasonal_corr,
            CORR(target_demand, zone_popularity_score) as demand_popularity_corr
        FROM iceberg.ml.demand_prediction_features
        WHERE feature_date >= current_date() - interval 1 day
        AND demand_24h_ago IS NOT NULL
    """).show()

def main():
    """Main function"""
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("Starting ML feature engineering pipeline...")
        
        # Create ML feature tables
        create_ml_tables(spark)
        
        # Create feature sets
        create_demand_prediction_features(spark)
        create_fare_prediction_features(spark)
        
        # Generate statistics
        generate_feature_statistics(spark)
        
        print("ML feature engineering pipeline completed successfully!")
        
    except Exception as e:
        print(f"Error in ML feature engineering pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
