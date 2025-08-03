import asyncio
import pandas as pd
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import logging

logger = logging.getLogger(__name__)

class DatabaseService:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, pool_pre_ping=True, pool_recycle=300)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as pandas DataFrame"""
        try:
            with self.engine.connect() as connection:
                result = pd.read_sql(query, connection, params=params)
                return result
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            raise
    
    def get_recent_trips(self, limit: int = 100, hours_back: int = 24) -> pd.DataFrame:
        """Get recent trips from the database"""
        query = """
            SELECT 
                trip_id,
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_location_id,
                dropoff_location_id,
                fare_amount,
                tip_amount,
                total_amount,
                payment_type
            FROM trips 
            WHERE pickup_datetime >= NOW() - INTERVAL '{hours} hours'
            ORDER BY pickup_datetime DESC 
            LIMIT {limit}
        """.format(hours=hours_back, limit=limit)
        
        return self.execute_query(query)
    
    def get_zone_metrics(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get aggregated metrics by zone"""
        query = """
            WITH zone_stats AS (
                SELECT 
                    COALESCE(t.pickup_location_id, t.dropoff_location_id) as zone_id,
                    COUNT(*) as total_trips,
                    AVG(t.fare_amount) as avg_fare,
                    SUM(t.total_amount) as total_revenue,
                    AVG(t.trip_distance) as avg_distance,
                    COUNT(CASE WHEN EXTRACT(hour FROM t.pickup_datetime) BETWEEN 17 AND 19 THEN 1 END) as peak_trips
                FROM trips t
                WHERE t.pickup_datetime BETWEEN %(start_date)s AND %(end_date)s
                    AND t.fare_amount > 0
                GROUP BY COALESCE(t.pickup_location_id, t.dropoff_location_id)
            )
            SELECT 
                zs.zone_id,
                COALESCE(tz.zone_name, 'Unknown Zone') as zone_name,
                COALESCE(tz.borough, 'Unknown') as borough,
                zs.total_trips,
                ROUND(zs.avg_fare::numeric, 2) as avg_fare,
                ROUND(zs.total_revenue::numeric, 2) as total_revenue,
                ROUND(zs.avg_distance::numeric, 2) as avg_distance,
                ROUND((zs.peak_trips::float / NULLIF(zs.total_trips, 0) * 100)::numeric, 2) as peak_hour_factor
            FROM zone_stats zs
            LEFT JOIN taxi_zones tz ON zs.zone_id = tz.location_id
            WHERE zs.zone_id IS NOT NULL
            ORDER BY zs.total_revenue DESC
        """
        
        return self.execute_query(query, {
            'start_date': start_date,
            'end_date': end_date
        })
    
    def get_hourly_trip_counts(self, days_back: int = 7) -> pd.DataFrame:
        """Get hourly trip counts for time series analysis"""
        query = """
            SELECT 
                DATE_TRUNC('hour', pickup_datetime) as hour,
                COUNT(*) as trip_count,
                AVG(fare_amount) as avg_fare,
                SUM(total_amount) as total_revenue
            FROM trips
            WHERE pickup_datetime >= NOW() - INTERVAL '{days} days'
                AND fare_amount > 0
            GROUP BY DATE_TRUNC('hour', pickup_datetime)
            ORDER BY hour
        """.format(days=days_back)
        
        return self.execute_query(query)
    
    def get_weather_impact(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get weather impact on trip patterns (synthetic data for demo)"""
        # This would normally join with weather data
        # For demo, we'll generate synthetic weather correlation
        query = """
        WITH daily_stats AS (
            SELECT 
                DATE(pickup_datetime) as trip_date,
                COUNT(*) as total_trips,
                AVG(fare_amount) as avg_fare,
                -- Synthetic weather data for demo
                CASE 
                    WHEN EXTRACT(dow FROM pickup_datetime) IN (0, 6) THEN 'Weekend'
                    WHEN EXTRACT(hour FROM pickup_datetime) BETWEEN 7 AND 9 THEN 'Rush Hour'
                    WHEN EXTRACT(hour FROM pickup_datetime) BETWEEN 17 AND 19 THEN 'Evening Rush'
                    ELSE 'Regular'
                END as time_category,
                -- Simulate weather conditions
                CASE 
                    WHEN RANDOM() < 0.2 THEN 'Rainy'
                    WHEN RANDOM() < 0.1 THEN 'Snowy'
                    WHEN RANDOM() < 0.3 THEN 'Cloudy'
                    ELSE 'Clear'
                END as weather_condition,
                20 + (RANDOM() * 15) as temperature_celsius,
                40 + (RANDOM() * 40) as humidity
            FROM trips
            WHERE pickup_datetime BETWEEN %(start_date)s AND %(end_date)s
                AND fare_amount > 0
            GROUP BY DATE(pickup_datetime)
        )
        SELECT 
            trip_date,
            weather_condition,
            temperature_celsius,
            humidity,
            total_trips,
            ROUND(avg_fare::numeric, 2) as avg_fare_amount,
            -- Calculate weather impact score (higher = more impact)
            CASE weather_condition
                WHEN 'Rainy' THEN LEAST(total_trips * 1.3, 100)
                WHEN 'Snowy' THEN LEAST(total_trips * 1.5, 100)
                WHEN 'Cloudy' THEN LEAST(total_trips * 1.1, 100)
                ELSE total_trips
            END as weather_impact_score
        FROM daily_stats
        ORDER BY trip_date DESC
        """
        
        return self.execute_query(query, {
            'start_date': start_date,
            'end_date': end_date
        })
    
    def get_demand_prediction(self, hours_ahead: int = 24) -> pd.DataFrame:
        """Generate demand predictions (ML model would go here)"""
        query = """
        WITH recent_patterns AS (
            SELECT 
                pickup_location_id as location_id,
                EXTRACT(hour FROM pickup_datetime) as hour_of_day,
                EXTRACT(dow FROM pickup_datetime) as day_of_week,
                COUNT(*) as historical_count,
                AVG(fare_amount) as avg_fare
            FROM trips
            WHERE pickup_datetime >= NOW() - INTERVAL '30 days'
                AND pickup_location_id IS NOT NULL
                AND fare_amount > 0
            GROUP BY pickup_location_id, EXTRACT(hour FROM pickup_datetime), EXTRACT(dow FROM pickup_datetime)
        ),
        predictions AS (
            SELECT 
                rp.location_id,
                tz.zone_name,
                NOW() + (generate_series(1, {hours}) || ' hours')::interval as prediction_hour,
                rp.historical_count * (0.8 + RANDOM() * 0.4) as predicted_demand,
                0.7 + (RANDOM() * 0.25) as confidence_score,
                CASE 
                    WHEN RANDOM() < 0.3 THEN 1.2  -- Weather boost
                    WHEN RANDOM() < 0.2 THEN 0.8  -- Weather reduction
                    ELSE 1.0
                END as weather_impact_factor,
                rp.historical_count as historical_avg
            FROM recent_patterns rp
            LEFT JOIN taxi_zones tz ON rp.location_id = tz.location_id
            WHERE rp.historical_count > 5  -- Only zones with significant activity
        )
        SELECT 
            location_id,
            COALESCE(zone_name, 'Unknown Zone') as zone_name,
            prediction_hour,
            ROUND(predicted_demand::numeric, 1) as predicted_demand,
            ROUND(confidence_score::numeric, 3) as confidence_score,
            ROUND(weather_impact_factor::numeric, 2) as weather_impact_factor,
            ROUND(historical_avg::numeric, 1) as historical_avg
        FROM predictions
        ORDER BY predicted_demand DESC, prediction_hour
        LIMIT 1000
        """.format(hours=hours_ahead)
        
        return self.execute_query(query)
    
    def get_real_time_activity(self, minutes_back: int = 60) -> pd.DataFrame:
        """Get real-time activity by zone"""
        query = """
        WITH recent_activity AS (
            SELECT 
                pickup_location_id as zone_id,
                COUNT(*) as pickup_count,
                SUM(total_amount) as revenue_last_hour,
                AVG(EXTRACT(epoch FROM (dropoff_datetime - pickup_datetime))/60) as avg_trip_duration
            FROM trips
            WHERE pickup_datetime >= NOW() - INTERVAL '{minutes} minutes'
                AND pickup_location_id IS NOT NULL
                AND dropoff_datetime IS NOT NULL
                AND fare_amount > 0
            GROUP BY pickup_location_id
        )
        SELECT 
            ra.zone_id,
            COALESCE(tz.zone_name, 'Unknown Zone') as zone_name,
            NOW() as activity_timestamp,
            -- Activity score based on trips and revenue
            LEAST((ra.pickup_count * 10 + ra.revenue_last_hour / 10)::numeric, 100) as activity_score,
            ra.pickup_count,
            ROUND(ra.revenue_last_hour::numeric, 2) as revenue_last_hour,
            ROUND(ra.avg_trip_duration::numeric, 1) as avg_wait_time
        FROM recent_activity ra
        LEFT JOIN taxi_zones tz ON ra.zone_id = tz.location_id
        WHERE ra.pickup_count > 0
        ORDER BY activity_score DESC
        """.format(minutes=minutes_back)
        
        return self.execute_query(query)
    
    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get key statistics for dashboard"""
        today = datetime.now().date()
        
        # Today's stats
        today_query = """
            SELECT 
                COUNT(*) as total_trips,
                COALESCE(SUM(total_amount), 0) as total_revenue,
                COALESCE(AVG(fare_amount), 0) as avg_fare,
                COUNT(DISTINCT pickup_location_id) as active_zones
            FROM trips 
            WHERE DATE(pickup_datetime) = CURRENT_DATE
                AND fare_amount > 0
        """
        
        today_stats = self.execute_query(today_query).iloc[0].to_dict()
        
        # Peak hour
        peak_query = """
            SELECT 
                EXTRACT(hour FROM pickup_datetime) as hour,
                COUNT(*) as trip_count
            FROM trips 
            WHERE DATE(pickup_datetime) = CURRENT_DATE
            GROUP BY EXTRACT(hour FROM pickup_datetime)
            ORDER BY trip_count DESC
            LIMIT 1
        """
        
        peak_result = self.execute_query(peak_query)
        peak_hour = f"{int(peak_result.iloc[0]['hour'])}:00" if not peak_result.empty else "N/A"
        
        # Top zones
        top_zones_query = """
        SELECT 
            COALESCE(tz.zone_name, 'Unknown') as zone_name,
            COUNT(*) as trips,
            SUM(total_amount) as revenue
        FROM trips t
        LEFT JOIN taxi_zones tz ON t.pickup_location_id = tz.location_id
        WHERE DATE(t.pickup_datetime) = CURRENT_DATE
            AND t.fare_amount > 0
        GROUP BY tz.zone_name
        ORDER BY revenue DESC
        LIMIT 5
        """
        
        top_zones = self.execute_query(top_zones_query).to_dict('records')
        
        return {
            'total_trips_today': int(today_stats['total_trips']),
            'total_revenue_today': float(today_stats['total_revenue']),
            'avg_fare_today': float(today_stats['avg_fare']),
            'active_zones': int(today_stats['active_zones']),
            'peak_hour': peak_hour,
            'weather_condition': 'Clear',  # Would come from weather API
            'top_zones': top_zones
        }
