#!/usr/bin/env python3
"""
NYC Taxi Data Generator for Session 1
Generates realistic taxi trip data and inserts it into PostgreSQL
to demonstrate CDC with Debezium and Kafka
"""

import os
import time
import random
import logging
from datetime import datetime, timedelta
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TaxiDataGenerator:
    def __init__(self, database_url):
        self.database_url = database_url
        self.conn = None
        self.cursor = None
        
        # NYC coordinates (approximate bounding box)
        self.nyc_bounds = {
            'min_lat': 40.4774,
            'max_lat': 40.9176,
            'min_lon': -74.2591,
            'max_lon': -73.7004
        }
        
        # Popular NYC locations for more realistic data
        self.popular_locations = [
            # Manhattan
            {'lat': 40.7589, 'lon': -73.9851, 'name': 'Times Square'},
            {'lat': 40.7505, 'lon': -73.9934, 'name': 'Penn Station'},
            {'lat': 40.7527, 'lon': -73.9772, 'name': 'Empire State Building'},
            {'lat': 40.7614, 'lon': -73.9776, 'name': 'Central Park'},
            {'lat': 40.7282, 'lon': -74.0776, 'name': 'Financial District'},
            
            # Brooklyn
            {'lat': 40.6892, 'lon': -73.9442, 'name': 'Brooklyn Heights'},
            {'lat': 40.6782, 'lon': -73.9442, 'name': 'Park Slope'},
            
            # Queens
            {'lat': 40.7282, 'lon': -73.7949, 'name': 'Jackson Heights'},
            {'lat': 40.7505, 'lon': -73.8803, 'name': 'Elmhurst'},
            
            # JFK Airport
            {'lat': 40.6413, 'lon': -73.7781, 'name': 'JFK Airport'},
            
            # LaGuardia Airport
            {'lat': 40.7769, 'lon': -73.8740, 'name': 'LaGuardia Airport'}
        ]
        
        self.connect_to_database()
        
    def connect_to_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(self.database_url)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def generate_coordinates(self, use_popular=True):
        """Generate pickup and dropoff coordinates"""
        if use_popular and random.random() < 0.7:  # 70% chance to use popular locations
            pickup = random.choice(self.popular_locations)
            dropoff = random.choice(self.popular_locations)
            
            # Add some random variation
            pickup_lat = pickup['lat'] + random.uniform(-0.01, 0.01)
            pickup_lon = pickup['lon'] + random.uniform(-0.01, 0.01)
            dropoff_lat = dropoff['lat'] + random.uniform(-0.01, 0.01)
            dropoff_lon = dropoff['lon'] + random.uniform(-0.01, 0.01)
        else:
            # Random coordinates within NYC bounds
            pickup_lat = random.uniform(self.nyc_bounds['min_lat'], self.nyc_bounds['max_lat'])
            pickup_lon = random.uniform(self.nyc_bounds['min_lon'], self.nyc_bounds['max_lon'])
            dropoff_lat = random.uniform(self.nyc_bounds['min_lat'], self.nyc_bounds['max_lat'])
            dropoff_lon = random.uniform(self.nyc_bounds['min_lon'], self.nyc_bounds['max_lon'])
        
        return pickup_lat, pickup_lon, dropoff_lat, dropoff_lon
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate approximate distance between two points (simplified)"""
        # Simple euclidean distance approximation for demo purposes
        lat_diff = abs(lat1 - lat2)
        lon_diff = abs(lon1 - lon2)
        return round(((lat_diff ** 2 + lon_diff ** 2) ** 0.5) * 69, 2)  # Rough miles conversion
    
    def generate_trip_data(self):
        """Generate realistic taxi trip data"""
        # Generate coordinates
        pickup_lat, pickup_lon, dropoff_lat, dropoff_lon = self.generate_coordinates()
        
        # Calculate trip distance
        trip_distance = self.calculate_distance(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)
        trip_distance = max(0.1, min(trip_distance, 50))  # Reasonable bounds
        
        # Generate pickup time (recent)
        now = datetime.now()
        pickup_time = now - timedelta(
            minutes=random.randint(0, 60),
            seconds=random.randint(0, 59)
        )
        
        # Generate dropoff time (after pickup)
        trip_duration_minutes = max(1, int(trip_distance * random.uniform(2, 8)))  # 2-8 minutes per mile
        dropoff_time = pickup_time + timedelta(minutes=trip_duration_minutes)
        
        # Generate other trip details
        vendor_id = random.choice([1, 2])
        passenger_count = random.choices([1, 2, 3, 4, 5, 6], weights=[50, 30, 10, 5, 3, 2])[0]
        payment_type = random.choices([1, 2, 3, 4], weights=[60, 30, 5, 5])[0]  # Credit card most common
        
        # Calculate fare (simplified pricing model)
        base_fare = 2.50
        per_mile_rate = 2.50
        per_minute_rate = 0.50
        
        distance_fare = trip_distance * per_mile_rate
        time_fare = trip_duration_minutes * per_minute_rate
        fare_amount = round(base_fare + distance_fare + time_fare, 2)
        
        # Additional charges
        extra = random.choice([0, 0.50, 1.00]) if random.random() < 0.3 else 0
        mta_tax = 0.50 if fare_amount > 0 else 0
        tip_amount = round(fare_amount * random.uniform(0.15, 0.25), 2) if payment_type == 1 else 0
        tolls_amount = random.choice([0, 5.76, 6.50, 9.75]) if random.random() < 0.1 else 0
        
        total_amount = fare_amount + extra + mta_tax + tip_amount + tolls_amount
        
        return {
            'vendor_id': vendor_id,
            'pickup_datetime': pickup_time,
            'dropoff_datetime': dropoff_time,
            'passenger_count': passenger_count,
            'trip_distance': Decimal(str(trip_distance)),
            'pickup_longitude': Decimal(str(pickup_lon)),
            'pickup_latitude': Decimal(str(pickup_lat)),
            'dropoff_longitude': Decimal(str(dropoff_lon)),
            'dropoff_latitude': Decimal(str(dropoff_lat)),
            'payment_type': payment_type,
            'fare_amount': Decimal(str(fare_amount)),
            'extra': Decimal(str(extra)),
            'mta_tax': Decimal(str(mta_tax)),
            'tip_amount': Decimal(str(tip_amount)),
            'tolls_amount': Decimal(str(tolls_amount)),
            'total_amount': Decimal(str(total_amount))
        }
    
    def insert_trip(self, trip_data):
        """Insert a single trip into the database"""
        try:
            insert_query = """
            INSERT INTO taxi.trips (
                vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
                pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
                payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount
            ) VALUES (
                %(vendor_id)s, %(pickup_datetime)s, %(dropoff_datetime)s, %(passenger_count)s, %(trip_distance)s,
                %(pickup_longitude)s, %(pickup_latitude)s, %(dropoff_longitude)s, %(dropoff_latitude)s,
                %(payment_type)s, %(fare_amount)s, %(extra)s, %(mta_tax)s, %(tip_amount)s, %(tolls_amount)s, %(total_amount)s
            ) RETURNING id;
            """
            
            self.cursor.execute(insert_query, trip_data)
            trip_id = self.cursor.fetchone()['id']
            self.conn.commit()
            
            return trip_id
            
        except Exception as e:
            logger.error(f"Error inserting trip: {e}")
            self.conn.rollback()
            raise
    
    def insert_batch(self, batch_size=100):
        """Insert a batch of trips"""
        try:
            trips = []
            for _ in range(batch_size):
                trip_data = self.generate_trip_data()
                trips.append(trip_data)
            
            insert_query = """
            INSERT INTO taxi.trips (
                vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
                pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
                payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount
            ) VALUES (
                %(vendor_id)s, %(pickup_datetime)s, %(dropoff_datetime)s, %(passenger_count)s, %(trip_distance)s,
                %(pickup_longitude)s, %(pickup_latitude)s, %(dropoff_longitude)s, %(dropoff_latitude)s,
                %(payment_type)s, %(fare_amount)s, %(extra)s, %(mta_tax)s, %(tip_amount)s, %(tolls_amount)s, %(total_amount)s
            );
            """
            
            self.cursor.executemany(insert_query, trips)
            self.conn.commit()
            
            logger.info(f"Inserted batch of {batch_size} trips")
            return len(trips)
            
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            self.conn.rollback()
            raise
    
    def simulate_updates(self, num_updates=10):
        """Simulate updates to existing trips (for CDC testing)"""
        try:
            # Get some recent trip IDs
            self.cursor.execute("""
                SELECT id FROM taxi.trips 
                ORDER BY created_at DESC 
                LIMIT %s
            """, (num_updates * 2,))
            
            trip_ids = [row['id'] for row in self.cursor.fetchall()]
            
            if not trip_ids:
                logger.warning("No trips found to update")
                return
            
            selected_ids = random.sample(trip_ids, min(num_updates, len(trip_ids)))
            
            for trip_id in selected_ids:
                # Simulate tip amount update (common in real scenarios)
                new_tip = round(random.uniform(0, 10), 2)
                
                update_query = """
                UPDATE taxi.trips 
                SET tip_amount = %s, 
                    total_amount = fare_amount + extra + mta_tax + %s + tolls_amount,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """
                
                self.cursor.execute(update_query, (new_tip, new_tip, trip_id))
            
            self.conn.commit()
            logger.info(f"Updated {len(selected_ids)} trips")
            
        except Exception as e:
            logger.error(f"Error updating trips: {e}")
            self.conn.rollback()
            raise
    
    def get_stats(self):
        """Get current database statistics"""
        try:
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_trips,
                    MAX(created_at) as latest_trip,
                    AVG(fare_amount) as avg_fare,
                    AVG(trip_distance) as avg_distance
                FROM taxi.trips
            """)
            
            stats = self.cursor.fetchone()
            return dict(stats)
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")


def main():
    """Main function to run the data generator"""
    # Get configuration from environment variables
    database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/taxi_db')
    batch_size = int(os.getenv('BATCH_SIZE', 50))
    interval_seconds = int(os.getenv('INTERVAL_SECONDS', 30))
    
    logger.info(f"Starting taxi data generator with batch_size={batch_size}, interval={interval_seconds}s")
    
    generator = TaxiDataGenerator(database_url)
    
    try:
        # Initial stats
        stats = generator.get_stats()
        logger.info(f"Initial stats: {json.dumps(stats, indent=2, default=str)}")
        
        iteration = 0
        while True:
            iteration += 1
            logger.info(f"Starting iteration {iteration}")
            
            # Insert new trips
            generator.insert_batch(batch_size)
            
            # Occasionally simulate updates (every 5th iteration)
            if iteration % 5 == 0:
                generator.simulate_updates(random.randint(1, 5))
            
            # Print stats every 10 iterations
            if iteration % 10 == 0:
                stats = generator.get_stats()
                logger.info(f"Current stats: {json.dumps(stats, indent=2, default=str)}")
            
            # Wait before next iteration
            time.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        generator.close()


if __name__ == "__main__":
    main()