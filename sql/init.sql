-- Initialize PostgreSQL database for NYC Taxi data
-- This script creates the necessary tables and enables logical replication

-- Create database if not exists (handled by environment variable)
-- \c taxi_db;

-- Create schema for taxi data
CREATE SCHEMA IF NOT EXISTS taxi;

-- Create the main taxi trips table
CREATE TABLE taxi.trips (
    id SERIAL PRIMARY KEY,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INTEGER,
    trip_distance DECIMAL(8,2),
    pickup_longitude DECIMAL(18,14),
    pickup_latitude DECIMAL(18,14),
    dropoff_longitude DECIMAL(18,14),
    dropoff_latitude DECIMAL(18,14),
    payment_type INTEGER,
    fare_amount DECIMAL(8,2),
    extra DECIMAL(8,2),
    mta_tax DECIMAL(8,2),
    tip_amount DECIMAL(8,2),
    tolls_amount DECIMAL(8,2),
    total_amount DECIMAL(8,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_trips_pickup_datetime ON taxi.trips(pickup_datetime);
CREATE INDEX idx_trips_dropoff_datetime ON taxi.trips(dropoff_datetime);
CREATE INDEX idx_trips_pickup_location ON taxi.trips(pickup_latitude, pickup_longitude);
CREATE INDEX idx_trips_dropoff_location ON taxi.trips(dropoff_latitude, dropoff_longitude);
CREATE INDEX idx_trips_created_at ON taxi.trips(created_at);

-- Create a lookup table for payment types
CREATE TABLE taxi.payment_types (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    description TEXT
);

-- Insert payment type data
INSERT INTO taxi.payment_types (id, name, description) VALUES
(1, 'Credit Card', 'Payment via credit card'),
(2, 'Cash', 'Cash payment'),
(3, 'No Charge', 'No charge trip'),
(4, 'Dispute', 'Disputed trip'),
(5, 'Unknown', 'Unknown payment method'),
(6, 'Voided Trip', 'Voided trip');

-- Create vendor lookup table
CREATE TABLE taxi.vendors (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT
);

-- Insert vendor data
INSERT INTO taxi.vendors (id, name, description) VALUES
(1, 'Creative Mobile Technologies', 'CMT'),
(2, 'VeriFone Inc', 'VTS'),
(3, 'Other', 'Other vendor');

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_trips_updated_at 
    BEFORE UPDATE ON taxi.trips 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Grant necessary permissions for replication
-- Create replication user (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'debezium') THEN
        CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
    END IF;
END
$$;

-- Grant permissions to debezium user
GRANT CONNECT ON DATABASE taxi_db TO debezium;
GRANT USAGE ON SCHEMA taxi TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA taxi TO debezium;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA taxi TO debezium;

-- Enable logical replication for the tables
ALTER TABLE taxi.trips REPLICA IDENTITY FULL;
ALTER TABLE taxi.payment_types REPLICA IDENTITY FULL;
ALTER TABLE taxi.vendors REPLICA IDENTITY FULL;

-- Create publication for Debezium
CREATE PUBLICATION dbz_publication FOR TABLE taxi.trips, taxi.payment_types, taxi.vendors;

-- Insert some sample data for testing
INSERT INTO taxi.trips (
    vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
    pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount
) VALUES
(1, '2024-01-01 12:00:00', '2024-01-01 12:15:00', 1, 2.5, 
 -73.9857, 40.7484, -73.9857, 40.7589, 1, 12.5, 0.5, 0.5, 2.5, 0.0, 15.5),
(2, '2024-01-01 12:05:00', '2024-01-01 12:25:00', 2, 4.2, 
 -73.9442, 40.8048, -73.9914, 40.7505, 1, 18.0, 0.5, 0.5, 3.6, 0.0, 22.6),
(1, '2024-01-01 12:10:00', '2024-01-01 12:20:00', 1, 1.8, 
 -73.9776, 40.7505, -73.9594, 40.7589, 2, 10.0, 0.5, 0.5, 0.0, 0.0, 11.0);

-- Create a view for trip summaries
CREATE OR REPLACE VIEW taxi.trip_summary AS
SELECT 
    DATE_TRUNC('hour', pickup_datetime) as hour,
    COUNT(*) as trip_count,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare,
    AVG(passenger_count) as avg_passengers
FROM taxi.trips
GROUP BY DATE_TRUNC('hour', pickup_datetime)
ORDER BY hour;

-- Create a materialized view for real-time analytics
CREATE MATERIALIZED VIEW taxi.hourly_stats AS
SELECT 
    DATE_TRUNC('hour', pickup_datetime) as hour,
    vendor_id,
    COUNT(*) as trip_count,
    SUM(passenger_count) as total_passengers,
    AVG(trip_distance) as avg_distance,
    SUM(fare_amount) as total_fare,
    AVG(fare_amount) as avg_fare,
    COUNT(DISTINCT payment_type) as payment_types_used
FROM taxi.trips
GROUP BY DATE_TRUNC('hour', pickup_datetime), vendor_id
ORDER BY hour DESC, vendor_id;

-- Create index on materialized view
CREATE INDEX idx_hourly_stats_hour ON taxi.hourly_stats(hour);
CREATE INDEX idx_hourly_stats_vendor ON taxi.hourly_stats(vendor_id);

-- Function to refresh materialized view
CREATE OR REPLACE FUNCTION refresh_hourly_stats()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW taxi.hourly_stats;
END;
$$ LANGUAGE plpgsql;

-- Display success message
SELECT 'Database initialized successfully!' as status;