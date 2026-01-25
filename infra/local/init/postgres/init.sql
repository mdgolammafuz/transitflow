-- 1. Identity & Access Management
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'transit') THEN
        CREATE USER transit WITH PASSWORD 'transit';
    END IF;
END $$;

ALTER USER transit WITH LOGIN;

-- 2. Schema Construction
CREATE SCHEMA IF NOT EXISTS metrics;
CREATE SCHEMA IF NOT EXISTS features;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS reference; -- Required for Metadata Bootstrap
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS snapshots;
CREATE SCHEMA IF NOT EXISTS dimensions;

-- 3. Reference Tables (Static Data for Gold Layer)
CREATE TABLE IF NOT EXISTS reference.stops (
    stop_id VARCHAR(50) PRIMARY KEY,
    stop_code VARCHAR(50),
    stop_name VARCHAR(255),
    stop_desc TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    zone_id VARCHAR(50),
    stop_url VARCHAR(255),
    location_type INTEGER,
    parent_station VARCHAR(50)
);

-- 4. Streaming & Analytics Tables
CREATE TABLE IF NOT EXISTS metrics.fleet_metrics (
    id BIGSERIAL PRIMARY KEY, 
    line_id VARCHAR(10) NOT NULL,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL, 
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    active_vehicles INT NOT NULL, 
    avg_delay_seconds FLOAT NOT NULL,
    max_delay_seconds INT NOT NULL, 
    on_time_count INT NOT NULL, 
    delayed_count INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS metrics.anomalies (
    id BIGSERIAL PRIMARY KEY, 
    vehicle_id VARCHAR(50) NOT NULL,
    line_id VARCHAR(10) NOT NULL, 
    anomaly_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL, 
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL, 
    delay_seconds INT,
    details JSONB, 
    detected_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit.reconciliation (
    id BIGSERIAL PRIMARY KEY, 
    check_date DATE NOT NULL,
    line_id VARCHAR(10) NOT NULL, 
    hour_of_day INT NOT NULL,
    stream_avg_delay FLOAT NOT NULL, 
    batch_avg_delay FLOAT NOT NULL,
    difference_pct FLOAT NOT NULL, 
    passed BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 5. Warehouse Tables (Bronze Landing Zone)
-- Strictly Aligned with Spark ENRICHED_SCHEMA
CREATE TABLE IF NOT EXISTS bronze.enriched (
    vehicle_id VARCHAR(50), 
    timestamp VARCHAR(50),           
    event_time_ms BIGINT, 
    latitude DOUBLE PRECISION, 
    longitude DOUBLE PRECISION, 
    speed_ms DOUBLE PRECISION,
    heading INTEGER,                 
    delay_seconds INTEGER, 
    door_status INTEGER,
    line_id VARCHAR(50), 
    direction_id INTEGER,            
    operator_id INTEGER,             
    next_stop_id VARCHAR(50),
    delay_trend DOUBLE PRECISION,    
    speed_trend DOUBLE PRECISION,    
    distance_since_last_m DOUBLE PRECISION, 
    time_since_last_ms BIGINT,
    is_stopped BOOLEAN,
    stopped_duration_ms BIGINT,
    processing_time BIGINT,          
    kafka_partition INTEGER,      
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP WITH TIME ZONE,
    date DATE,
    ingestion_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Strictly Aligned with Spark STOP_EVENTS_SCHEMA
CREATE TABLE IF NOT EXISTS bronze.stop_events (
    vehicle_id VARCHAR(50), 
    stop_id VARCHAR(50),
    line_id VARCHAR(50), 
    direction_id INTEGER,            
    arrival_time BIGINT,
    delay_at_arrival INTEGER, 
    dwell_time_ms BIGINT,            
    door_status INTEGER,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP WITH TIME ZONE,
    date DATE,
    ingestion_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 6. Global Permissions
GRANT CONNECT ON DATABASE transitflow_db TO transit;
GRANT USAGE ON SCHEMA public, metrics, features, audit, bronze, reference, staging, intermediate, marts, snapshots, dimensions TO transit;

DO $$ 
DECLARE
    schema_name TEXT;
    -- Updated to include 'reference'
    schemas TEXT[] := ARRAY['public', 'metrics', 'features', 'audit', 'bronze', 'reference', 'staging', 'intermediate', 'marts', 'snapshots', 'dimensions'];
BEGIN
    FOREACH schema_name IN ARRAY schemas
    LOOP
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I TO transit', schema_name);
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I TO transit', schema_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL PRIVILEGES ON TABLES TO transit', schema_name);
    END LOOP;
END $$;

ALTER USER transit SET search_path TO public, metrics, features, audit, bronze, reference, staging, intermediate, marts, snapshots, dimensions;