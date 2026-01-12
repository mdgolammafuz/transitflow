-- 1. Identity & Access Management
ALTER USER transit WITH LOGIN;

-- 2. Schema Construction
CREATE SCHEMA IF NOT EXISTS metrics;
CREATE SCHEMA IF NOT EXISTS features;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS snapshots;
CREATE SCHEMA IF NOT EXISTS dimensions;

-- 3. Streaming Tables
CREATE TABLE IF NOT EXISTS metrics.fleet_metrics (
    id BIGSERIAL PRIMARY KEY, line_id VARCHAR(10) NOT NULL,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL, window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    active_vehicles INT NOT NULL, avg_delay_seconds FLOAT NOT NULL,
    max_delay_seconds INT NOT NULL, on_time_count INT NOT NULL, delayed_count INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS metrics.anomalies (
    id BIGSERIAL PRIMARY KEY, vehicle_id VARCHAR(50) NOT NULL,
    line_id VARCHAR(10) NOT NULL, anomaly_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL, latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL, delay_seconds INT,
    details JSONB, detected_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit.reconciliation (
    id BIGSERIAL PRIMARY KEY, check_date DATE NOT NULL,
    line_id VARCHAR(10) NOT NULL, hour_of_day INT NOT NULL,
    stream_avg_delay FLOAT NOT NULL, batch_avg_delay FLOAT NOT NULL,
    difference_pct FLOAT NOT NULL, passed BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. Warehouse Tables (Bronze Landing Zone)
CREATE TABLE IF NOT EXISTS bronze.enriched (
    vehicle_id VARCHAR(50), 
    operator_id VARCHAR(50),
    line_id VARCHAR(50), 
    direction_id VARCHAR(10),
    event_time_ms BIGINT, 
    event_timestamp TIMESTAMP WITH TIME ZONE,
    event_date DATE,
    latitude DOUBLE PRECISION, 
    longitude DOUBLE PRECISION, 
    speed_kmh DOUBLE PRECISION,
    speed_ms DOUBLE PRECISION,
    heading DOUBLE PRECISION,
    door_status INTEGER,
    delay_seconds INTEGER, 
    delay_trend VARCHAR(20),
    speed_trend VARCHAR(20),
    next_stop_id VARCHAR(50),
    stop_sequence INTEGER,
    is_stopped BOOLEAN,
    stopped_duration_ms BIGINT,
    distance_since_last_ms FLOAT,
    time_since_last_ms BIGINT,
    kafka_partition INTEGER,      
    kafka_offset BIGINT,
    partition_date DATE,
    date DATE,
    processing_time TIMESTAMP WITH TIME ZONE,
    ingestion_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.stop_events (
    vehicle_id VARCHAR(50), 
    line_id VARCHAR(50), 
    stop_id VARCHAR(50), 
    stop_code VARCHAR(50),        -- Added for dim_stops
    stop_name VARCHAR(255),
    zone_id VARCHAR(50),          
    operator_id VARCHAR(50),
    direction_id VARCHAR(10),
    arrival_time BIGINT,
    arrival_timestamp TIMESTAMP WITH TIME ZONE,
    arrival_date DATE,
    delay_at_arrival INTEGER, 
    dwell_time_ms INTEGER,
    distance_since_last_ms FLOAT,
    stop_sequence INTEGER,
    door_opened BOOLEAN,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    date DATE,
    ingestion_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. Global Permissions
GRANT CONNECT ON DATABASE transit TO transit;
GRANT USAGE ON SCHEMA public, metrics, features, audit, bronze, staging, intermediate, marts, snapshots, dimensions TO transit;

DO $$ 
DECLARE
    schema_name TEXT;
    schemas TEXT[] := ARRAY['public', 'metrics', 'features', 'audit', 'bronze', 'staging', 'intermediate', 'marts', 'snapshots', 'dimensions'];
BEGIN
    FOREACH schema_name IN ARRAY schemas
    LOOP
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I TO transit', schema_name);
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I TO transit', schema_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL PRIVILEGES ON TABLES TO transit', schema_name);
    END LOOP;
END $$;

ALTER USER transit SET search_path TO public, metrics, features, audit, bronze, staging, intermediate, marts, snapshots, dimensions;
