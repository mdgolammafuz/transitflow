-- =============================================================================
-- BERLIN AIR QUALITY - POSTGRESQL SCHEMA
-- =============================================================================
-- Bronze Layer: Immutable raw data
-- 
-- Principle : Raw Data is Immutable Truth
-- - INSERT only (no UPDATE/DELETE)
-- - Partitioned by time
-- - Tracks processing engine (flink vs spark)
-- =============================================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- =============================================================================
-- BRONZE LAYER
-- =============================================================================

-- Raw PM2.5 measurements (individual station readings)
CREATE TABLE IF NOT EXISTS bronze.raw_pm25 (
    id BIGSERIAL,
    station_id VARCHAR(100) NOT NULL,
    station_name VARCHAR(200) NOT NULL,
    measurement_timestamp TIMESTAMPTZ NOT NULL,
    value_ugm3 DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    is_valid BOOLEAN DEFAULT TRUE,
    
    -- Kafka metadata (for replay)
    kafka_topic VARCHAR(100),
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    
    -- Processing metadata
    processing_engine VARCHAR(20) NOT NULL,  -- 'flink' or 'spark'
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (id, measurement_timestamp)
) PARTITION BY RANGE (measurement_timestamp);

-- Create monthly partitions (example for 2024)
CREATE TABLE IF NOT EXISTS bronze.raw_pm25_2024_01 
    PARTITION OF bronze.raw_pm25 
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE IF NOT EXISTS bronze.raw_pm25_2024_12 
    PARTITION OF bronze.raw_pm25 
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS bronze.raw_pm25_2025_01 
    PARTITION OF bronze.raw_pm25 
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- Create default partition for out-of-range data
CREATE TABLE IF NOT EXISTS bronze.raw_pm25_default 
    PARTITION OF bronze.raw_pm25 DEFAULT;


-- Hourly PM2.5 aggregates (city-level)
-- Written by both Flink (real-time) and Spark (batch)
CREATE TABLE IF NOT EXISTS bronze.hourly_pm25 (
    window_start TIMESTAMPTZ NOT NULL PRIMARY KEY,
    window_end TIMESTAMPTZ NOT NULL,
    pm25_mean DOUBLE PRECISION NOT NULL,
    pm25_max DOUBLE PRECISION NOT NULL,
    pm25_min DOUBLE PRECISION NOT NULL,
    stations_reporting INTEGER NOT NULL,
    readings_count INTEGER NOT NULL,
    processing_engine VARCHAR(20) NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_hourly_pm25_window 
    ON bronze.hourly_pm25 (window_start DESC);


-- Raw weather observations
CREATE TABLE IF NOT EXISTS bronze.raw_weather (
    id BIGSERIAL,
    observation_timestamp TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL,
    wind_speed_ms DOUBLE PRECISION NOT NULL,
    wind_direction_deg INTEGER NOT NULL,
    precipitation_mm DOUBLE PRECISION NOT NULL,
    humidity_pct DOUBLE PRECISION NOT NULL,
    pressure_hpa DOUBLE PRECISION NOT NULL,
    cloud_cover_pct DOUBLE PRECISION NOT NULL,
    is_forecast BOOLEAN DEFAULT FALSE,
    forecast_made_at TIMESTAMPTZ,
    
    kafka_topic VARCHAR(100),
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    processing_engine VARCHAR(20) NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (id, observation_timestamp)
) PARTITION BY RANGE (observation_timestamp);

-- Default partition for weather
CREATE TABLE IF NOT EXISTS bronze.raw_weather_default 
    PARTITION OF bronze.raw_weather DEFAULT;


-- =============================================================================
-- QUARANTINE (Failed records)
-- =============================================================================

CREATE TABLE IF NOT EXISTS quarantine.failed_records (
    id BIGSERIAL PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    error_type VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    failed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ,
    resolved_by VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_quarantine_unresolved 
    ON quarantine.failed_records (failed_at) 
    WHERE resolved_at IS NULL;


-- =============================================================================
-- RECONCILIATION (Flink vs Spark comparison)
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.reconciliation_log (
    id BIGSERIAL PRIMARY KEY,
    check_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    
    flink_pm25_mean DOUBLE PRECISION,
    spark_pm25_mean DOUBLE PRECISION,
    mean_diff DOUBLE PRECISION,
    
    flink_count INTEGER,
    spark_count INTEGER,
    count_diff_pct DOUBLE PRECISION,
    
    status VARCHAR(20) NOT NULL,  -- 'PASSED', 'FAILED', 'SKIPPED'
    notes TEXT
);


-- =============================================================================
-- PERMISSIONS (Principle #2: Immutability)
-- =============================================================================

-- Create ingestion role (INSERT only)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ingestion_role') THEN
        CREATE ROLE ingestion_role;
    END IF;
END
$$;

-- Grant INSERT only on Bronze tables
GRANT USAGE ON SCHEMA bronze TO ingestion_role;
GRANT INSERT ON ALL TABLES IN SCHEMA bronze TO ingestion_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO ingestion_role;

-- Explicitly REVOKE UPDATE/DELETE (defense in depth)
REVOKE UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze FROM ingestion_role;

-- Create readonly role for analytics
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'readonly_role') THEN
        CREATE ROLE readonly_role;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA bronze, silver, gold TO readonly_role;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze, silver, gold TO readonly_role;


-- =============================================================================
-- VIEWS (Convenience)
-- =============================================================================

-- Latest hourly readings
CREATE OR REPLACE VIEW bronze.v_latest_hourly AS
SELECT *
FROM bronze.hourly_pm25
ORDER BY window_start DESC
LIMIT 24;

-- Readings by processing engine
CREATE OR REPLACE VIEW bronze.v_readings_by_engine AS
SELECT 
    processing_engine,
    COUNT(*) as record_count,
    MIN(measurement_timestamp) as earliest,
    MAX(measurement_timestamp) as latest
FROM bronze.raw_pm25
GROUP BY processing_engine;


-- =============================================================================
-- DONE
-- =============================================================================

-- Verify tables created
SELECT schemaname, tablename 
FROM pg_tables 
WHERE schemaname IN ('bronze', 'silver', 'gold', 'quarantine')
ORDER BY schemaname, tablename;
