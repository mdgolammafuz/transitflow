-- Transit SLA - PostgreSQL Schema
-- Basic tables for metrics and features

-- Schema for real-time metrics
CREATE SCHEMA IF NOT EXISTS metrics;

-- Schema for feature store
CREATE SCHEMA IF NOT EXISTS features;

-- Schema for audit/reconciliation
CREATE SCHEMA IF NOT EXISTS audit;

-- Fleet-level metrics (written by Flink)
CREATE TABLE metrics.fleet_metrics (
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

CREATE INDEX idx_fleet_metrics_line_window ON metrics.fleet_metrics(line_id, window_end);

-- Anomaly events (written by Flink)
CREATE TABLE metrics.anomalies (
    id BIGSERIAL PRIMARY KEY,
    vehicle_id INT NOT NULL,
    line_id VARCHAR(10) NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,  -- STUCK, DELAY_SPIKE, ROUTE_DEVIATION
    severity VARCHAR(20) NOT NULL,       -- LOW, MEDIUM, HIGH
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    delay_seconds INT,
    details JSONB,
    detected_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_anomalies_vehicle ON metrics.anomalies(vehicle_id, detected_at);
CREATE INDEX idx_anomalies_type ON metrics.anomalies(anomaly_type, detected_at);

-- Stop historical features (written by Spark daily)
CREATE TABLE features.stop_historical (
    stop_id INT NOT NULL,
    hour_of_day INT NOT NULL,
    day_of_week INT NOT NULL,
    avg_delay_seconds FLOAT NOT NULL,
    stddev_delay_seconds FLOAT NOT NULL,
    avg_dwell_time_seconds FLOAT NOT NULL,
    p90_delay_seconds FLOAT NOT NULL,
    sample_count INT NOT NULL,
    computed_date DATE NOT NULL,
    PRIMARY KEY (stop_id, hour_of_day, day_of_week, computed_date)
);

-- Reconciliation log (stream vs batch comparison)
CREATE TABLE audit.reconciliation (
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

CREATE INDEX idx_reconciliation_date ON audit.reconciliation(check_date, line_id);

-- Grant permissions
GRANT USAGE ON SCHEMA metrics TO transit;
GRANT USAGE ON SCHEMA features TO transit;
GRANT USAGE ON SCHEMA audit TO transit;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metrics TO transit;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA features TO transit;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA audit TO transit;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metrics TO transit;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA audit TO transit;
