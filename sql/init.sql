-- =============================================================================
-- Cold-Chain Medicine Monitoring Database Schema
-- Database: coldchain
-- =============================================================================

CREATE TABLE IF NOT EXISTS sensor_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50),
    shipment_id VARCHAR(50) NOT NULL,
    medicine VARCHAR(100) NOT NULL,
    vehicle_id VARCHAR(50),
    location VARCHAR(100),
    temperature DOUBLE PRECISION NOT NULL,
    humidity DOUBLE PRECISION,
    door_status VARCHAR(20),
    battery_level DOUBLE PRECISION,
    event_time TIMESTAMP NOT NULL,
    condition VARCHAR(20) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS temperature_alerts (
    alert_id SERIAL PRIMARY KEY,
    event_id VARCHAR(50),
    shipment_id VARCHAR(50) NOT NULL,
    medicine VARCHAR(100) NOT NULL,
    vehicle_id VARCHAR(50),
    location VARCHAR(100),
    temperature DOUBLE PRECISION NOT NULL,
    humidity DOUBLE PRECISION,
    door_status VARCHAR(20),
    battery_level DOUBLE PRECISION,
    event_time TIMESTAMP NOT NULL,
    condition VARCHAR(20) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shipment_summary (
    summary_id SERIAL PRIMARY KEY,
    shipment_id VARCHAR(50) NOT NULL,
    medicine VARCHAR(100) NOT NULL,
    reading_count INT NOT NULL,
    avg_temperature DOUBLE PRECISION NOT NULL,
    min_temperature DOUBLE PRECISION NOT NULL,
    max_temperature DOUBLE PRECISION NOT NULL,
    violation_count INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indices for rapid dashboard aggregation and time-series filtering
CREATE INDEX IF NOT EXISTS idx_sensor_events_time ON sensor_events(event_time);
CREATE INDEX IF NOT EXISTS idx_sensor_events_shipment ON sensor_events(shipment_id);
CREATE INDEX IF NOT EXISTS idx_sensor_events_medicine ON sensor_events(medicine);
CREATE INDEX IF NOT EXISTS idx_temperature_alerts_time ON temperature_alerts(event_time);
CREATE INDEX IF NOT EXISTS idx_temperature_alerts_shipment ON temperature_alerts(shipment_id);
