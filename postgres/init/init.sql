CREATE TABLE IF NOT EXISTS mobile_network_events (
    id SERIAL PRIMARY KEY,
    user_id BYTEA NOT NULL,
    cell_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS mobile_network_cells (
    cell_id VARCHAR(255) PRIMARY KEY,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    date DATE NOT NULL
);

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS local_admin_units (
    lau_id VARCHAR(50) PRIMARY KEY,
    country_code VARCHAR(2) NOT NULL,
    lau_name VARCHAR(255) NOT NULL,
    geometry GEOMETRY(MULTIPOLYGON, 3035) NULL 
);

CREATE INDEX IF NOT EXISTS idx_events_timestamp ON mobile_network_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_date ON mobile_network_events(date);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON mobile_network_events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_cell_id ON mobile_network_events(cell_id);
CREATE INDEX IF NOT EXISTS idx_cells_date ON mobile_network_cells(date);


CREATE OR REPLACE VIEW events_per_hour AS
SELECT 
    date_trunc('hour', timestamp) as hour,
    COUNT(*) as event_count
FROM mobile_network_events
GROUP BY date_trunc('hour', timestamp)
ORDER BY hour;

CREATE OR REPLACE VIEW users_per_hour AS
SELECT 
    date_trunc('hour', timestamp) as hour,
    COUNT(DISTINCT user_id) as user_count
FROM mobile_network_events
GROUP BY date_trunc('hour', timestamp)
ORDER BY hour;

CREATE OR REPLACE VIEW locations_per_user AS
SELECT 
    date,
    user_id,
    COUNT(DISTINCT cell_id) as location_count
FROM mobile_network_events
GROUP BY date, user_id
ORDER BY date, location_count DESC;

CREATE OR REPLACE VIEW unique_users_per_admin_unit AS
SELECT 
    lau.lau_id,
    lau.lau_name,
    mne.date,
    COUNT(DISTINCT mne.user_id) as unique_user_count
FROM mobile_network_events mne
JOIN mobile_network_cells mnc ON mne.cell_id = mnc.cell_id
JOIN local_admin_units lau 
    ON ST_Contains(lau.geometry, ST_Transform(ST_SetSRID(ST_Point(mnc.longitude, mnc.latitude), 4326), 3035)::geometry)
GROUP BY lau.lau_id, lau.lau_name, mne.date
ORDER BY lau.lau_id, mne.date;