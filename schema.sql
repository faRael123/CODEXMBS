CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(80) NOT NULL UNIQUE,
    email VARCHAR(120) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    role VARCHAR(20) NOT NULL CHECK (role IN ('admin', 'driver', 'conductor')),
    full_name VARCHAR(120) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    login_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_sessions_user FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS buses (
    id SERIAL PRIMARY KEY,
    plate_number VARCHAR(40) NOT NULL UNIQUE,
    capacity INT NOT NULL DEFAULT 30,
    status VARCHAR(20) NOT NULL DEFAULT 'offline' CHECK (status IN ('online', 'offline', 'maintenance')),
    route_color VARCHAR(20) DEFAULT '#1d4ed8',
    notes TEXT NULL
);

CREATE TABLE IF NOT EXISTS routes (
    id SERIAL PRIMARY KEY,
    route_name VARCHAR(120) NOT NULL UNIQUE,
    start_point VARCHAR(120) NOT NULL,
    end_point VARCHAR(120) NOT NULL,
    distance_km DECIMAL(8,2) NOT NULL DEFAULT 0,
    expected_duration_minutes INT NOT NULL DEFAULT 0,
    coords_json JSONB NOT NULL,
    is_published SMALLINT NOT NULL DEFAULT 1,
    minimum_fare DECIMAL(10,2) NOT NULL DEFAULT 15.00,
    discounted_fare DECIMAL(10,2) NOT NULL DEFAULT 12.00,
    display_order INT NOT NULL DEFAULT 0
);

ALTER TABLE routes ADD COLUMN IF NOT EXISTS is_published SMALLINT NOT NULL DEFAULT 1;
ALTER TABLE routes ADD COLUMN IF NOT EXISTS minimum_fare DECIMAL(10,2) NOT NULL DEFAULT 15.00;
ALTER TABLE routes ADD COLUMN IF NOT EXISTS discounted_fare DECIMAL(10,2) NOT NULL DEFAULT 12.00;
ALTER TABLE routes ADD COLUMN IF NOT EXISTS display_order INT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS stops (
    id SERIAL PRIMARY KEY,
    stop_name VARCHAR(150) NOT NULL UNIQUE,
    latitude DECIMAL(10,6) NOT NULL,
    longitude DECIMAL(10,6) NOT NULL,
    landmark VARCHAR(180) NULL,
    is_active SMALLINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS route_stops (
    id SERIAL PRIMARY KEY,
    route_id INT NOT NULL,
    stop_id INT NOT NULL,
    stop_sequence INT NOT NULL,
    minutes_from_start INT NOT NULL DEFAULT 0,
    CONSTRAINT fk_route_stops_route FOREIGN KEY (route_id) REFERENCES routes(id) ON DELETE CASCADE,
    CONSTRAINT fk_route_stops_stop FOREIGN KEY (stop_id) REFERENCES stops(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_route_stop_sequence ON route_stops(route_id, stop_sequence);
CREATE UNIQUE INDEX IF NOT EXISTS uq_route_stop_pair ON route_stops(route_id, stop_id);

CREATE TABLE IF NOT EXISTS trips (
    id SERIAL PRIMARY KEY,
    driver_id INT NULL,
    conductor_id INT NULL,
    bus_id INT NOT NULL,
    route_id INT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'completed')),
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP NULL,
    scheduled_end TIMESTAMP NULL,
    occupancy INT NOT NULL DEFAULT 0,
    peak_occupancy INT NOT NULL DEFAULT 0,
    duration_minutes INT NOT NULL DEFAULT 0,
    average_load DECIMAL(8,2) NOT NULL DEFAULT 0,
    notes TEXT NULL,
    CONSTRAINT fk_trips_driver FOREIGN KEY (driver_id) REFERENCES users(id),
    CONSTRAINT fk_trips_conductor FOREIGN KEY (conductor_id) REFERENCES users(id),
    CONSTRAINT fk_trips_bus FOREIGN KEY (bus_id) REFERENCES buses(id),
    CONSTRAINT fk_trips_route FOREIGN KEY (route_id) REFERENCES routes(id)
);

CREATE TABLE IF NOT EXISTS trip_records (
    id SERIAL PRIMARY KEY,
    trip_id INT NOT NULL,
    students INT NOT NULL DEFAULT 0,
    pwd INT NOT NULL DEFAULT 0,
    senior INT NOT NULL DEFAULT 0,
    regular INT NOT NULL DEFAULT 0,
    boarded INT NOT NULL DEFAULT 0,
    dropped INT NOT NULL DEFAULT 0,
    total INT NOT NULL DEFAULT 0,
    occupancy_after INT NOT NULL DEFAULT 0,
    crowd_level VARCHAR(20) NOT NULL DEFAULT 'Low',
    stop_name VARCHAR(150) NOT NULL DEFAULT 'Unknown',
    latitude DECIMAL(10,6) NULL,
    longitude DECIMAL(10,6) NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_trip_records_trip FOREIGN KEY (trip_id) REFERENCES trips(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS trip_transactions (
    id SERIAL PRIMARY KEY,
    trip_id INT NOT NULL,
    conductor_id INT NULL,
    event_type VARCHAR(20) NOT NULL DEFAULT 'board' CHECK (event_type IN ('board', 'drop')),
    passenger_type VARCHAR(20) NOT NULL DEFAULT 'regular' CHECK (passenger_type IN ('student', 'pwd', 'senior', 'regular', 'mixed')),
    quantity INT NOT NULL DEFAULT 1,
    fare_amount DECIMAL(10,2) NULL,
    stop_name VARCHAR(150) NOT NULL DEFAULT 'Unknown',
    latitude DECIMAL(10,6) NULL,
    longitude DECIMAL(10,6) NULL,
    occupancy_after INT NOT NULL DEFAULT 0,
    notes TEXT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    origin_stop VARCHAR(150) NULL,
    destination_stop VARCHAR(150) NULL,
    CONSTRAINT fk_trip_transactions_trip FOREIGN KEY (trip_id) REFERENCES trips(id) ON DELETE CASCADE,
    CONSTRAINT fk_trip_transactions_conductor FOREIGN KEY (conductor_id) REFERENCES users(id) ON DELETE SET NULL
);

ALTER TABLE trip_transactions ADD COLUMN IF NOT EXISTS origin_stop VARCHAR(150) NULL;
ALTER TABLE trip_transactions ADD COLUMN IF NOT EXISTS destination_stop VARCHAR(150) NULL;

CREATE TABLE IF NOT EXISTS gps_logs (
    id SERIAL PRIMARY KEY,
    trip_id INT NOT NULL,
    latitude DECIMAL(10,6) NOT NULL,
    longitude DECIMAL(10,6) NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_gps_logs_trip FOREIGN KEY (trip_id) REFERENCES trips(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bus_cameras (
    id SERIAL PRIMARY KEY,
    bus_id INT NOT NULL,
    camera_name VARCHAR(120) NOT NULL,
    stream_type VARCHAR(40) NOT NULL DEFAULT 'external' CHECK (stream_type IN ('hls', 'mjpeg', 'embed', 'external', 'webrtc', 'rtsp_gateway')),
    stream_url TEXT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'unconfigured' CHECK (status IN ('online', 'offline', 'maintenance', 'unconfigured')),
    is_active SMALLINT NOT NULL DEFAULT 1,
    last_seen_at TIMESTAMP NULL,
    notes TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_bus_cameras_bus FOREIGN KEY (bus_id) REFERENCES buses(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS system_logs (
    id SERIAL PRIMARY KEY,
    user_id INT NULL,
    role VARCHAR(40) NULL,
    action VARCHAR(120) NOT NULL,
    description TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_system_logs_user FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS service_alerts (
    id SERIAL PRIMARY KEY,
    trip_id INT NULL,
    route_id INT NULL,
    stop_name VARCHAR(150) NULL,
    title VARCHAR(180) NOT NULL,
    message TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL DEFAULT 'info' CHECK (severity IN ('info', 'warning', 'critical')),
    is_active SMALLINT NOT NULL DEFAULT 1,
    created_by INT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NULL,
    archived_at TIMESTAMP NULL,
    CONSTRAINT fk_service_alerts_trip FOREIGN KEY (trip_id) REFERENCES trips(id) ON DELETE SET NULL,
    CONSTRAINT fk_service_alerts_route FOREIGN KEY (route_id) REFERENCES routes(id) ON DELETE SET NULL,
    CONSTRAINT fk_service_alerts_user FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
);

ALTER TABLE service_alerts ADD COLUMN IF NOT EXISTS trip_id INT NULL;
ALTER TABLE service_alerts ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP NULL;
ALTER TABLE service_alerts ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP NULL;

CREATE INDEX IF NOT EXISTS idx_trips_status ON trips(status);
CREATE INDEX IF NOT EXISTS idx_trip_records_trip_id ON trip_records(trip_id);
CREATE INDEX IF NOT EXISTS idx_trip_records_recorded_at ON trip_records(recorded_at);
CREATE INDEX IF NOT EXISTS idx_trip_transactions_trip_id ON trip_transactions(trip_id);
CREATE INDEX IF NOT EXISTS idx_trip_transactions_recorded_at ON trip_transactions(recorded_at);
CREATE INDEX IF NOT EXISTS idx_gps_logs_trip_id ON gps_logs(trip_id);
CREATE INDEX IF NOT EXISTS idx_bus_cameras_bus_active ON bus_cameras(bus_id, is_active);
CREATE INDEX IF NOT EXISTS idx_service_alerts_active ON service_alerts(is_active, created_at);
CREATE INDEX IF NOT EXISTS idx_route_stops_route_sequence ON route_stops(route_id, stop_sequence);
