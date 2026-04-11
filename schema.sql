CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(80) NOT NULL UNIQUE,
    email VARCHAR(120) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    role ENUM('admin', 'driver', 'conductor') NOT NULL,
    full_name VARCHAR(120) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    login_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_sessions_user FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS buses (
    id INT AUTO_INCREMENT PRIMARY KEY,
    plate_number VARCHAR(40) NOT NULL UNIQUE,
    capacity INT NOT NULL DEFAULT 30,
    status ENUM('online', 'offline', 'maintenance') NOT NULL DEFAULT 'offline',
    route_color VARCHAR(20) DEFAULT '#1d4ed8',
    notes TEXT NULL
);

CREATE TABLE IF NOT EXISTS routes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    route_name VARCHAR(120) NOT NULL UNIQUE,
    start_point VARCHAR(120) NOT NULL,
    end_point VARCHAR(120) NOT NULL,
    distance_km DECIMAL(8,2) NOT NULL DEFAULT 0,
    expected_duration_minutes INT NOT NULL DEFAULT 0,
    coords_json JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS trips (
    id INT AUTO_INCREMENT PRIMARY KEY,
    driver_id INT NULL,
    conductor_id INT NULL,
    bus_id INT NOT NULL,
    route_id INT NOT NULL,
    status ENUM('active', 'completed') NOT NULL DEFAULT 'active',
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at DATETIME NULL,
    scheduled_end DATETIME NULL,
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
    id INT AUTO_INCREMENT PRIMARY KEY,
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
    recorded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_trip_records_trip FOREIGN KEY (trip_id) REFERENCES trips(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS trip_transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    trip_id INT NOT NULL,
    conductor_id INT NULL,
    event_type ENUM('board', 'drop') NOT NULL DEFAULT 'board',
    passenger_type ENUM('student', 'pwd', 'senior', 'regular', 'mixed') NOT NULL DEFAULT 'regular',
    quantity INT NOT NULL DEFAULT 1,
    fare_amount DECIMAL(10,2) NULL,
    stop_name VARCHAR(150) NOT NULL DEFAULT 'Unknown',
    latitude DECIMAL(10,6) NULL,
    longitude DECIMAL(10,6) NULL,
    occupancy_after INT NOT NULL DEFAULT 0,
    notes TEXT NULL,
    recorded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_trip_transactions_trip FOREIGN KEY (trip_id) REFERENCES trips(id) ON DELETE CASCADE,
    CONSTRAINT fk_trip_transactions_conductor FOREIGN KEY (conductor_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS gps_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    trip_id INT NOT NULL,
    latitude DECIMAL(10,6) NOT NULL,
    longitude DECIMAL(10,6) NOT NULL,
    recorded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_gps_logs_trip FOREIGN KEY (trip_id) REFERENCES trips(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS system_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NULL,
    role VARCHAR(40) NULL,
    action VARCHAR(120) NOT NULL,
    description TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_system_logs_user FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_trips_status ON trips(status);
CREATE INDEX idx_trip_records_trip_id ON trip_records(trip_id);
CREATE INDEX idx_trip_records_recorded_at ON trip_records(recorded_at);
CREATE INDEX idx_trip_transactions_trip_id ON trip_transactions(trip_id);
CREATE INDEX idx_trip_transactions_recorded_at ON trip_transactions(recorded_at);
CREATE INDEX idx_gps_logs_trip_id ON gps_logs(trip_id);
