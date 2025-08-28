-- Create schemas if they don't already exist
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS audit;
-- Drop tables in reverse order of dependency to ensure a clean slate
DROP TABLE IF EXISTS silver.viewing_activity CASCADE;
DROP TABLE IF EXISTS silver.payments CASCADE;
DROP TABLE IF EXISTS silver.subscriptions CASCADE;
DROP TABLE IF EXISTS silver.devices CASCADE;
DROP TABLE IF EXISTS silver.watchlist CASCADE;
DROP TABLE IF EXISTS silver.users CASCADE;
DROP TABLE IF EXISTS silver.locations CASCADE;
DROP TABLE IF EXISTS audit.rejected_rows CASCADE;
-- Drop lookup tables

DROP TABLE IF EXISTS silver.dim_genres CASCADE;
DROP TABLE IF EXISTS silver.dim_content_types CASCADE;
DROP TABLE IF EXISTS silver.dim_device_types CASCADE;
DROP TABLE IF EXISTS silver.dim_os CASCADE;
DROP TABLE IF EXISTS silver.dim_plan_types CASCADE;
DROP TABLE IF EXISTS silver.dim_event_types CASCADE;
-- Create the audit table
CREATE TABLE audit.rejected_rows (
rejection_id SERIAL PRIMARY KEY,
source_table TEXT,
target_table TEXT,
rejection_reason TEXT,
row_data JSONB,
rejected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
-- ===================================
-- CREATE LOOKUP (DIMENSION) TABLES
-- ===================================
CREATE TABLE silver.dim_genres ( genre_id SERIAL PRIMARY KEY, genre_name TEXT UNIQUE
NOT NULL );
CREATE TABLE silver.dim_content_types ( content_type_id SERIAL PRIMARY KEY,
content_type_name TEXT UNIQUE NOT NULL );
CREATE TABLE silver.dim_device_types ( device_type_id SERIAL PRIMARY KEY,
device_type_name TEXT UNIQUE NOT NULL );
CREATE TABLE silver.dim_os ( os_id SERIAL PRIMARY KEY, os_name TEXT UNIQUE NOT NULL
);
CREATE TABLE silver.dim_plan_types ( plan_type_id SERIAL PRIMARY KEY, plan_type_name
TEXT UNIQUE NOT NULL );
CREATE TABLE silver.dim_event_types ( event_type_id SERIAL PRIMARY KEY,
event_type_name TEXT UNIQUE NOT NULL );
-- ===================================
-- CREATE MAIN SILVER TABLES
-- ===================================
CREATE TABLE silver.locations (
country_id INTEGER PRIMARY KEY,
country_code VARCHAR(10),
country_name VARCHAR(255),
region_id INTEGER,
region_code VARCHAR(10),
region_name VARCHAR(255)
);
CREATE TABLE silver.users (
user_id INTEGER PRIMARY KEY,
user_legacy_id VARCHAR(255) UNIQUE NOT NULL,
name VARCHAR(255),
email VARCHAR(255),
age INTEGER,
country_id INTEGER REFERENCES silver.locations(country_id),
region_id INTEGER,
region_code VARCHAR(10),
region_name VARCHAR(255),
signup_date DATE,
acquisition_source VARCHAR(100)
);
CREATE TABLE silver.watchlist (
content_id INTEGER PRIMARY KEY,
title TEXT,
content_type_id INTEGER REFERENCES silver.dim_content_types(content_type_id),
genre_id INTEGER REFERENCES silver.dim_genres(genre_id),
release_year INTEGER,
seasons INTEGER,
episodes INTEGER,
content_duration_minutes INTEGER
);

CREATE TABLE silver.devices (
device_id INTEGER PRIMARY KEY,
user_legacy_id VARCHAR(255) REFERENCES silver.users(user_legacy_id),
device_type_id INTEGER REFERENCES silver.dim_device_types(device_type_id),
os_id INTEGER REFERENCES silver.dim_os(os_id),
last_used TIMESTAMP WITH TIME ZONE
);
CREATE TABLE silver.subscriptions (
sub_id INTEGER PRIMARY KEY,
user_legacy_id VARCHAR(255) REFERENCES silver.users(user_legacy_id),
plan_type_id INTEGER REFERENCES silver.dim_plan_types(plan_type_id),
start_date DATE,
end_date DATE,
last_event_type_id INTEGER REFERENCES silver.dim_event_types(event_type_id),
last_event_timestamp TIMESTAMP WITH TIME ZONE
);
CREATE TABLE silver.payments (
payment_id INTEGER PRIMARY KEY,
user_legacy_id VARCHAR(255),
sub_id INTEGER REFERENCES silver.subscriptions(sub_id),
amount NUMERIC(10, 2),
payment_date TIMESTAMP WITH TIME ZONE,
method VARCHAR(50)
);
CREATE TABLE silver.viewing_activity (
session_id INTEGER PRIMARY KEY,
user_legacy_id VARCHAR(255) REFERENCES silver.users(user_legacy_id),
content_id INTEGER REFERENCES silver.watchlist(content_id),
device_id INTEGER REFERENCES silver.devices(device_id),
session_start_ts TIMESTAMP WITH TIME ZONE,
duration_watched_sec INTEGER,
completion_percentage NUMERIC(5, 2),
rating INTEGER
);