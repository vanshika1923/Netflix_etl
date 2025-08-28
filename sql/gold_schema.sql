-- --- gold_schema.sql ---
-- DDL for the 'gold' layer, which is a star schema
-- The Gold layer is designed for analytics and reporting.
-- It is idempotent, meaning it can be run multiple times safely.

-- Drop tables first to ensure a clean slate, in reverse dependency order
DROP TABLE IF EXISTS gold.fact_viewing_activity CASCADE;
DROP TABLE IF EXISTS gold.fact_subscription_events CASCADE;
DROP TABLE IF EXISTS gold.dim_user CASCADE;
DROP TABLE IF EXISTS gold.dim_content CASCADE;
DROP TABLE IF EXISTS gold.dim_date CASCADE;

-- Drop the schema itself to ensure a clean slate for the gold layer
DROP SCHEMA IF EXISTS gold CASCADE;

-- Create the gold schema
CREATE SCHEMA gold;

-- 1. Create Dimension Tables

-- dim_date: A time dimension for time-based analysis.
CREATE TABLE gold.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    month_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- dim_user: A dimension for user attributes.
-- It uses the user_legacy_id from the silver layer as its key.
CREATE TABLE gold.dim_user (
    user_key VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    country_name VARCHAR(100),
    region_name VARCHAR(100),
    first_subscription_date DATE,
    current_plan_type VARCHAR(50),
    is_active BOOLEAN
);

-- dim_content: A dimension for content (movies, series, etc.).
CREATE TABLE gold.dim_content (
    content_key VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255),
    content_type_name VARCHAR(50),
    genre_name VARCHAR(50),
    release_year SMALLINT
);

-- 2. Create Fact Tables

-- fact_subscription_events: A fact table to track subscription events.
-- It links to the date and user dimensions.
CREATE TABLE gold.fact_subscription_events (
    date_key INT NOT NULL,
    user_key VARCHAR(255) NOT NULL,
    event_type_name VARCHAR(50) NOT NULL,
    mrr_change NUMERIC(10, 2), -- Monthly Recurring Revenue change
    -- Foreign keys
    FOREIGN KEY (date_key) REFERENCES gold.dim_date (date_key),
    FOREIGN KEY (user_key) REFERENCES gold.dim_user (user_key)
);

-- fact_viewing_activity: A fact table for viewing events.
-- It links to date, user, and content dimensions.
CREATE TABLE gold.fact_viewing_activity (
    date_key INT NOT NULL,
    user_key VARCHAR(255) NOT NULL,
    content_key VARCHAR(255) NOT NULL,
    device_id VARCHAR(255),
    duration_watched_sec INT,
    user_rating SMALLINT,
    -- Foreign keys
    FOREIGN KEY (date_key) REFERENCES gold.dim_date (date_key),
    FOREIGN KEY (user_key) REFERENCES gold.dim_user (user_key),
    FOREIGN KEY (content_key) REFERENCES gold.dim_content (content_key)
);