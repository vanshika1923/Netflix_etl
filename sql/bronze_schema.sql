-- bronze_schema.sql
-- Raw landing tables that match the Apps Script sheets 1:1
-- Intentional design: all columns as TEXT, no PKs/FKs/constraints.
CREATE SCHEMA IF NOT EXISTS bronze;
-- Drop in reverse dependency order (not strictly necessary in bronze, but tidy)
DROP TABLE IF EXISTS bronze.viewing_activity;
DROP TABLE IF EXISTS bronze.payments;
DROP TABLE IF EXISTS bronze.subscriptions;
DROP TABLE IF EXISTS bronze.devices;
DROP TABLE IF EXISTS bronze.watchlist;
DROP TABLE IF EXISTS bronze.users;
DROP TABLE IF EXISTS bronze.locations;
-- 1) locations
-- Sheet headers: country_id, country_code, country_name, region_id, region_code, region_name
CREATE TABLE bronze.locations (
  country_id   TEXT,
  country_code TEXT,
  country_name TEXT,
  region_id    TEXT,
  region_code  TEXT,
  region_name  TEXT
);
-- 2) users
-- Sheet headers: user_id, user_legacy_id, name, email, age, country_id, region_id,
--                region_code, region_name, signup_date, acquisition_source
CREATE TABLE bronze.users (
  user_id            TEXT,
  user_legacy_id     TEXT,
  name               TEXT,
  email              TEXT,
  age                TEXT,
  country_id         TEXT,
  region_id          TEXT,
  region_code        TEXT,
  region_name        TEXT,
  signup_date        TEXT,  -- Apps Script formats YYYY-MM-DD; keep as TEXT in bronze
  acquisition_source TEXT
);
-- 3) watchlist
-- Sheet headers: content_id, title, content_type, genre, release_year, seasons, episodes, content_duration_minutes
CREATE TABLE bronze.watchlist (
  content_id               TEXT,
  title                    TEXT,
  content_type             TEXT,
  genre                    TEXT,
  release_year             TEXT,
  seasons                  TEXT,
  episodes                 TEXT,
  content_duration_minutes TEXT
);
-- 4) devices
-- Sheet headers: device_id, user_legacy_id, device_type, operating_system, last_used
CREATE TABLE bronze.devices (
  device_id         TEXT,
  user_legacy_id    TEXT,
  device_type       TEXT,
  operating_system  TEXT,
  last_used         TEXT   -- Apps Script writes YYYY-MM-DD; keep as TEXT in bronze
);
-- 5) subscriptions
-- Sheet headers: sub_id, user_legacy_id, plan_type, start_date, end_date, last_event_type, last_event_timestamp
CREATE TABLE bronze.subscriptions (
  sub_id               TEXT,
  user_legacy_id       TEXT,
  plan_type            TEXT,
  start_date           TEXT,
  end_date             TEXT,
  last_event_type      TEXT,
  last_event_timestamp TEXT  -- Timestamp-like; keep as TEXT in bronze
);
-- 6) payments
-- Sheet headers: payment_id, user_legacy_id, sub_id, amount, payment_date, method
CREATE TABLE bronze.payments (
  payment_id   TEXT,
  user_legacy_id TEXT,
  sub_id       TEXT,
  amount       TEXT,  -- keep raw; can include negatives / decimals as text
  payment_date TEXT,
  method       TEXT
);
-- 7) viewing_activity
-- Sheet headers: session_id, user_legacy_id, content_id, device_id, session_start_ts,
--                duration_watched_sec, completion_percentage, rating
CREATE TABLE bronze.viewing_activity (
  session_id            TEXT,
  user_legacy_id        TEXT,
  content_id            TEXT,
  device_id             TEXT,
  session_start_ts      TEXT,  -- timestamp-like; keep as TEXT in bronze
  duration_watched_sec  TEXT,
  completion_percentage TEXT,
  rating                TEXT
);