-- --- build_silver_layer.sql ---
-- This script populates the Silver layer from Bronze using lookup tables.
-- It is idempotent and can be run safely multiple times.

-- Truncate all tables to ensure a clean run
TRUNCATE TABLE
silver.viewing_activity, silver.payments, silver.subscriptions,
silver.devices, silver.watchlist, silver.users, silver.locations,
audit.rejected_rows, silver.dim_genres, silver.dim_content_types,
silver.dim_device_types, silver.dim_os, silver.dim_plan_types,
silver.dim_event_types
RESTART IDENTITY CASCADE;

---
-- Step 1: Populate Dimension (Lookup) Tables from Bronze Data
---
INSERT INTO silver.dim_genres (genre_name)
SELECT DISTINCT SPLIT_PART(TRIM(genre), ',', 1) FROM bronze.watchlist WHERE genre IS NOT NULL;
INSERT INTO silver.dim_content_types (content_type_name)
SELECT DISTINCT INITCAP(TRIM(content_type)) FROM bronze.watchlist WHERE content_type IS NOT NULL;
INSERT INTO silver.dim_device_types (device_type_name)
SELECT DISTINCT (CASE WHEN UPPER(TRIM(device_type)) LIKE 'MOBILE%%' THEN 'Mobile' WHEN UPPER(TRIM(device_type)) LIKE 'SMARTTV%%' THEN 'SmartTV' WHEN UPPER(TRIM(device_type)) LIKE 'TABLET%%' THEN 'Tablet' WHEN UPPER(TRIM(device_type)) LIKE 'WEB%%' THEN 'Web' ELSE NULL END) FROM bronze.devices WHERE device_type IS NOT NULL;
INSERT INTO silver.dim_os (os_name)
SELECT DISTINCT (CASE WHEN UPPER(TRIM(operating_system)) LIKE 'ANDROID%%' THEN 'Android' WHEN UPPER(TRIM(operating_system)) LIKE 'IOS%%' THEN 'iOS' WHEN UPPER(TRIM(operating_system)) LIKE 'WEBOS%%' THEN 'WebOS' WHEN UPPER(TRIM(operating_system)) LIKE 'TIZEN%%' THEN 'Tizen' WHEN UPPER(TRIM(operating_system)) LIKE 'CHROME%%' THEN 'Chrome' ELSE NULL END) FROM bronze.devices WHERE operating_system IS NOT NULL;
INSERT INTO silver.dim_plan_types (plan_type_name)
SELECT DISTINCT INITCAP(TRIM(plan_type)) FROM bronze.subscriptions WHERE plan_type IS NOT NULL;
INSERT INTO silver.dim_event_types (event_type_name)
SELECT DISTINCT (CASE WHEN UPPER(TRIM(last_event_type)) = 'SUBSCRIBE' THEN 'SIGNUP' ELSE UPPER(TRIM(last_event_type)) END) FROM bronze.subscriptions WHERE last_event_type IS NOT NULL;

---
-- Step 2: Process and load main Silver tables
---
-- Load: LOCATIONS
INSERT INTO silver.locations (country_id, country_code, country_name, region_id, region_code, region_name)
SELECT DISTINCT ON (country_id) country_id, TRIM(country_code), TRIM(country_name), region_id, TRIM(region_code), TRIM(region_name)
FROM bronze.locations WHERE country_id IS NOT NULL;

-- Load: USERS
-- FIX: Added robust date parsing for signup_date
INSERT INTO silver.users (user_id, user_legacy_id, name, email, age, country_id, region_id, region_code, region_name, signup_date, acquisition_source)
SELECT DISTINCT ON (b.user_legacy_id)
    b.user_id,
    b.user_legacy_id,
    INITCAP(TRIM(b.name)),
    b.email,
    b.age,
    b.country_id,
    b.region_id,
    b.region_code,
    b.region_name,
    CASE
        WHEN b.signup_date ~ '^\d{4}-\d{2}-\d{2}' THEN b.signup_date::DATE
        WHEN b.signup_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(b.signup_date, 'MM/DD/YYYY')
        ELSE NULL
    END AS signup_date,
    b.acquisition_source
FROM
    bronze.users b
JOIN silver.locations sl ON b.country_id = sl.country_id
WHERE b.name IS NOT NULL AND TRIM(b.name) != '';

-- Load: WATCHLIST
INSERT INTO silver.watchlist (content_id, title, content_type_id, genre_id, release_year, seasons, episodes, content_duration_minutes)
SELECT DISTINCT ON (w.content_id) w.content_id, w.title, ct.content_type_id, g.genre_id, w.release_year, w.seasons, w.episodes, w.content_duration_minutes
FROM bronze.watchlist w
LEFT JOIN silver.dim_content_types ct ON INITCAP(TRIM(w.content_type)) = ct.content_type_name
LEFT JOIN silver.dim_genres g ON SPLIT_PART(TRIM(w.genre), ',', 1) = g.genre_name;

-- Load: DEVICES
-- FIX: Added robust timestamp parsing for last_used
INSERT INTO silver.devices (device_id, user_legacy_id, device_type_id, os_id, last_used)
SELECT DISTINCT ON (d.device_id)
    d.device_id,
    d.user_legacy_id,
    dt.device_type_id,
    os.os_id,
    CASE
        WHEN d.last_used ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}Z$' THEN d.last_used::TIMESTAMP
        ELSE NULL
    END AS last_used
FROM bronze.devices d
JOIN silver.users u ON d.user_legacy_id = u.user_legacy_id
LEFT JOIN silver.dim_device_types dt ON (CASE WHEN UPPER(TRIM(d.device_type)) LIKE 'MOBILE%%' THEN 'Mobile' WHEN UPPER(TRIM(d.device_type)) LIKE 'SMARTTV%%' THEN 'SmartTV' WHEN UPPER(TRIM(d.device_type)) LIKE 'TABLET%%' THEN 'Tablet' WHEN UPPER(TRIM(d.device_type)) LIKE 'WEB%%' THEN 'Web' ELSE NULL END) = dt.device_type_name
LEFT JOIN silver.dim_os os ON (CASE WHEN UPPER(TRIM(d.operating_system)) LIKE 'ANDROID%%' THEN 'Android' WHEN UPPER(TRIM(d.operating_system)) LIKE 'IOS%%' THEN 'iOS' WHEN UPPER(TRIM(d.operating_system)) LIKE 'WEBOS%%' THEN 'WebOS' WHEN UPPER(TRIM(d.operating_system)) LIKE 'TIZEN%%' THEN 'Tizen' WHEN UPPER(TRIM(d.operating_system)) LIKE 'CHROME%%' THEN 'Chrome' ELSE NULL END) = os.os_name;

-- Load: SUBSCRIPTIONS
-- FIX: Added robust date and timestamp parsing
INSERT INTO silver.subscriptions (sub_id, user_legacy_id, plan_type_id, start_date, end_date, last_event_type_id, last_event_timestamp)
SELECT DISTINCT ON (s.sub_id)
    s.sub_id,
    s.user_legacy_id,
    pt.plan_type_id,
    CASE
        WHEN s.start_date ~ '^\d{4}-\d{2}-\d{2}$' THEN s.start_date::DATE
        ELSE NULL
    END AS start_date,
    CASE
        WHEN s.end_date ~ '^\d{4}-\d{2}-\d{2}$' THEN s.end_date::DATE
        ELSE NULL
    END AS end_date,
    et.event_type_id,
    CASE
        WHEN s.last_event_timestamp ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}Z$' THEN s.last_event_timestamp::TIMESTAMP
        ELSE NULL
    END AS last_event_timestamp
FROM bronze.subscriptions s
JOIN silver.users u ON s.user_legacy_id = u.user_legacy_id
LEFT JOIN silver.dim_plan_types pt ON INITCAP(TRIM(s.plan_type)) = pt.plan_type_name
LEFT JOIN silver.dim_event_types et ON (CASE WHEN UPPER(TRIM(s.last_event_type)) = 'SUBSCRIBE' THEN 'SIGNUP' ELSE UPPER(TRIM(s.last_event_type)) END) = et.event_type_name
WHERE (s.end_date IS NULL OR s.end_date::DATE >= s.start_date::DATE);

-- Load: PAYMENTS
-- FIX: Added robust timestamp parsing for payment_date
INSERT INTO silver.payments (payment_id, user_legacy_id, sub_id, amount, payment_date, method)
SELECT DISTINCT ON (p.payment_id)
    p.payment_id,
    p.user_legacy_id,
    p.sub_id,
    p.amount,
    CASE
        WHEN p.payment_date ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}Z$' THEN p.payment_date::TIMESTAMP
        ELSE NULL
    END AS payment_date,
    p.method
FROM bronze.payments p
JOIN silver.subscriptions s ON p.sub_id = s.sub_id
WHERE p.amount >= 0;

-- Load: VIEWING ACTIVITY
-- FIX: Added robust timestamp parsing for session_start_ts
INSERT INTO silver.viewing_activity (session_id, user_legacy_id, content_id, device_id, session_start_ts, duration_watched_sec, completion_percentage, rating)
SELECT DISTINCT ON (v.session_id)
    v.session_id,
    v.user_legacy_id,
    v.content_id,
    v.device_id,
    CASE
        WHEN v.session_start_ts ~ '^\d{4}-\d{2}-\d{2}' THEN v.session_start_ts::TIMESTAMP
        WHEN v.session_start_ts ~ '^\d{1,2}/\d{1,2}/\d{4}' THEN to_timestamp(v.session_start_ts, 'MM/DD/YYYY')
        ELSE NULL
    END AS session_start_ts,
    v.duration_watched_sec,
    v.completion_percentage,
    v.rating
FROM bronze.viewing_activity v
JOIN silver.users u ON v.user_legacy_id = u.user_legacy_id
JOIN silver.watchlist w ON v.content_id = w.content_id
JOIN silver.devices d ON v.device_id = d.device_id
WHERE (v.rating IS NULL OR (v.rating >= 1 AND v.rating <= 5));