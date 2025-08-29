# Music Events & Ticketing — Medallion Pipeline (Bronze → Silver → Gold → BI)

Batch ETL for a BookMyShow/Bandsintown‑style domain. Ingest 8 Google Sheets (simulated with CSVs) into **Bronze**, clean to **Silver**, and aggregate to **Gold** for analytics such as **sell‑through %, no‑show %, and ROI by campaign**.

## Quick Start
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# CSVs already included in ./bronze_inputs (synthetic). Replace with real exports any time.
python etl.py all
```
DB defaults to SQLite `warehouse.sqlite`. Set `DATABASE_URL` in `.env` for Postgres.

## Commands
```bash
python etl.py load_bronze
python etl.py build_silver
python etl.py build_gold
python etl.py all
python etl.py schedule   # hourly demo scheduler
```

## Sheets → Tables
- artists → `bronze_artists`
- venues → `bronze_venues`
- events → `bronze_events`
- customers → `bronze_customers`
- tickets → `bronze_tickets`
- checkins → `bronze_checkins`
- refunds → `bronze_refunds`
- campaigns → `bronze_campaigns`

## Gold Outputs (KPIs)
- `gold_event_sales`: tickets_sold, attendance, **no_show_rate**, **sell_through %**, gross/net revenue
- `gold_daily_metrics`: daily tickets & revenue
- `gold_campaign_roi`: spend, conversions, CPA, ROI by event & channel


### 2. Data Dictionary

```markdown


## Bronze Schema
The raw, unprocessed data ingested directly from Google Sheets.
* **locations**: Raw location data.
    * `country_id` (INT)
    * `country_code` (VARCHAR)
    * `country_name` (VARCHAR)
    * `region_id` (INT)
    * `region_code` (VARCHAR)
    * `region_name` (VARCHAR)
* **users**: Raw user information.
    * `user_id` (INT)
    * `user_legacy_id` (VARCHAR)
    * `name` (VARCHAR)
    * `email` (VARCHAR)
    * `age` (INT)
    * `country_id` (INT)
    * `region_id` (INT)
    * `region_code` (VARCHAR)
    * `region_name` (VARCHAR)
    * `signup_date` (VARCHAR)
    * `acquisition_source` (VARCHAR)
* ... (and so on for all your bronze tables)

## Silver Schema
The cleansed and validated data, ready for aggregation.
* **users**: Cleansed user information with enforced data types and unique keys.
    * `user_id` (INT)
    * `user_legacy_id` (VARCHAR) - Primary Key.
    * `name` (VARCHAR)
    * `email` (VARCHAR)
    * `age` (INT)
    * `country_id` (INT)
    * `region_id` (INT)
    * `region_code` (VARCHAR)
    * `region_name` (VARCHAR)
    * `signup_date` (DATE)
    * `acquisition_source` (VARCHAR)
* ... (and so on for all your silver tables)

## Gold Schema
The final, aggregated data model built for analytics and dashboarding.
* **dim_date**: A time dimension table[cite: 38].
    * `date_key` (INT) - Primary Key.
    * `full_date` (DATE)
    * ... (and so on for all date-related fields)
* **dim_user**: A user dimension table[cite: 38].
    * `user_key` (VARCHAR) - Primary Key.
    * `name` (VARCHAR)
    * ... (and so on for all user-related fields)
* **fact_viewing_activity**: A fact table for content viewing events[cite: 38].
    * `date_key` (INT) - Foreign Key.
    * `user_key` (VARCHAR) - Foreign Key.
    * `content_key` (VARCHAR) - Foreign Key.
    * ... (and so on for all fact table columns)

