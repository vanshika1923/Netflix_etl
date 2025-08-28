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
