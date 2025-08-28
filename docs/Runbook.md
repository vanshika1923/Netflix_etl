# Runbook
1) Create venv, install requirements, configure `.env` if using Postgres.
2) `python etl.py all` to load Bronze → Silver → Gold.
3) Check logs in `logs/pipeline.log` and audit tables:
   - `audit_loads`, `audit_dq_results`, `audit_rejected_rows`.
4) Point BI tool to `gold_*` tables.
