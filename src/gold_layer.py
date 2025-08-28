import os
import pandas as pd
import logging
import hashlib
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession

# --- Configuration and Setup ---
# Build an absolute path to the .env file in the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

LOG_FILE = os.path.join(project_root, 'logs/gold_build.log')
DB_SCHEMA_SILVER = 'silver'
DB_SCHEMA_GOLD = 'gold'

# --- Database Connection ---
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# --- Logging Setup ---
logs_dir = os.path.join(project_root, 'logs')
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def load_df_to_gold(df, table_name):
    """Helper function to load a DataFrame into a Gold table."""
    if df.empty:
        logging.warning(f"DataFrame for gold.{table_name} is empty. Skipping.")
        return
    logging.info(f"Loading {len(df)} rows into gold.{table_name}...")
    df.to_sql(
        table_name,
        engine,
        schema=DB_SCHEMA_GOLD,
        if_exists='append',
        index=False
    )
    logging.info(f"Successfully loaded data into gold.{table_name}.")

def run():
    """Main function to build the Gold layer using a star schema model."""
    logging.info("--- Starting Gold Layer Build Process ---")
    start_time = time.time()
    try:
        # Step 1: Execute DDL to create/reset the Gold schema and its tables.
        with engine.connect() as connection:
            with connection.begin():
                logging.info("Executing gold_schema.sql DDL...")
                sql_file_path = os.path.join(project_root, 'sql/gold_schema.sql')
                with open(sql_file_path, 'r') as f:
                    connection.execute(text(f.read()))
                logging.info("Successfully executed gold_schema.sql.")

        # Step 2: Build and load the Dimension tables first.
        # --- Build dim_date ---
        logging.info("Building gold.dim_date...")
        date_query = """
        INSERT INTO gold.dim_date (date_key, full_date, year, quarter, month,
        day, day_of_week, month_name, is_weekend)
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
            d::DATE AS full_date,
            EXTRACT(YEAR FROM d) AS year,
            EXTRACT(QUARTER FROM d) AS quarter,
            EXTRACT(MONTH FROM d) AS month,
            EXTRACT(DAY FROM d) AS day,
            EXTRACT(ISODOW FROM d) AS day_of_week,
            TRIM(TO_CHAR(d, 'Month')) AS month_name,
            (EXTRACT(ISODOW FROM d) IN (6, 7)) AS is_weekend
        FROM generate_series('2018-01-01'::date, '2025-12-31'::date, '1 day'::interval) d;
        """
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text(date_query))
        logging.info("Successfully populated gold.dim_date.")

        # --- Build dim_content ---
        logging.info("Building gold.dim_content...")
        content_query = f"""
        SELECT
            w.content_id AS content_key,
            w.title,
            ct.content_type_name,
            g.genre_name,
            w.release_year
        FROM {DB_SCHEMA_SILVER}.watchlist w
        LEFT JOIN {DB_SCHEMA_SILVER}.dim_content_types ct ON w.content_type_id = ct.content_type_id
        LEFT JOIN {DB_SCHEMA_SILVER}.dim_genres g ON w.genre_id = g.genre_id;
        """
        df_content = pd.read_sql(content_query, engine)
        load_df_to_gold(df_content, 'dim_content')

        # --- Build dim_user ---
        logging.info("Building gold.dim_user...")
        user_query = f"""
        WITH latest_subscription AS (
            SELECT
                s.user_legacy_id,
                s.start_date,
                s.end_date,
                s.plan_type_id,
                ROW_NUMBER() OVER (PARTITION BY s.user_legacy_id ORDER BY s.start_date DESC) AS rn
            FROM {DB_SCHEMA_SILVER}.subscriptions s
        )
        SELECT
            TRIM(u.user_legacy_id) AS user_key,
            u.name,
            u.email,
            l.country_name,
            l.region_name,
            MIN(sub.start_date) AS first_subscription_date,
            pt.plan_type_name AS current_plan_type,
            (latest_sub.end_date IS NULL OR latest_sub.end_date > CURRENT_DATE) AS is_active
        FROM {DB_SCHEMA_SILVER}.users u
        LEFT JOIN {DB_SCHEMA_SILVER}.locations l ON u.country_id = l.country_id
        LEFT JOIN {DB_SCHEMA_SILVER}.subscriptions sub ON u.user_legacy_id = sub.user_legacy_id
        LEFT JOIN latest_subscription latest_sub ON u.user_legacy_id = latest_sub.user_legacy_id AND latest_sub.rn = 1
        LEFT JOIN {DB_SCHEMA_SILVER}.dim_plan_types pt ON latest_sub.plan_type_id = pt.plan_type_id
        GROUP BY
            TRIM(u.user_legacy_id), u.name, u.email, l.country_name, l.region_name, pt.plan_type_name, latest_sub.end_date;
        """
        df_user = pd.read_sql(user_query, engine)
        # FIX: Corrected table name to 'dim_user'
        load_df_to_gold(df_user, 'dim_user')

        # Step 3: Build and load the Fact tables using the dimension keys.
        # --- Build fact_subscription_events ---
        logging.info("Building gold.fact_subscription_events...")
        sub_events_query = f"""
                SELECT
                    d.date_key,
                    u.user_key,
                    et.event_type_name,
                    p.amount AS mrr_change
                FROM {DB_SCHEMA_SILVER}.payments p
                JOIN {DB_SCHEMA_SILVER}.subscriptions s ON p.sub_id = s.sub_id
                -- FIX: Trim the user_legacy_id to match the key in dim_user
                JOIN gold.dim_user u ON TRIM(p.user_legacy_id) = u.user_key
                JOIN gold.dim_date d ON s.start_date = d.full_date
                JOIN {DB_SCHEMA_SILVER}.dim_event_types et ON s.last_event_type_id = et.event_type_id;
                """
        df_sub_events = pd.read_sql(sub_events_query, engine)
        load_df_to_gold(df_sub_events, 'fact_subscription_events')

        # --- Build fact_viewing_activity ---
        logging.info("Building gold.fact_viewing_activity...")
        viewing_fact_query = f"""
                        SELECT
                            d.date_key,
                            u.user_key,
                            c.content_key,
                            va.device_id,
                            va.duration_watched_sec,
                            va.rating AS user_rating
                        FROM {DB_SCHEMA_SILVER}.viewing_activity va
                        -- FIX: Trim the user_legacy_id to match the key in dim_user
                        JOIN gold.dim_user u ON TRIM(va.user_legacy_id) = u.user_key
                        JOIN gold.dim_content c ON va.content_id::VARCHAR = c.content_key
                        JOIN gold.dim_date d ON va.session_start_ts::DATE = d.full_date;
                        """
        df_viewing_fact = pd.read_sql(viewing_fact_query, engine)
        load_df_to_gold(df_viewing_fact, 'fact_viewing_activity')
    except Exception as e:
        logging.error(f"An error occurred during the Gold build process: {e}", exc_info=True)
        raise

    end_time = time.time()
    logging.info(f"--- Gold Layer Build Process Finished in {end_time - start_time:.2f} seconds ---")

if __name__ == '__main__':
    run()