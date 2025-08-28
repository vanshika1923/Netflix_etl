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

LOG_FILE = os.path.join(project_root, 'logs/bronze_load.log')
DB_SCHEMA = 'bronze'

# --- Google Sheets Configuration ---
GCP_CREDENTIALS_FILE = os.path.join(project_root, 'gcp_credentials.json')
GOOGLE_SHEET_NAME = 'Medallion_data'
TABLES = [
    'locations', 'users', 'watchlist', 'devices',
    'subscriptions', 'payments', 'viewing_activity'
]

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


# --- Helper Functions ---
def get_dataframe_checksum(df):
    """Calculates a checksum based on the DataFrame's content."""
    df_string = pd.util.hash_pandas_object(df, index=True).to_string()
    return hashlib.sha256(df_string.encode()).hexdigest()


def connect_to_google_sheets():
    """Establishes a connection to Google Sheets using service account credentials."""
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive.readonly'
        ]
        creds = Credentials.from_service_account_file(GCP_CREDENTIALS_FILE, scopes=scopes)
        # Use an AuthorizedSession to handle auth and bypass SSL verification if needed
        authed_session = AuthorizedSession(creds)
        authed_session.verify = False  # SSL bypass for tricky network environments
        gc = gspread.Client(auth=creds, session=authed_session)
        logging.info("Successfully connected to Google Sheets API (with SSL verification disabled).")
        return gc
    except Exception as e:
        logging.error(f"Failed to connect to Google Sheets API: {e}")
        return None


def run():
    """Main function to extract from GSheets and load into the bronze schema."""
    logging.info("--- Starting Bronze Layer Process (Live from Google Sheets) ---")
    start_time = time.time()

    # 1. Execute DDL to set up the database schema
    try:
        with engine.connect() as connection:
            sql_file_path = os.path.join(project_root, 'sql/bronze_schema.sql')
            with open(sql_file_path, 'r') as f:
                ddl_script = f.read()
            with connection.begin():
                connection.execute(text(ddl_script))

        logging.info("Successfully executed bronze_schema.sql DDL.")
    except Exception as e:
        logging.error(f"Error executing DDL script: {e}")
        raise

    # 2. Connect to Google Sheets
    gc = connect_to_google_sheets()
    if not gc:
        return

    try:
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        logging.info(f"Opened Google Sheet: '{GOOGLE_SHEET_NAME}'")
    except gspread.exceptions.SpreadsheetNotFound:
        logging.error(f"Spreadsheet '{GOOGLE_SHEET_NAME}' not found. Please check the name and sharing settings.")
        return

    # 3. Extract, log, and load each table
    for table_name in TABLES:
        try:
            worksheet = spreadsheet.worksheet(table_name)
            logging.info(f"Extracting data from worksheet: '{table_name}'...")

            df = get_as_dataframe(worksheet, evaluate_formulas=True)
            df.dropna(how='all', inplace=True)

            if df.empty:
                logging.warning(f"No data found in worksheet '{table_name}'. Skipping.")
                continue

            checksum = get_dataframe_checksum(df)
            logging.info(f"Extracted {len(df)} rows from '{table_name}'. Checksum: {checksum}")

            # Load to PostgreSQL
            df.to_sql(
                table_name,
                engine,
                schema=DB_SCHEMA,
                if_exists='replace',
                index=False,
                method='multi'
            )

            logging.info(f"Successfully loaded data into {DB_SCHEMA}.{table_name}.")

        except gspread.exceptions.WorksheetNotFound:
            logging.warning(f"Worksheet '{table_name}' not found in the spreadsheet. Skipping.")
        except Exception as e:
            logging.error(f"Failed to process table '{table_name}'. Error: {e}")

    end_time = time.time()
    logging.info(f"--- Bronze Layer Process Finished in {end_time - start_time:.2f} seconds ---")


if __name__ == '__main__':
    run()