import os
import logging
import time
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- Configuration and Setup ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

LOG_FILE = os.path.join(project_root, 'logs/silver_build.log')

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


# In your silver_layer.py script

def run():
    """Builds the Silver layer by executing the build_silver_layer.sql script."""
    logging.info("--- Starting Silver Layer Build Process using a single SQL script ---")
    start_time = time.time()

    try:
        with engine.connect() as connection:
            with connection.begin():
                # Step 1: Create the silver schema and all tables
                # This is the step that was missing from your script
                schema_file_path = os.path.join(project_root, 'sql/silver_schema.sql')
                logging.info(f"Executing SQL from {schema_file_path}...")
                with open(schema_file_path, 'r') as f:
                    schema_script = f.read()
                connection.execute(text(schema_script))
                logging.info("Successfully executed silver_schema.sql DDL.")

                # Step 2: Now, execute the main build script
                sql_file_path = os.path.join(project_root, 'sql/build_silver_layer.sql')
                logging.info(f"Executing SQL from {sql_file_path}...")
                with open(sql_file_path, 'r') as f:
                    sql_script = f.read()
                connection.execute(text(sql_script))

        logging.info("Successfully executed the unified silver build script.")

    except Exception as e:
        logging.error(f"An error occurred during the Silver build process: {e}", exc_info=True)
        raise

    end_time = time.time()
    logging.info(f"--- Silver Layer Build Process Finished in {end_time - start_time:.2f} seconds ---")
if __name__ == '__main__':
    run()