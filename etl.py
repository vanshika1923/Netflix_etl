#!/usr/bin/env python3
import os
import sys
import subprocess
import logging
from dotenv import load_dotenv
import time

# --- Configuration and Setup ---
# A more robust way to find the project root from the current file's location
current_file_dir = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(current_file_dir) == 'src':
    project_root = os.path.join(current_file_dir, '..')
else:
    project_root = current_file_dir

dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

# --- Logging Setup ---
logs_dir = os.path.join(project_root, 'logs')
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

LOG_FILE = os.path.join(project_root, 'logs/etl_pipeline.log')
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)


# --- Pipeline Orchestration ---
def run_pipeline():
    """
    Runs the entire ETL pipeline in the correct order:
    Bronze -> Silver -> Gold.
    """
    scripts = [
        "bronze_layer.py",
        "silver_layer.py",
        "gold_layer.py"
    ]

    python_executable = sys.executable

    logging.info("--- Starting ETL Pipeline ---")
    start_time = time.time()

    for script_name in scripts:
        logging.info(f"\n--- Executing {script_name} ---")
        script_path = os.path.join(project_root, 'src', script_name)
        command = [python_executable, script_path]

        try:
            subprocess.run(command, check=True, cwd=project_root)
            logging.info(f"✅ Successfully completed {script_name}")
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ ETL Pipeline failed at {script_name}. Exiting.")
            logging.error(f"Error details: {e}")
            return

    end_time = time.time()
    logging.info(f"\n✅✅✅ ETL Pipeline completed successfully in {end_time - start_time:.2f} seconds ✅✅✅")


if __name__ == '__main__':
    run_pipeline()