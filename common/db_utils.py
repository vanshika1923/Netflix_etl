# In common/db_utils.py

import os
import psycopg2
import logging
from dotenv import load_dotenv

def get_db_connection():
    """
    Establishes and returns a connection to the PostgreSQL database using
    credentials from the .env file.
    """
    load_dotenv()  # Load environment variables from .env file

    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        logging.info("Database connection established successfully.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to the database: {e}")
        raise

def execute_sql_file(conn, file_path):
    """
    Executes all SQL commands from a given .sql file.
    """
    logging.info(f"Executing SQL from {file_path}...")
    with conn.cursor() as cur:
        with open(file_path, 'r') as f:
            sql_content = f.read()
            cur.execute(sql_content)
    conn.commit()
    logging.info(f"✅ Successfully executed and COMMITTED {file_path}.")