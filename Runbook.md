Data Pipeline Runbook
This document provides a step-by-step guide for setting up the environment, executing the end-to-end data pipeline, and verifying the results.

🛠️ 1. Prerequisite Setup
Follow these steps to configure your local environment.

1.1. Clone the Repository
Clone the project source code from the GitHub repository.

git clone <your-repository-url>
cd <your-repository-name>

1.2. Set Up a Python Virtual Environment
It is highly recommended to use a virtual environment to manage dependencies.

# Create a virtual environment
python -m venv venv

# Activate the environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

1.3. Install Dependencies
Install all required Python packages from the requirements.txt file.

pip install -r requirements.txt

1.4. Configure Environment Variables
The pipeline requires credentials for the database and Google API.

Locate the .env.example file in the root directory.

Create a copy of this file and name it .env.

Populate the .env file with your credentials.

# .env file
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
GOOGLE_SHEETS_CREDENTIALS_PATH=path/to/your/google_credentials.json

1.5. Set Up PostgreSQL Database
Ensure you have a running PostgreSQL instance. Use a client like psql or a GUI tool to run the following commands:

-- Create a new database for the pipeline
CREATE DATABASE your_db_name;

-- Create a dedicated user and grant privileges
CREATE USER your_db_user WITH PASSWORD 'your_db_password';
GRANT ALL PRIVILEGES ON DATABASE your_db_name TO your_db_user;

🚀 2. Pipeline Execution
Once the setup is complete, you can run the ETL pipeline.

2.1. Prepare Source Data
Ensure your raw data is correctly formatted and available in the Google Sheet named Medallion_data.

2.2. Run the End-to-End Pipeline
Execute the main script from the project's root directory. This single command will trigger all three stages of the pipeline.

python src/etl.py

Pipeline Stages:

Bronze Layer: Ingests raw data from the Google Sheet into the bronze schema without modification.

Silver Layer: Cleans, validates, deduplicates, and standardizes the data, storing the results in the silver schema.

Gold Layer: Transforms and aggregates the cleansed data into a star schema (fact and dimension tables) in the gold schema, ready for analytics.

✅ 3. Verification and Validation
After the pipeline execution completes, follow these steps to validate the run.

3.1. Check Logs
Review the log file for detailed information about the execution process and confirm a success message.

cat logs/etl_pipeline.log

Look for a final entry like ETL pipeline completed successfully.

3.2. Verify Database Tables
Connect to your PostgreSQL database using a client and verify that the gold schema tables are populated correctly.

Check table existence: Ensure fact and dimension tables have been created in the gold schema.

Query the data: Run sample queries to check if the data has been loaded.

-- Example: Check the row count of a fact table
SELECT COUNT(*) FROM gold.fact_sales;

-- Example: Select a few records from a dimension table
SELECT * FROM gold.dim_customer LIMIT 10;

📦 4. Post-Execution and Deployment
4.1. BI Tool Connection
Connect your BI tool (e.g., Tableau, Power BI) to the PostgreSQL database using the credentials from your .env file. Point the tool to the tables in the gold schema to build dashboards.

4.2. Schedule Automation
To run the pipeline on a schedule, you can use the provided automation script or configure a cron job.

# Example of running a scheduling script
python scripts/schedule_pipeline.py

Note: Refer to the script's documentation for details on configuring the schedule.

4.3. Version Control
Commit and push all final code changes, logs, and documentation to the GitHub repository.

git add .
git commit -m "feat: Finalize pipeline execution and update runbook"
git push origin main
