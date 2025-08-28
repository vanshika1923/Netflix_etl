-- Create the schema for auditing purposes
CREATE SCHEMA IF NOT EXISTS audit;
-- Drop the table to ensure the script is re-runnable
DROP TABLE IF EXISTS audit.rejected_rows CASCADE;
-- Create the table to log rejected rows from the ETL process
CREATE TABLE audit.rejected_rows (
rejection_id SERIAL PRIMARY KEY,
source_table TEXT,
target_table TEXT,
rejection_reason TEXT,
row_data JSONB,
rejected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);