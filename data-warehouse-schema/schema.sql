\c data_warehouse

CREATE SCHEMA data_warehouse AUTHORIZATION username;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_warehouse TO username;

-- Disable fsync at the end of every transaction.
-- This settings might cause data loss in the event of a power failure
SET LOCAL synchronous_commit TO OFF;

-- Create tables
CREATE TABLE data_warehouse.admissions
(
  id SERIAL PRIMARY KEY,
  admission_id VARCHAR(64) NOT NULL UNIQUE,
  patient_id INTEGER NOT NULL,
  admission_timestamp TIMESTAMP NOT NULL,
  discharge_timestamp TIMESTAMP
);
