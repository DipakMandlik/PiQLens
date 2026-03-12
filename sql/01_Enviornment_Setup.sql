-- ============================================================================
-- PRODUCTION-READY ENVIRONMENT SETUP
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Initialize Snowflake environment with 3-layer architecture
-- Layers: Bronze (Raw) → Silver (Cleansed) → Gold (Analytics)
-- Version: 1.0.0
-- Author: Pi-Qualytics Team
-- Date: 2026-01-22
-- ============================================================================

-- ============================================================================
-- SECTION 1: ROLE & WAREHOUSE SETUP
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Ingestion Warehouse (for ETL and data loading)
CREATE WAREHOUSE IF NOT EXISTS DQ_INGESTION_WH
WITH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60        -- Suspend after 1 minute of inactivity
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT   = 1
    MAX_CLUSTER_COUNT   = 2
    SCALING_POLICY      = 'STANDARD'
COMMENT = 'Warehouse for data ingestion, ETL operations, and Bronze layer processing';

-- Analytics Warehouse (for DQ checks and reporting)
CREATE WAREHOUSE IF NOT EXISTS DQ_ANALYTICS_WH
WITH
    WAREHOUSE_SIZE      = 'SMALL'
    AUTO_SUSPEND        = 300       -- Suspend after 5 minutes of inactivity
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT   = 1
    MAX_CLUSTER_COUNT   = 3
    SCALING_POLICY      = 'STANDARD'
COMMENT = 'Warehouse for analytics, data quality checks, and Gold layer aggregations';



-- ============================================================================
-- SECTION 2: DATABASE STRUCTURE (MEDALLION ARCHITECTURE)
-- ============================================================================

-- TESTING Data Warehouse 
CREATE DATABASE IF NOT EXISTS TESTING
    DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'TESTING-DATABASE';

USE DATABASE TESTING;

-- Purpose: Store raw, unprocessed data exactly as received from source systems
CREATE SCHEMA IF NOT EXISTS TESTING_SCHEMA
    DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'TESTING_SCHEMA';


-- ============================================================================
-- SECTION 3: DATA QUALITY FRAMEWORK DATABASE
-- ============================================================================

CREATE DATABASE IF NOT EXISTS DATA_QUALITY_DB
    DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Data Quality Framework - Configuration, metrics, and observability';

USE DATABASE DATA_QUALITY_DB;

-- Configuration Schema (metadata and rules)
CREATE SCHEMA IF NOT EXISTS DQ_CONFIG
COMMENT = 'DQ Configuration - Dataset configs, rules, schedules, and mappings';

-- Metrics Schema (results and logs)
CREATE SCHEMA IF NOT EXISTS DQ_METRICS
COMMENT = 'DQ Metrics - Check results, profiling data, run logs, and failed records';

-- Engine Schema (stored procedures)
CREATE SCHEMA IF NOT EXISTS DQ_ENGINE
COMMENT = 'DQ Engine - Stored procedures and functions for quality checks';

-- Observability Schema (AI-driven insights)
CREATE SCHEMA IF NOT EXISTS DB_METRICS
COMMENT = 'DQ Observability - AI-driven metrics, insights, and schema registry';

-- ============================================================================
-- SECTION 4: FILE FORMAT & STAGE SETUP
-- ============================================================================
USE DATABASE TESTING;
USE SCHEMA TESTING_SCHEMA;
USE WAREHOUSE DQ_INGESTION_WH;

-- Standard CSV File Format
CREATE OR REPLACE FILE FORMAT CSV_FILE_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = '\134'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    NULL_IF = ('NULL', 'null', '', 'N/A', 'NA', 'n/a', '-')
    EMPTY_FIELD_AS_NULL = TRUE
COMMENT = 'Standard CSV file format for data ingestion with flexible error handling';

-- JSON File Format (for future use)
CREATE OR REPLACE FILE FORMAT JSON_FILE_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = TRUE
COMMENT = 'Standard JSON file format for semi-structured data ingestion';

-- Internal Stage for CSV uploads
CREATE OR REPLACE STAGE CSV_STAGE
    FILE_FORMAT = CSV_FILE_FORMAT
    DIRECTORY = (ENABLE = TRUE)
COMMENT = 'Internal stage for uploading CSV files from local/external sources';


-- ============================================================================
-- SECTION 5: TESTING LAYER - RAW DATA TABLES
-- ============================================================================

USE SCHEMA TESTING_SCHEMA;

create or replace TABLE TESTING.TESTING_SCHEMA.Test_Customer (
	ACCOUNT_ID VARCHAR(16777216) COMMENT 'Account unique identifier (raw)',
	OPENING_BALANCE VARCHAR(16777216) COMMENT 'Opening balance (raw, may contain invalid numbers)',
	OPENED_DATE VARCHAR(16777216) COMMENT 'Account opening date (raw format)',
    EMAIL VARCHAR(123456),
	LOAD_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record load timestamp'
)COMMENT='Bronze - Raw account data (schema-on-read, all columns as STRING)'
;


-- ============================================================================
-- SECTION 6: VERIFICATION QUERIES
-- ============================================================================

-- Verify databases
SHOW DATABASES LIKE '%TESTING%';
SHOW DATABASES LIKE '%DATA_QUALITY_DB%';



-- ============================================================================
-- SECTION 7: GRANT PERMISSIONS
-- ============================================================================

-- Grant usage on warehouses
GRANT USAGE ON WAREHOUSE DQ_INGESTION_WH TO ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE DQ_ANALYTICS_WH TO ROLE ACCOUNTADMIN;

-- Grant database permissions
GRANT ALL ON DATABASE TESTING TO ROLE ACCOUNTADMIN;
GRANT ALL ON DATABASE DATA_QUALITY_DB TO ROLE ACCOUNTADMIN;

-- Grant schema permissions
GRANT ALL ON ALL SCHEMAS IN DATABASE TESTING TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL SCHEMAS IN DATABASE DATA_QUALITY_DB TO ROLE ACCOUNTADMIN;


-- ============================================================================
-- SETUP COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Upload CSV files to @CSV_STAGE using SnowSQL or Snowflake UI
-- 2. Execute 02_Data_Loading.sql to load data into Bronze layer
-- 3. Execute 03_Silver_Layer_Setup.sql to create cleansed tables
-- 4. Execute 04_Gold_Layer_Setup.sql to create analytics views
-- 5. Execute 05_DQ_Framework_Setup.sql to initialize quality checks
-- ============================================================================
