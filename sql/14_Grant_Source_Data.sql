-- ============================================================================
-- PI_QLENS SOURCE DATA ACCESS GRANTS
-- ============================================================================
-- Purpose:
-- Grant PIQLENS_APP_USER (via PIQLENS_ENGINEER_ROLE) read access to source
-- databases/schemas/tables used for Data Quality execution.
--
-- Safe design:
-- - Keep write permissions in DATA_QUALITY_DB roles as-is.
-- - Add read-only access for source data via PIQLENS_SOURCE_READER_ROLE.
-- - No change to key-pair authentication setup.
-- ============================================================================

USE ROLE SECURITYADMIN;

-- ----------------------------------------------------------------------------
-- 1) Create source-reader role and attach to PIQLens hierarchy
-- ----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS PIQLENS_SOURCE_READER_ROLE;

-- Engineer inherits source read access
GRANT ROLE PIQLENS_SOURCE_READER_ROLE TO ROLE PIQLENS_ENGINEER_ROLE;

-- Optional: uncomment if analysts should also browse source tables
-- GRANT ROLE PIQLENS_SOURCE_READER_ROLE TO ROLE PIQLENS_ANALYST_ROLE;

-- Optional: uncomment if viewers should also browse source tables
-- GRANT ROLE PIQLENS_SOURCE_READER_ROLE TO ROLE PIQLENS_VIEWER_ROLE;

-- Warehouse usage for source reads
GRANT USAGE ON WAREHOUSE DQ_ANALYTICS_WH TO ROLE PIQLENS_SOURCE_READER_ROLE;

-- ----------------------------------------------------------------------------
-- 2) SOURCE ACCESS: TESTING database (current known source in this project)
-- ----------------------------------------------------------------------------
GRANT USAGE ON DATABASE BANKING TO ROLE PIQLENS_SOURCE_READER_ROLE;
GRANT USAGE ON SCHEMA BANKING.BRONZE TO ROLE PIQLENS_SOURCE_READER_ROLE;
GRANT USAGE ON SCHEMA BANKING.SILVER TO ROLE PIQLENS_SOURCE_READER_ROLE;
GRANT USAGE ON SCHEMA BANKING.GOLD TO ROLE PIQLENS_SOURCE_READER_ROLE;

-- Existing objects
GRANT SELECT ON ALL TABLES IN SCHEMA BANKING.BRONZE TO ROLE PIQLENS_SOURCE_READER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA BANKING.SILVER TO ROLE PIQLENS_SOURCE_READER_ROLE;
GRANT SELECT ON ALL VIEWS  IN SCHEMA BANKING.SILVER TO ROLE PIQLENS_SOURCE_READER_ROLE;
GRANT SELECT ON ALL VIEWS  IN SCHEMA BANKING.GOLD TO ROLE PIQLENS_SOURCE_READER_ROLE;

-- Future objects
GRANT SELECT ON FUTURE TABLES IN SCHEMA BANKING.BRONZE TO ROLE PIQLENS_SOURCE_READER_ROLE;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA BANKING.SILVER TO ROLE PIQLENS_SOURCE_READER_ROLE;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA BANKING.GOLD TO ROLE PIQLENS_SOURCE_READER_ROLE;

-- ----------------------------------------------------------------------------
-- 3) OPTIONAL: add additional source databases/schemas here
-- ----------------------------------------------------------------------------
-- Example pattern:
-- GRANT USAGE ON DATABASE <SOURCE_DB> TO ROLE PIQLENS_SOURCE_READER_ROLE;
-- GRANT USAGE ON SCHEMA <SOURCE_DB>.<SOURCE_SCHEMA> TO ROLE PIQLENS_SOURCE_READER_ROLE;
-- GRANT SELECT ON ALL TABLES IN SCHEMA <SOURCE_DB>.<SOURCE_SCHEMA> TO ROLE PIQLENS_SOURCE_READER_ROLE;
-- GRANT SELECT ON ALL VIEWS  IN SCHEMA <SOURCE_DB>.<SOURCE_SCHEMA> TO ROLE PIQLENS_SOURCE_READER_ROLE;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA <SOURCE_DB>.<SOURCE_SCHEMA> TO ROLE PIQLENS_SOURCE_READER_ROLE;
-- GRANT SELECT ON FUTURE VIEWS  IN SCHEMA <SOURCE_DB>.<SOURCE_SCHEMA> TO ROLE PIQLENS_SOURCE_READER_ROLE;

-- ----------------------------------------------------------------------------
-- 4) Verification (admin session)
-- ----------------------------------------------------------------------------
SHOW GRANTS TO ROLE PIQLENS_SOURCE_READER_ROLE;
SHOW GRANTS OF ROLE PIQLENS_ENGINEER_ROLE;
SHOW GRANTS TO USER PIQLENS_APP_USER;

-- ----------------------------------------------------------------------------
-- 5) Verification (run in PIQLENS_APP_USER session)
-- ----------------------------------------------------------------------------
-- USE ROLE PIQLENS_ENGINEER_ROLE;
-- USE WAREHOUSE DQ_ANALYTICS_WH;
-- SHOW DATABASES;
-- SHOW SCHEMAS IN DATABASE TESTING;
-- SHOW TABLES IN SCHEMA TESTING.TESTING_SCHEMA;
-- SELECT COUNT(*) AS SAMPLE_COUNT FROM TESTING.TESTING_SCHEMA.TEST_CUSTOMER;

-- ============================================================================
-- End
-- ============================================================================
