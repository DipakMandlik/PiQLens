select current_timestamp();

ALTER SESSION SET TIMEZONE = 'Asia/Kolkata';

INSERT INTO TESTING.TESTING_SCHEMA.TEST_CUSTOMER
(
    ACCOUNT_ID,
    OPENING_BALANCE,
    OPENED_DATE,
    LOAD_TIMESTAMP,
    EMAIL
)
VALUES
-- 1️⃣ Perfect record
('ACC3001','75001.00','2023-05-13',CURRENT_TIMESTAMP(),'a03@gmail.com'),   
('ACC3002','75002.00','2023-05-14',CURRENT_TIMESTAMP(),'b03@gmail.com'),   
('ACC3003','75003.00','2192929',CURRENT_TIMESTAMP(),'a03gmail.com'),   
('ACC3004','75004.00','20230516',CURRENT_TIMESTAMP(),'a03&&gmail.com'),   
('ACC3005','75005.00','2023-05-17',CURRENT_TIMESTAMP(),'c03@gmail.com'),    
('ACC3006','75006.00','2023-05-18',CURRENT_TIMESTAMP(),'d03@gmail.com'),    
('ACC3007','75007.00','2023-05-19',CURRENT_TIMESTAMP(),'e03@gmail.com'),   
('ACC3008','75008.00','2023-05-20',CURRENT_TIMESTAMP(),'f03@gmail.com'),   
('ACC3009','75009.00','2023-05-21',CURRENT_TIMESTAMP(),'g03@gmail.com'),    
('ACC3010','75010.00','2023-05-22',CURRENT_TIMESTAMP(),'h03@gmail.com');    


select * from data_quality_db.dq_metrics.dq_run_control;

select * from data_quality_db.dq_metrics.dq_daily_summary;

select * from data_quality_db.dq_metrics.dq_check_results;

delete from data_quality_db.dq_metrics.dq_check_results where check_timestamp >= '2026-02-20 10:58:32.109';
delete from data_quality_db.dq_metrics.DQ_RUN_CONTROL where START_TS >= '2026-02-20 10:52:54.039';
delete from data_quality_db.dq_metrics.dq_daily_summary where SUMMARY_DATE >= '2026-02-20';

select * from data_quality_db.dq_config.dataset_rule_config where dataset_id='DS_TESTING_ACCOUNT';

select * from testing.testing_schema.test_customer;


delete from testing.testing_schema.test_customer where load_timestamp>='2026-02-19 10:10:41.356';

-- ============================================================================
-- ADD RUN_TYPE AND EXECUTION_MODE COLUMNS TO DQ_RUN_CONTROL
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Add explicit RUN_TYPE and EXECUTION_MODE columns so that every
--          execution is self-describing instead of relying on RUN_ID patterns
-- Prerequisites: 06_Metrics_Tables.sql executed
-- Version: 1.0.0
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_METRICS;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- ============================================================================
-- SECTION 1: ADD NEW COLUMNS
-- ============================================================================

ALTER TABLE DQ_RUN_CONTROL
  ADD COLUMN IF NOT EXISTS RUN_TYPE VARCHAR(50)
  COMMENT 'FULL_SCAN | INCREMENTAL_SCAN | CUSTOM_SCAN | PROFILING';

ALTER TABLE DQ_RUN_CONTROL
  ADD COLUMN IF NOT EXISTS EXECUTION_MODE VARCHAR(50)
  COMMENT 'MANUAL | SCHEDULED | AUTO';

-- ============================================================================
-- SECTION 2: BACKFILL EXISTING ROWS
-- ============================================================================

-- Derive RUN_TYPE from RUN_ID naming convention used by SPs and routes
UPDATE DQ_RUN_CONTROL
SET RUN_TYPE = CASE
    WHEN RUN_ID LIKE 'DQ_INC%' OR UPPER(RUN_ID) LIKE '%INCR%' THEN 'INCREMENTAL_SCAN'
    WHEN RUN_ID LIKE 'DQ_PROFILE%' THEN 'PROFILING'
    WHEN RUN_ID LIKE 'DQ_CUSTOM%' THEN 'CUSTOM_SCAN'
    ELSE 'FULL_SCAN'
  END
WHERE RUN_TYPE IS NULL;

-- Derive EXECUTION_MODE from TRIGGERED_BY
UPDATE DQ_RUN_CONTROL
SET EXECUTION_MODE = CASE
    WHEN UPPER(COALESCE(TRIGGERED_BY, '')) IN ('S','SCHEDULED','SCHEDULED_TASK','SYSTEM','SCHEDULER') THEN 'SCHEDULED'
    WHEN UPPER(COALESCE(TRIGGERED_BY, '')) IN ('A','AUTO','AUTOMATED','BOT','ETL_PIPELINE') THEN 'AUTO'
    ELSE 'MANUAL'
  END
WHERE EXECUTION_MODE IS NULL;

-- ============================================================================
-- SECTION 3: VERIFICATION
-- ============================================================================

SELECT RUN_TYPE, EXECUTION_MODE, COUNT(*) AS CNT
FROM DQ_RUN_CONTROL
GROUP BY RUN_TYPE, EXECUTION_MODE
ORDER BY RUN_TYPE, EXECUTION_MODE;

SELECT '=== RUN_TYPE / EXECUTION_MODE MIGRATION COMPLETE ===' AS STATUS;



SELECT * FROM DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES;


select sql_template from data_quality_db.dq_config.rule_sql_template where rule_id=2;


SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION;



SELECT 
    COUNT(*) AS total_rows,
    COUNT(CASE 
        WHEN LOAD_TIMESTAMP >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP()) 
        THEN 1 END) AS qualifying_rows
FROM TESTING.TESTING_SCHEMA.TEST_CUSTOMER;




-- ========================================================================

-- DQ_FAILED_RECORDS schema and helper procedures
-- Path: sql/production/20_DQ_FAILED_RECORDS.sql
-- Purpose: store deterministic sample failed rows (max 100 per rule-run)
-- Target engine: Snowflake (examples use Snowflake JS stored procedures)

-- 1) Table definition
CREATE TABLE IF NOT EXISTS DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS (
  FAILURE_ID NUMBER AUTOINCREMENT,
  RUN_ID VARCHAR,
  DATASET_ID VARCHAR,
  RULE_ID NUMBER,
  RULE_NAME VARCHAR,
  RULE_TYPE VARCHAR,
  COLUMN_NAME VARCHAR,
  TABLE_NAME VARCHAR,
  PRIMARY_KEY_COLUMN VARCHAR,
  PRIMARY_KEY_VALUE VARCHAR,
  FAILED_VALUE VARCHAR,
  FAILURE_REASON VARCHAR,
  FAILED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 2) Clustering for faster rule / dataset lookups (Snowflake clustering key)
-- Adjust per account/warehouse and table size
ALTER TABLE DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS
  CLUSTER BY (RUN_ID, DATASET_ID, RULE_ID);

-- 3) Example Snowflake stored procedure to insert up to 100 failing rows
-- Note: This is an example pattern. Validate/whitelist identifiers from dataset
-- metadata to prevent SQL injection. `table_name`, `pk_col`, and `failure_col`
-- must come from trusted config; failure_condition must be constructed safely.


-- 4) Example usage (pattern) from rule executor (pseudocode):
-- Assume you have dataset configuration with primary key column name.
-- CALL DATA_QUALITY_DB.DQ_METRICS.INSERT_FAILED_ROWS(
--   :run_id, :dataset_id, :rule_id, :rule_name, :rule_type,
--   :table_name, :pk_col, :failure_col, :failure_condition
-- );

-- 5) Example static INSERT (non-proc) template for engines without JS procs
-- Replace <placeholders> with validated identifiers/expressions.
-- INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS (
--   RUN_ID, DATASET_ID, RULE_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, TABLE_NAME,
--   PRIMARY_KEY_COLUMN, PRIMARY_KEY_VALUE, FAILED_VALUE, FAILURE_REASON)
-- SELECT
--   :run_id,
--   :dataset_id,
--   :rule_id,
--   :rule_name,
--   :rule_type,
--   '<failure_col>',
--   '<table_name>',
--   '<pk_col>',
--   TO_VARCHAR(t.<pk_col>),
--   TO_VARCHAR(t.<failure_col>),
--   'Validation failed: <brief reason>'
-- FROM <table_name> t
-- WHERE <failure_condition>
-- LIMIT 100;

-- 6) Maintenance guidance:
-- - Keep only samples to control storage; consider a retention policy (e.g. 90 days)
--   using a time-truncated copy or scheduled purge job.
-- - Use clustering keys as above to speed up queries by RUN, DATASET, RULE.
-- - Consider materialized views for aggregated metrics (summary) built from
--   DQ_FAILED_RECORDS when needed.




INSERT INTO TESTING.TESTING_SCHEMA.TEST_CUSTOMER
(
    ACCOUNT_ID,
    OPENING_BALANCE,
    OPENED_DATE,
    LOAD_TIMESTAMP,
    EMAIL
)
VALUES
-- 1️⃣ Perfect record
('ACC3013','75013.00','2023-05-23',CURRENT_TIMESTAMP(),'dipa03@gmail.com'),
('ACC3014','75023.00','2023-05-24',CURRENT_TIMESTAMP(),'dipa');


-- 24/02/2026
select current_timestamp();
INSERT INTO TESTING.TESTING_SCHEMA.TEST_CUSTOMER
(
    ACCOUNT_ID,
    OPENING_BALANCE,
    OPENED_DATE,
    LOAD_TIMESTAMP,
    EMAIL
)
VALUES
-- 1️⃣ Perfect record
('ACC3015','75033.00','2023-05-24',CURRENT_TIMESTAMP(),'dipakmandlik03@gmail.com'),
('ACC3016','75043.00','2023-05-26',CURRENT_TIMESTAMP(),'d.m@pibythree.com');


describe table data_quality_db.dq_metrics.dq_daily_summary;

select * from testing.testing_schema.test_customer;