-- ============================================================================
-- DATA QUALITY METRICS TABLES - PRODUCTION SETUP
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Create all DQ metrics and results tracking tables
-- Prerequisites: 01_Environment_Setup.sql and 05_Config_Tables.sql executed
-- Version: 1.0.0
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_METRICS;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- ============================================================================
-- SECTION 1: DROP EXISTING TABLES (IF RECREATING)
-- ============================================================================
-- Note: Uncomment only if you need to recreate tables from scratch

-- DROP TABLE IF EXISTS DQ_FAILED_RECORDS;
-- DROP TABLE IF EXISTS DQ_CHECK_RESULTS;
-- DROP TABLE IF EXISTS DQ_DAILY_SUMMARY;
-- DROP TABLE IF EXISTS DQ_WEEKLY_SUMMARY;
-- DROP TABLE IF EXISTS DQ_MONTHLY_SUMMARY;
-- DROP TABLE IF EXISTS DQ_QUARTERLY_SUMMARY;
-- DROP TABLE IF EXISTS DQ_COLUMN_PROFILE;
-- DROP TABLE IF EXISTS DQ_RUN_CONTROL;
-- DROP TABLE IF EXISTS DATA_LINEAGE;

-- ============================================================================
-- SECTION 2: RUN CONTROL TABLE
-- ============================================================================

CREATE OR REPLACE TABLE DQ_RUN_CONTROL (

    -- Unique Run Identifier
    RUN_ID                      VARCHAR(100) PRIMARY KEY,

    -- Execution Classification
    EXECUTION_MODE              VARCHAR(20) COMMENT 'FULL or INCREMENTAL',
    TRIGGER_TYPE                VARCHAR(20) COMMENT 'MANUAL | SCHEDULED | API',
    TRIGGERED_BY                VARCHAR(100) COMMENT 'User or system that triggered the run',

    -- Timing Information
    START_TS                    TIMESTAMP_NTZ COMMENT 'Run start timestamp',
    END_TS                      TIMESTAMP_NTZ COMMENT 'Run end timestamp',
    DURATION_SECONDS            NUMBER(10,2) COMMENT 'Total execution duration in seconds',

    -- Run Status
    RUN_STATUS                  VARCHAR(50) COMMENT 
        'RUNNING | COMPLETED | COMPLETED_WITH_ERRORS | COMPLETED_WITH_FAILURES | FAILED',

    -- Dataset-Level Aggregates
    TOTAL_DATASETS              NUMBER COMMENT 'Number of datasets eligible',
    DATASETS_PROCESSED          NUMBER COMMENT 'Datasets executed',
    DATASETS_SKIPPED            NUMBER COMMENT 'Datasets skipped (no new data)',

    -- Check-Level Aggregates
    TOTAL_CHECKS                NUMBER COMMENT 'Total number of checks executed',
    PASSED_CHECKS               NUMBER COMMENT 'Number of checks that passed',
    FAILED_CHECKS               NUMBER COMMENT 'Number of checks that failed',
    WARNING_CHECKS              NUMBER COMMENT 'Number of checks with warnings',
    SKIPPED_CHECKS              NUMBER COMMENT 'Number of checks skipped',

    -- Error Handling
    ERROR_MESSAGE               VARCHAR(4000) COMMENT 'Error message if run failed',

    -- Audit
    CREATED_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

)
COMMENT = 'Run-level control and orchestration tracking for DQ executions';


-- Create clustering for performance
ALTER TABLE DQ_RUN_CONTROL CLUSTER BY (START_TS);

-- ============================================================================
-- SECTION 3: CHECK RESULTS TABLE
-- ============================================================================

CREATE OR REPLACE TABLE DQ_CHECK_RESULTS (
    CHECK_ID                    NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID                      VARCHAR(100) NOT NULL,
    CHECK_TIMESTAMP             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Dataset Information
    DATASET_ID                  VARCHAR(100) COMMENT 'Dataset identifier from config',
    DATABASE_NAME               VARCHAR(100) COMMENT 'Source database name',
    SCHEMA_NAME                 VARCHAR(100) COMMENT 'Source schema name',
    TABLE_NAME                  VARCHAR(100) COMMENT 'Source table name',
    COLUMN_NAME                 VARCHAR(100) COMMENT 'Column name (NULL for table-level checks)',
    
    -- Rule Information
    RULE_ID                     NUMBER COMMENT 'Rule ID from rule_master',
    RULE_NAME                   VARCHAR(100) COMMENT 'Rule name',
    RULE_TYPE                   VARCHAR(50) COMMENT 'COMPLETENESS | UNIQUENESS | VALIDITY | CONSISTENCY | FRESHNESS | VOLUME',
    RULE_LEVEL                  VARCHAR(20) COMMENT 'COLUMN | TABLE',
    
    -- Metrics (Atomic Level)
    TOTAL_RECORDS               NUMBER COMMENT 'Total records checked',
    VALID_RECORDS               NUMBER COMMENT 'Number of valid records',
    INVALID_RECORDS             NUMBER COMMENT 'Number of invalid records',
    NULL_RECORDS                NUMBER COMMENT 'Number of NULL records',
    DUPLICATE_RECORDS           NUMBER COMMENT 'Number of duplicate records',
    PASS_RATE                   NUMBER(10,2) COMMENT 'Pass rate percentage (0-100)',
    THRESHOLD                   NUMBER(10,2) COMMENT 'Configured threshold percentage',
    
    -- Status and Performance
    CHECK_STATUS                VARCHAR(50) COMMENT 'PASSED | FAILED | WARNING | ERROR | SKIPPED',
    EXECUTION_TIME_MS           NUMBER COMMENT 'Execution time in milliseconds',
    EXECUTION_CREDITS           NUMBER(10,6) COMMENT 'Snowflake credits consumed',
    
    -- Failure Details
    FAILURE_REASON              VARCHAR(500) COMMENT 'Reason for failure if check failed',
    SAMPLE_INVALID_VALUES       VARIANT COMMENT 'JSON array of sample invalid values',
    
    -- Metadata
    CREATED_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT FK_CHECK_RUN FOREIGN KEY (RUN_ID) REFERENCES DQ_RUN_CONTROL(RUN_ID)
)
COMMENT = 'Atomic-level metrics log for each individual DQ check execution';

-- Create clustering and indexes for performance
ALTER TABLE DQ_CHECK_RESULTS CLUSTER BY (CHECK_TIMESTAMP, TABLE_NAME);

-- ============================================================================
-- SECTION 4: FAILED RECORDS TABLE
-- ============================================================================

CREATE OR REPLACE TABLE DQ_FAILED_RECORDS (
    FAILURE_ID                  NUMBER AUTOINCREMENT PRIMARY KEY,
    CHECK_ID                    NUMBER NOT NULL,
    RUN_ID                      VARCHAR(100) NOT NULL,
    
    -- Source Information
    DATASET_ID                  VARCHAR(100) COMMENT 'Dataset identifier',
    TABLE_NAME                  VARCHAR(100) COMMENT 'Source table name',
    COLUMN_NAME                 VARCHAR(100) COMMENT 'Column name with failure',
    
    -- Rule Information
    RULE_NAME                   VARCHAR(100) COMMENT 'Rule that detected the failure',
    RULE_TYPE                   VARCHAR(50) COMMENT 'Type of rule',
    FAILURE_TYPE                VARCHAR(100) COMMENT 'NULL_VALUE | DUPLICATE_VALUE | INVALID_FORMAT | FK_VIOLATION | etc.',
    
    -- Failed Record Details (Sample - up to 100 records per check)
    FAILED_RECORD_PK            VARCHAR(500) COMMENT 'Primary key of failed record',
    FAILED_COLUMN_VALUE         VARCHAR(4000) COMMENT 'Actual value that failed',
    EXPECTED_PATTERN            VARCHAR(500) COMMENT 'Expected pattern or value',
    ACTUAL_VALUE_TYPE           VARCHAR(50) COMMENT 'Data type of actual value',
    
    -- Context (Additional columns for debugging)
    RELATED_COLUMNS             VARIANT COMMENT 'JSON object with related column values',
    ROW_CONTEXT                 VARIANT COMMENT 'JSON object with full row context',
    
    -- Failure Classification
    FAILURE_CATEGORY            VARCHAR(50) COMMENT 'Category of failure for grouping',
    IS_CRITICAL                 BOOLEAN DEFAULT FALSE COMMENT 'True if failure is critical',
    CAN_AUTO_REMEDIATE          BOOLEAN DEFAULT FALSE COMMENT 'True if can be auto-fixed',
    REMEDIATION_SUGGESTION      VARCHAR(1000) COMMENT 'Suggested remediation action',
    
    -- SQL for Investigation
    DEBUG_SQL                   VARCHAR(4000) COMMENT 'SQL query to investigate the failure',
    
    -- Timestamps
    DETECTED_TS                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT FK_FAILED_CHECK FOREIGN KEY (CHECK_ID) REFERENCES DQ_CHECK_RESULTS(CHECK_ID)
)
COMMENT = 'Sample failed records with details for debugging and analysis (max 100 per check)';

-- Create clustering for performance
ALTER TABLE DQ_FAILED_RECORDS CLUSTER BY (DETECTED_TS, TABLE_NAME);

-- ============================================================================
-- SECTION 5: COLUMN PROFILE TABLE
-- ============================================================================

CREATE OR REPLACE TABLE DQ_COLUMN_PROFILE (
    PROFILE_ID                  NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID                      VARCHAR(100) COMMENT 'Reference to run control',
    DATASET_ID                  VARCHAR(100) COMMENT 'Dataset identifier',
    DATABASE_NAME               VARCHAR(100) COMMENT 'Source database',
    SCHEMA_NAME                 VARCHAR(100) COMMENT 'Source schema',
    TABLE_NAME                  VARCHAR(100) COMMENT 'Source table',
    COLUMN_NAME                 VARCHAR(100) COMMENT 'Column being profiled',
    DATA_TYPE                   VARCHAR(50) COMMENT 'Column data type',
    
    -- Basic Statistics
    TOTAL_RECORDS               NUMBER COMMENT 'Total row count',
    NULL_COUNT                  NUMBER COMMENT 'Number of NULL values',
    DISTINCT_COUNT              NUMBER COMMENT 'Number of distinct values',
    
    -- Value Statistics
    MIN_VALUE                   VARCHAR COMMENT 'Minimum value (as string)',
    MAX_VALUE                   VARCHAR COMMENT 'Maximum value (as string)',
    AVG_VALUE                   NUMBER COMMENT 'Average value (for numeric columns)',
    STDDEV_VALUE                NUMBER COMMENT 'Standard deviation (for numeric columns)',
    
    -- Length Statistics
    MIN_LENGTH                  NUMBER COMMENT 'Minimum string length',
    MAX_LENGTH                  NUMBER COMMENT 'Maximum string length',
    AVG_LENGTH                  NUMBER COMMENT 'Average string length',
    
    -- Data Quality Indicators
    FUTURE_DATE_COUNT           NUMBER COMMENT 'Count of future dates (for date columns)',
    
    -- Metadata
    PROFILE_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Column-level profiling statistics for data quality analysis';

-- ============================================================================
-- SECTION 6: DAILY SUMMARY TABLE
-- ============================================================================

CREATE OR REPLACE TABLE DQ_DAILY_SUMMARY (
    SUMMARY_ID                  NUMBER AUTOINCREMENT PRIMARY KEY,
    SUMMARY_DATE                DATE COMMENT 'Date of summary',
    
    -- Dataset Information
    DATASET_ID                  VARCHAR(100) COMMENT 'Dataset identifier',
    DATABASE_NAME               VARCHAR(100) COMMENT 'Database name',
    SCHEMA_NAME                 VARCHAR(100) COMMENT 'Schema name',
    TABLE_NAME                  VARCHAR(100) COMMENT 'Table name',
    BUSINESS_DOMAIN             VARCHAR(100) COMMENT 'Business domain',
    
    -- Aggregated Metrics
    TOTAL_CHECKS                NUMBER COMMENT 'Total checks executed',
    PASSED_CHECKS               NUMBER COMMENT 'Checks that passed',
    FAILED_CHECKS               NUMBER COMMENT 'Checks that failed',
    WARNING_CHECKS              NUMBER COMMENT 'Checks with warnings',
    SKIPPED_CHECKS              NUMBER COMMENT 'Checks skipped',
    
    -- Score Calculation
    DQ_SCORE                    NUMBER(10,2) COMMENT 'Overall DQ score (0-100)',
    PREV_DAY_SCORE              NUMBER(10,2) COMMENT 'Previous day score for comparison',
    SCORE_TREND                 VARCHAR(20) COMMENT 'IMPROVING | STABLE | DEGRADING',
    
    -- Rule Type Breakdown
    COMPLETENESS_SCORE          NUMBER(10,2) COMMENT 'Completeness score (0-100)',
    UNIQUENESS_SCORE            NUMBER(10,2) COMMENT 'Uniqueness score (0-100)',
    VALIDITY_SCORE              NUMBER(10,2) COMMENT 'Validity score (0-100)',
    CONSISTENCY_SCORE           NUMBER(10,2) COMMENT 'Consistency score (0-100)',
    FRESHNESS_SCORE             NUMBER(10,2) COMMENT 'Freshness score (0-100)',
    VOLUME_SCORE                NUMBER(10,2) COMMENT 'Volume score (0-100)',
    
    -- Status Classification
    TRUST_LEVEL                 VARCHAR(20) COMMENT 'HIGH | MEDIUM | LOW',
    QUALITY_GRADE               VARCHAR(10) COMMENT 'A+ | A | B | C | D | F',
    IS_SLA_MET                  BOOLEAN COMMENT 'True if SLA requirements met',
    
    -- Volume Metrics
    TOTAL_RECORDS               NUMBER COMMENT 'Total records processed',
    FAILED_RECORDS_COUNT        NUMBER COMMENT 'Total failed records',
    FAILURE_RATE                NUMBER(10,2) COMMENT 'Failure rate percentage',
    
    -- Execution Metrics
    TOTAL_EXECUTION_TIME_SEC    NUMBER(10,2) COMMENT 'Total execution time in seconds',
    TOTAL_CREDITS_CONSUMED      NUMBER(10,6) COMMENT 'Total Snowflake credits consumed',
    
    -- Last Run Info
    LAST_RUN_ID                 VARCHAR(100) COMMENT 'Last run identifier',
    LAST_RUN_TS                 TIMESTAMP_NTZ COMMENT 'Last run timestamp',
    LAST_RUN_STATUS             VARCHAR(50) COMMENT 'Last run status',
    
    -- Metadata
    CREATED_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS                  TIMESTAMP_NTZ COMMENT 'Last update timestamp'
)
COMMENT = 'Daily aggregated DQ summary by dataset with trend analysis';

-- Create clustering for performance
ALTER TABLE DQ_DAILY_SUMMARY CLUSTER BY (SUMMARY_DATE, TABLE_NAME);

-- ============================================================================
-- SECTION 7: WEEKLY SUMMARY TABLE
-- ============================================================================

CREATE OR REPLACE TABLE DQ_WEEKLY_SUMMARY (
    WEEK_ID                     VARCHAR(10) COMMENT 'Format: 2026-W01',
    WEEK_START_DATE             DATE COMMENT 'Week start date (Monday)',
    WEEK_END_DATE               DATE COMMENT 'Week end date (Sunday)',
    DATASET_ID                  VARCHAR(100) COMMENT 'Dataset identifier',
    TABLE_NAME                  VARCHAR(100) COMMENT 'Table name',
    BUSINESS_DOMAIN             VARCHAR(100) COMMENT 'Business domain',
    
    -- Aggregated Metrics
    TOTAL_RUNS                  NUMBER COMMENT 'Total runs during week',
    TOTAL_CHECKS                NUMBER COMMENT 'Total checks executed',
    PASSED_CHECKS               NUMBER COMMENT 'Checks that passed',
    FAILED_CHECKS               NUMBER COMMENT 'Checks that failed',
    WARNING_CHECKS              NUMBER COMMENT 'Checks with warnings',
    
    -- Average Scores
    AVG_DQ_SCORE                NUMBER(10,2) COMMENT 'Average DQ score for week',
    AVG_COMPLETENESS_SCORE      NUMBER(10,2) COMMENT 'Average completeness score',
    AVG_UNIQUENESS_SCORE        NUMBER(10,2) COMMENT 'Average uniqueness score',
    AVG_VALIDITY_SCORE          NUMBER(10,2) COMMENT 'Average validity score',
    AVG_CONSISTENCY_SCORE       NUMBER(10,2) COMMENT 'Average consistency score',
    AVG_FRESHNESS_SCORE         NUMBER(10,2) COMMENT 'Average freshness score',
    AVG_VOLUME_SCORE            NUMBER(10,2) COMMENT 'Average volume score',
    
    -- Trend Indicators
    SCORE_TREND                 VARCHAR(20) COMMENT 'IMPROVING | STABLE | DEGRADING',
    SCORE_CHANGE_PCT            NUMBER(10,2) COMMENT 'Score change percentage vs previous week',
    PREV_WEEK_SCORE             NUMBER(10,2) COMMENT 'Previous week average score',
    
    -- Quality Classification
    QUALITY_GRADE               VARCHAR(10) COMMENT 'A+ | A | B | C | D | F',
    TRUST_LEVEL                 VARCHAR(20) COMMENT 'HIGH | MEDIUM | LOW',
    WEEKS_BELOW_SLA             NUMBER COMMENT 'Consecutive weeks below SLA',
    
    -- Volume Metrics
    TOTAL_RECORDS_CHECKED       NUMBER COMMENT 'Total records checked',
    TOTAL_INVALID_RECORDS       NUMBER COMMENT 'Total invalid records',
    AVG_FAILURE_RATE            NUMBER(10,2) COMMENT 'Average failure rate',
    
    -- Performance Metrics
    TOTAL_EXECUTION_TIME_SEC    NUMBER(10,2) COMMENT 'Total execution time',
    TOTAL_CREDITS_CONSUMED      NUMBER(10,6) COMMENT 'Total credits consumed',
    
    -- Metadata
    CREATED_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS                  TIMESTAMP_NTZ
)
COMMENT = 'Weekly aggregated DQ metrics for trend analysis';

-- ============================================================================
-- SECTION 8: MONTHLY SUMMARY TABLE
-- ============================================================================

CREATE OR REPLACE TABLE DQ_MONTHLY_SUMMARY (
    MONTH_ID                    VARCHAR(7) COMMENT 'Format: 2026-01',
    MONTH_START_DATE            DATE COMMENT 'First day of month',
    MONTH_END_DATE              DATE COMMENT 'Last day of month',
    DATASET_ID                  VARCHAR(100) COMMENT 'Dataset identifier',
    TABLE_NAME                  VARCHAR(100) COMMENT 'Table name',
    BUSINESS_DOMAIN             VARCHAR(100) COMMENT 'Business domain',
    
    -- Aggregated Metrics
    TOTAL_RUNS                  NUMBER COMMENT 'Total runs during month',
    TOTAL_CHECKS                NUMBER COMMENT 'Total checks executed',
    PASSED_CHECKS               NUMBER COMMENT 'Checks that passed',
    FAILED_CHECKS               NUMBER COMMENT 'Checks that failed',
    WARNING_CHECKS              NUMBER COMMENT 'Checks with warnings',
    
    -- Average Scores
    AVG_DQ_SCORE                NUMBER(10,2) COMMENT 'Average DQ score for month',
    AVG_COMPLETENESS_SCORE      NUMBER(10,2) COMMENT 'Average completeness score',
    AVG_UNIQUENESS_SCORE        NUMBER(10,2) COMMENT 'Average uniqueness score',
    AVG_VALIDITY_SCORE          NUMBER(10,2) COMMENT 'Average validity score',
    AVG_CONSISTENCY_SCORE       NUMBER(10,2) COMMENT 'Average consistency score',
    AVG_FRESHNESS_SCORE         NUMBER(10,2) COMMENT 'Average freshness score',
    AVG_VOLUME_SCORE            NUMBER(10,2) COMMENT 'Average volume score',
    
    -- Min/Max Scores (for volatility)
    MIN_DQ_SCORE                NUMBER(10,2) COMMENT 'Minimum DQ score during month',
    MAX_DQ_SCORE                NUMBER(10,2) COMMENT 'Maximum DQ score during month',
    SCORE_VOLATILITY            NUMBER(10,2) COMMENT 'Standard deviation of scores',
    
    -- Trend Indicators
    SCORE_TREND                 VARCHAR(20) COMMENT 'IMPROVING | STABLE | DEGRADING',
    SCORE_CHANGE_PCT            NUMBER(10,2) COMMENT 'Score change percentage vs previous month',
    PREV_MONTH_SCORE            NUMBER(10,2) COMMENT 'Previous month average score',
    
    -- Quality Classification
    QUALITY_GRADE               VARCHAR(10) COMMENT 'A+ | A | B | C | D | F',
    TRUST_LEVEL                 VARCHAR(20) COMMENT 'HIGH | MEDIUM | LOW',
    SLA_COMPLIANCE_PCT          NUMBER(10,2) COMMENT 'Percentage of days meeting SLA',
    
    -- Issue Tracking
    TOTAL_CRITICAL_ISSUES       NUMBER COMMENT 'Total critical issues detected',
    TOP_FAILURE_TYPES           VARCHAR(500) COMMENT 'Most common failure types',
    
    -- Volume Metrics
    TOTAL_RECORDS_CHECKED       NUMBER COMMENT 'Total records checked',
    TOTAL_INVALID_RECORDS       NUMBER COMMENT 'Total invalid records',
    AVG_FAILURE_RATE            NUMBER(10,2) COMMENT 'Average failure rate',
    
    -- Performance Metrics
    TOTAL_EXECUTION_TIME_SEC    NUMBER(10,2) COMMENT 'Total execution time',
    TOTAL_CREDITS_CONSUMED      NUMBER(10,6) COMMENT 'Total credits consumed',
    
    -- Metadata
    CREATED_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS                  TIMESTAMP_NTZ
)
COMMENT = 'Monthly aggregated DQ metrics for long-term trend analysis';

-- ============================================================================
-- SECTION 9: QUARTERLY SUMMARY TABLE
-- ============================================================================

CREATE OR REPLACE TABLE DQ_QUARTERLY_SUMMARY (
    QUARTER_ID                  VARCHAR(7) COMMENT 'Format: 2026-Q1',
    QUARTER_START_DATE          DATE COMMENT 'First day of quarter',
    QUARTER_END_DATE            DATE COMMENT 'Last day of quarter',
    DATASET_ID                  VARCHAR(100) COMMENT 'Dataset identifier',
    TABLE_NAME                  VARCHAR(100) COMMENT 'Table name',
    BUSINESS_DOMAIN             VARCHAR(100) COMMENT 'Business domain',
    
    -- Aggregated Metrics
    TOTAL_RUNS                  NUMBER COMMENT 'Total runs during quarter',
    TOTAL_CHECKS                NUMBER COMMENT 'Total checks executed',
    PASSED_CHECKS               NUMBER COMMENT 'Checks that passed',
    FAILED_CHECKS               NUMBER COMMENT 'Checks that failed',
    
    -- Average Scores
    AVG_DQ_SCORE                NUMBER(10,2) COMMENT 'Average DQ score for quarter',
    AVG_COMPLETENESS_SCORE      NUMBER(10,2) COMMENT 'Average completeness score',
    AVG_UNIQUENESS_SCORE        NUMBER(10,2) COMMENT 'Average uniqueness score',
    AVG_VALIDITY_SCORE          NUMBER(10,2) COMMENT 'Average validity score',
    AVG_CONSISTENCY_SCORE       NUMBER(10,2) COMMENT 'Average consistency score',
    
    -- Trend Analysis
    SCORE_TREND                 VARCHAR(20) COMMENT 'IMPROVING | STABLE | DEGRADING',
    SCORE_IMPROVEMENT_PCT       NUMBER(10,2) COMMENT 'Score improvement percentage',
    BEST_MONTH                  VARCHAR(7) COMMENT 'Best performing month',
    WORST_MONTH                 VARCHAR(7) COMMENT 'Worst performing month',
    
    -- Quality Metrics
    QUALITY_GRADE               VARCHAR(10) COMMENT 'A+ | A | B | C | D | F',
    SLA_COMPLIANCE_PCT          NUMBER(10,2) COMMENT 'Percentage of days meeting SLA',
    
    -- Metadata
    CREATED_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Quarterly aggregated DQ metrics for executive reporting';

-- ============================================================================
-- SECTION 10: DATA LINEAGE TABLE
-- ============================================================================

CREATE OR REPLACE TABLE DATA_LINEAGE (
    ID                          VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    UPSTREAM_DATABASE           VARCHAR(100) COMMENT 'Source database',
    UPSTREAM_SCHEMA             VARCHAR(100) COMMENT 'Source schema',
    UPSTREAM_TABLE              VARCHAR(100) COMMENT 'Source table',
    DOWNSTREAM_DATABASE         VARCHAR(100) COMMENT 'Target database',
    DOWNSTREAM_SCHEMA           VARCHAR(100) COMMENT 'Target schema',
    DOWNSTREAM_TABLE            VARCHAR(100) COMMENT 'Target table',
    LINEAGE_TYPE                VARCHAR(50) COMMENT 'DIRECT | TRANSFORMATION | AGGREGATION',
    TRANSFORMATION_LOGIC        VARCHAR(4000) COMMENT 'Description of transformation',
    CREATED_AT                  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Data lineage tracking for upstream and downstream dependencies';

-- ============================================================================
-- SECTION 11: ALLOWED VALUES CONFIG TABLE
-- ============================================================================

USE SCHEMA DQ_CONFIG;

CREATE OR REPLACE TABLE ALLOWED_VALUES_CONFIG (
    DATASET_ID                  VARCHAR(100) COMMENT 'Dataset identifier',
    COLUMN_NAME                 VARCHAR(100) COMMENT 'Column name',
    ALLOWED_VALUES              VARCHAR(2000) COMMENT 'Comma-separated allowed values in format: ''A'',''B'',''C''',
    IS_ACTIVE                   BOOLEAN DEFAULT TRUE,
    CREATED_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS                  TIMESTAMP_NTZ
)
COMMENT = 'Configuration for allowed values validation rules';

-- ============================================================================
-- SECTION 12: SAMPLE DATA LINEAGE (BRONZE → SILVER)
-- ============================================================================

USE SCHEMA DQ_METRICS;

-- Insert sample lineage for 3-layer architecture
INSERT INTO DATA_LINEAGE (UPSTREAM_DATABASE, UPSTREAM_SCHEMA, UPSTREAM_TABLE, DOWNSTREAM_DATABASE, DOWNSTREAM_SCHEMA, DOWNSTREAM_TABLE, LINEAGE_TYPE, TRANSFORMATION_LOGIC)
VALUES
    -- Bronze to Silver lineage
    ('BANKING_DW', 'BRONZE', 'STG_CUSTOMER', 'BANKING_DW', 'SILVER', 'CUSTOMER', 'TRANSFORMATION', 'Type conversion, validation, DQ scoring'),
    ('BANKING_DW', 'BRONZE', 'STG_ACCOUNT', 'BANKING_DW', 'SILVER', 'ACCOUNT', 'TRANSFORMATION', 'Type conversion, validation, FK enforcement'),
    ('BANKING_DW', 'BRONZE', 'STG_TRANSACTION', 'BANKING_DW', 'SILVER', 'TRANSACTION', 'TRANSFORMATION', 'Type conversion, validation, amount validation'),
    ('BANKING_DW', 'BRONZE', 'STG_DAILY_BALANCE', 'BANKING_DW', 'SILVER', 'DAILY_BALANCE', 'TRANSFORMATION', 'Type conversion, validation'),
    ('BANKING_DW', 'BRONZE', 'STG_FX_RATE', 'BANKING_DW', 'SILVER', 'FX_RATE', 'TRANSFORMATION', 'Type conversion, validation'),
    
    -- Silver to Gold lineage
    ('BANKING_DW', 'SILVER', 'CUSTOMER', 'BANKING_DW', 'GOLD', 'VW_CUSTOMER_360', 'AGGREGATION', 'Customer 360 view with account and transaction metrics'),
    ('BANKING_DW', 'SILVER', 'ACCOUNT', 'BANKING_DW', 'GOLD', 'VW_CUSTOMER_360', 'AGGREGATION', 'Account aggregation for customer 360'),
    ('BANKING_DW', 'SILVER', 'TRANSACTION', 'BANKING_DW', 'GOLD', 'VW_CUSTOMER_360', 'AGGREGATION', 'Transaction aggregation for customer 360');

-- ============================================================================
-- SECTION 13: VERIFICATION
-- ============================================================================

-- Verify all tables created
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES,
    COMMENT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'DQ_METRICS'
AND TABLE_NAME IN (
    'DQ_RUN_CONTROL', 'DQ_CHECK_RESULTS', 'DQ_FAILED_RECORDS', 
    'DQ_COLUMN_PROFILE', 'DQ_DAILY_SUMMARY', 'DQ_WEEKLY_SUMMARY',
    'DQ_MONTHLY_SUMMARY', 'DQ_QUARTERLY_SUMMARY', 'DATA_LINEAGE'
)
ORDER BY TABLE_NAME;

-- Verify column sizes for critical tables
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'DQ_METRICS'
AND TABLE_NAME = 'DQ_RUN_CONTROL'
AND COLUMN_NAME IN ('RUN_STATUS', 'RUN_ID', 'ERROR_MESSAGE')
ORDER BY ORDINAL_POSITION;

-- Verify lineage data
SELECT 
    UPSTREAM_DATABASE || '.' || UPSTREAM_SCHEMA || '.' || UPSTREAM_TABLE AS UPSTREAM,
    DOWNSTREAM_DATABASE || '.' || DOWNSTREAM_SCHEMA || '.' || DOWNSTREAM_TABLE AS DOWNSTREAM,
    LINEAGE_TYPE,
    TRANSFORMATION_LOGIC
FROM DATA_LINEAGE
ORDER BY UPSTREAM, DOWNSTREAM;

-- ============================================================================
-- METRICS SETUP COMPLETE
-- ============================================================================

SELECT '=== METRICS TABLES SETUP COMPLETE ===' AS STATUS;
SELECT 'Total Metrics Tables: ' || COUNT(*) AS INFO 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'DQ_METRICS';

-- ============================================================================
-- NEXT STEPS
-- ============================================================================
-- 1. Run a profiling job to populate metrics
-- 2. Check DQ_RUN_CONTROL for execution status
-- 3. Review DQ_CHECK_RESULTS for detailed check outcomes
-- 4. Analyze DQ_DAILY_SUMMARY for trends
-- ============================================================================
