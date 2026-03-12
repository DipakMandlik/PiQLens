-- ============================================================================
-- AI-DRIVEN OBSERVABILITY & INSIGHTS - PRODUCTION VERSION
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Unified observability layer for AI-driven insights and monitoring
-- Prerequisites: All previous setup scripts executed (01-10)
-- Version: 1.0.0
-- ============================================================================
-- 
-- This file contains:
-- 1. DB_METRICS schema for observability
-- 2. DQ_METRICS - Unified fact table for all observable metrics
-- 3. DQ_AI_INSIGHTS - AI-generated insights with traceability
-- 4. V_SCHEMA_REGISTRY - Schema introspection for AI SQL generation
-- 5. V_UNIFIED_METRICS - Materialized view combining all metrics
-- 6. SP_INGEST_METRIC - Safe metric ingestion procedure
-- 7. SP_BACKFILL_METRICS - Populate from existing DQ data
-- 8. SP_GENERATE_AI_INSIGHTS - Generate insights from metrics
-- 
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- ============================================================================
-- SCHEMA CREATION
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS DB_METRICS
    COMMENT = 'AI-Driven Observability Metrics and Insights for Pi-Qualytics';

USE SCHEMA DB_METRICS;

-- ============================================================================
-- TABLE 1: UNIFIED METRICS FACT TABLE
-- ============================================================================
-- Purpose: Single source of truth for all observable metrics
-- Design: Star schema fact table with dimensional attributes
-- ============================================================================

CREATE TABLE IF NOT EXISTS DQ_METRICS (
    METRIC_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    ASSET_ID VARCHAR(511) NOT NULL COMMENT 'Fully qualified: DATABASE.SCHEMA.TABLE',
    COLUMN_NAME VARCHAR(255) COMMENT 'Optional: Column name for column-level metrics',
    METRIC_NAME VARCHAR(100) NOT NULL COMMENT 'Standardized: row_count, null_rate, freshness_hours, dq_score, etc.',
    METRIC_VALUE FLOAT COMMENT 'Numeric value of the metric',
    METRIC_TEXT VARCHAR(1024) COMMENT 'Text value for categorical metrics (status, grade, etc.)',
    METRIC_TIME TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Measurement timestamp',
    SOURCE_SYSTEM VARCHAR(50) DEFAULT 'PI_QUALYTICS' COMMENT 'Origin: PI_QUALYTICS, PROFILING, CUSTOM_CHECK, etc.',
    RUN_ID VARCHAR(100) COMMENT 'Reference to DQ_RUN_CONTROL.RUN_ID for traceability',
    TAGS VARIANT COMMENT 'JSON: Additional context {rule_type, severity, business_domain, etc.}',
    CREATED_TS TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Unified fact table for all observability metrics - enables AI-driven insights';

-- Clustering for performance
ALTER TABLE DQ_METRICS CLUSTER BY (ASSET_ID, METRIC_TIME);

-- ============================================================================
-- TABLE 2: AI INSIGHTS
-- ============================================================================
-- Purpose: Store validated, AI-generated insights with full traceability
-- Design: Immutable ledger with severity levels and actionability flags
-- ============================================================================

CREATE TABLE IF NOT EXISTS DQ_AI_INSIGHTS (
    INSIGHT_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    ASSET_ID VARCHAR(511) NOT NULL COMMENT 'Target asset (DATABASE.SCHEMA.TABLE)',
    INSIGHT_TYPE VARCHAR(50) NOT NULL COMMENT 'ANOMALY | TREND | SCHEMA_CHANGE | IMPACT | FRESHNESS | QUALITY | VOLUME',
    SUMMARY VARCHAR(500) NOT NULL COMMENT 'Executive summary - one-line description',
    DETAILS VARIANT COMMENT 'JSON: {bullets: [...], evidence: {...}, source_metrics: [...], recommendations: [...]}',
    SEVERITY VARCHAR(20) DEFAULT 'INFO' COMMENT 'INFO | WARNING | CRITICAL',
    IS_ACTIONABLE BOOLEAN DEFAULT FALSE COMMENT 'True if user action is recommended',
    CONFIDENCE_SCORE FLOAT COMMENT '0-100: AI confidence in this insight',
    SOURCE_METRICS VARIANT COMMENT 'JSON array of METRIC_IDs used to generate this insight',
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    EXPIRES_AT TIMESTAMP_TZ COMMENT 'Auto-expire old insights (default: 30 days)',
    ACKNOWLEDGED_BY VARCHAR(100) COMMENT 'User who acknowledged this insight',
    ACKNOWLEDGED_AT TIMESTAMP_TZ COMMENT 'When insight was acknowledged'
)
COMMENT = 'Immutable ledger of AI-generated, validated insights with full traceability';

-- ============================================================================
-- VIEW 1: SCHEMA REGISTRY
-- ============================================================================
-- Purpose: Antigravity AI reads this BEFORE generating SQL
-- Critical: Must include ALL relevant schemas for accurate SQL generation
-- ============================================================================

CREATE OR REPLACE VIEW V_SCHEMA_REGISTRY AS
SELECT
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA AS SCHEMA_NAME,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    ORDINAL_POSITION,
    COMMENT AS COLUMN_COMMENT
FROM DATA_QUALITY_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA IN ('DQ_CONFIG', 'DQ_METRICS', 'DQ_ENGINE', 'DB_METRICS', 'BRONZE', 'SILVER', 'GOLD')
  AND TABLE_CATALOG = 'DATA_QUALITY_DB'
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;

COMMENT ON VIEW V_SCHEMA_REGISTRY IS
    'Antigravity AI introspection view - READ THIS BEFORE GENERATING SQL to avoid column name errors';

-- ============================================================================
-- VIEW 2: UNIFIED METRICS (Materialized from existing DQ tables)
-- ============================================================================
-- Purpose: Combine metrics from all sources into DQ_METRICS format
-- Design: UNION ALL from DQ_CHECK_RESULTS, DQ_COLUMN_PROFILE, DQ_DAILY_SUMMARY
-- ============================================================================

CREATE OR REPLACE VIEW V_UNIFIED_METRICS AS
-- Metrics from DQ_CHECK_RESULTS (Quality Checks)
SELECT
    UUID_STRING() AS METRIC_ID,
    DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS ASSET_ID,
    COLUMN_NAME,
    'dq_pass_rate' AS METRIC_NAME,
    PASS_RATE AS METRIC_VALUE,
    CHECK_STATUS AS METRIC_TEXT,
    CHECK_TIMESTAMP AS METRIC_TIME,
    'DQ_CHECK' AS SOURCE_SYSTEM,
    RUN_ID,
    OBJECT_CONSTRUCT(
        'rule_type', RULE_TYPE,
        'rule_name', RULE_NAME,
        'threshold', THRESHOLD,
        'total_records', TOTAL_RECORDS,
        'invalid_records', INVALID_RECORDS
    ) AS TAGS,
    CREATED_TS
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
WHERE CHECK_TIMESTAMP >= DATEADD(day, -30, CURRENT_TIMESTAMP())

UNION ALL

-- Metrics from DQ_COLUMN_PROFILE (Profiling)
SELECT
    UUID_STRING() AS METRIC_ID,
    DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS ASSET_ID,
    COLUMN_NAME,
    'null_rate' AS METRIC_NAME,
    (NULL_COUNT * 100.0 / NULLIF(TOTAL_RECORDS, 0)) AS METRIC_VALUE,
    NULL AS METRIC_TEXT,
    PROFILE_TS AS METRIC_TIME,
    'PROFILING' AS SOURCE_SYSTEM,
    RUN_ID,
    OBJECT_CONSTRUCT(
        'data_type', DATA_TYPE,
        'distinct_count', DISTINCT_COUNT,
        'total_records', TOTAL_RECORDS
    ) AS TAGS,
    PROFILE_TS AS CREATED_TS
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE
WHERE PROFILE_TS >= DATEADD(day, -30, CURRENT_TIMESTAMP())

UNION ALL

-- Metrics from DQ_DAILY_SUMMARY (Aggregated Scores)
SELECT
    UUID_STRING() AS METRIC_ID,
    DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS ASSET_ID,
    NULL AS COLUMN_NAME,
    'dq_score' AS METRIC_NAME,
    DQ_SCORE AS METRIC_VALUE,
    QUALITY_GRADE AS METRIC_TEXT,
    SUMMARY_DATE AS METRIC_TIME,
    'DAILY_SUMMARY' AS SOURCE_SYSTEM,
    LAST_RUN_ID AS RUN_ID,
    OBJECT_CONSTRUCT(
        'business_domain', BUSINESS_DOMAIN,
        'trust_level', TRUST_LEVEL,
        'is_sla_met', IS_SLA_MET,
        'total_checks', TOTAL_CHECKS,
        'failed_checks', FAILED_CHECKS
    ) AS TAGS,
    CREATED_TS
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
WHERE SUMMARY_DATE >= DATEADD(day, -30, CURRENT_TIMESTAMP());

COMMENT ON VIEW V_UNIFIED_METRICS IS
    'Unified view of all metrics from DQ_CHECK_RESULTS, DQ_COLUMN_PROFILE, and DQ_DAILY_SUMMARY';

-- ============================================================================
-- PROCEDURE 1: METRIC INGESTION
-- ============================================================================
-- Purpose: Safely insert new metrics with validation
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_INGEST_METRIC(
    P_ASSET_ID VARCHAR,
    P_COLUMN_NAME VARCHAR,
    P_METRIC_NAME VARCHAR,
    P_METRIC_VALUE FLOAT,
    P_METRIC_TEXT VARCHAR,
    P_SOURCE_SYSTEM VARCHAR,
    P_RUN_ID VARCHAR,
    P_TAGS VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Validate required fields
    IF (P_ASSET_ID IS NULL OR P_METRIC_NAME IS NULL) THEN
        RETURN 'ERROR: ASSET_ID and METRIC_NAME are required';
    END IF;
    
    INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_METRICS (
        ASSET_ID,
        COLUMN_NAME,
        METRIC_NAME,
        METRIC_VALUE,
        METRIC_TEXT,
        SOURCE_SYSTEM,
        RUN_ID,
        TAGS
    )
    VALUES (
        UPPER(:P_ASSET_ID),
        :P_COLUMN_NAME,
        UPPER(:P_METRIC_NAME),
        :P_METRIC_VALUE,
        :P_METRIC_TEXT,
        COALESCE(:P_SOURCE_SYSTEM, 'PI_QUALYTICS'),
        :P_RUN_ID,
        :P_TAGS
    );
    
    RETURN 'SUCCESS: Metric ingested with ID ' || (SELECT MAX(METRIC_ID) FROM DATA_QUALITY_DB.DB_METRICS.DQ_METRICS);
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- PROCEDURE 2: BACKFILL METRICS
-- ============================================================================
-- Purpose: Populate DQ_METRICS from existing DQ tables (one-time or periodic)
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_BACKFILL_METRICS(
    P_DAYS_BACK INTEGER DEFAULT 30
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    LET rows_inserted INTEGER DEFAULT 0;
    
    -- Clear existing backfilled data to avoid duplicates
    DELETE FROM DATA_QUALITY_DB.DB_METRICS.DQ_METRICS
    WHERE METRIC_TIME >= DATEADD(day, -:P_DAYS_BACK, CURRENT_TIMESTAMP());
    
    -- Insert from V_UNIFIED_METRICS
    INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_METRICS (
        ASSET_ID, COLUMN_NAME, METRIC_NAME, METRIC_VALUE, METRIC_TEXT,
        METRIC_TIME, SOURCE_SYSTEM, RUN_ID, TAGS
    )
    SELECT
        ASSET_ID, COLUMN_NAME, METRIC_NAME, METRIC_VALUE, METRIC_TEXT,
        METRIC_TIME, SOURCE_SYSTEM, RUN_ID, TAGS
    FROM DATA_QUALITY_DB.DB_METRICS.V_UNIFIED_METRICS;
    
    rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Backfilled ' || rows_inserted || ' metrics from last ' || :P_DAYS_BACK || ' days';
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- PROCEDURE 3: GENERATE AI INSIGHTS
-- ============================================================================
-- Purpose: Analyze metrics and generate actionable insights
-- Design: Rule-based insights (can be enhanced with ML models)
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_GENERATE_AI_INSIGHTS(
    P_ASSET_ID VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
from datetime import datetime, timedelta
import json

def main(session, p_asset_id):
    """
    Generate insights from metrics using rule-based analysis
    Future: Can integrate with ML models for advanced anomaly detection
    """
    
    insights_generated = 0
    
    # Build asset filter
    asset_filter = f"WHERE ASSET_ID = '{p_asset_id}'" if p_asset_id else ""
    
    try:
        # 1. ANOMALY DETECTION: Sudden drop in DQ scores
        anomaly_query = f"""
            WITH recent_scores AS (
                SELECT 
                    ASSET_ID,
                    METRIC_VALUE AS DQ_SCORE,
                    METRIC_TIME,
                    LAG(METRIC_VALUE) OVER (PARTITION BY ASSET_ID ORDER BY METRIC_TIME) AS PREV_SCORE
                FROM DATA_QUALITY_DB.DB_METRICS.DQ_METRICS
                WHERE METRIC_NAME = 'DQ_SCORE'
                  AND METRIC_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                {asset_filter}
            )
            SELECT 
                ASSET_ID,
                DQ_SCORE,
                PREV_SCORE,
                (PREV_SCORE - DQ_SCORE) AS SCORE_DROP,
                METRIC_TIME
            FROM recent_scores
            WHERE PREV_SCORE IS NOT NULL
              AND (PREV_SCORE - DQ_SCORE) > 10
            ORDER BY SCORE_DROP DESC
            LIMIT 5
        """
        
        anomalies = session.sql(anomaly_query).collect()
        
        for row in anomalies:
            asset = row['ASSET_ID']
            drop = round(row['SCORE_DROP'], 2)
            current = round(row['DQ_SCORE'], 2)
            previous = round(row['PREV_SCORE'], 2)
            
            summary = f"DQ Score dropped {drop}% (from {previous}% to {current}%)"
            details = {
                "bullets": [
                    f"Previous score: {previous}%",
                    f"Current score: {current}%",
                    f"Drop: {drop}%",
                    "Investigate recent data changes or rule updates"
                ],
                "evidence": {
                    "metric_name": "dq_score",
                    "previous_value": previous,
                    "current_value": current,
                    "change": -drop
                },
                "recommendations": [
                    "Review failed checks in DQ_CHECK_RESULTS",
                    "Check for schema changes or data pipeline issues",
                    "Verify data source quality"
                ]
            }
            
            session.sql(f"""
                INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_AI_INSIGHTS (
                    ASSET_ID, INSIGHT_TYPE, SUMMARY, DETAILS, SEVERITY,
                    IS_ACTIONABLE, CONFIDENCE_SCORE, EXPIRES_AT
                ) VALUES (
                    '{asset}',
                    'ANOMALY',
                    '{summary}',
                    PARSE_JSON('{json.dumps(details)}'),
                    'WARNING',
                    TRUE,
                    85.0,
                    DATEADD(day, 30, CURRENT_TIMESTAMP())
                )
            """).collect()
            
            insights_generated += 1
        
        # 2. TREND DETECTION: Consistent quality improvement/degradation
        trend_query = f"""
            WITH score_trend AS (
                SELECT 
                    ASSET_ID,
                    AVG(CASE WHEN METRIC_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP()) 
                        THEN METRIC_VALUE END) AS RECENT_AVG,
                    AVG(CASE WHEN METRIC_TIME < DATEADD(day, -3, CURRENT_TIMESTAMP()) 
                        AND METRIC_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                        THEN METRIC_VALUE END) AS OLDER_AVG
                FROM DATA_QUALITY_DB.DB_METRICS.DQ_METRICS
                WHERE METRIC_NAME = 'DQ_SCORE'
                  AND METRIC_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                {asset_filter}
                GROUP BY ASSET_ID
            )
            SELECT 
                ASSET_ID,
                RECENT_AVG,
                OLDER_AVG,
                (RECENT_AVG - OLDER_AVG) AS TREND
            FROM score_trend
            WHERE RECENT_AVG IS NOT NULL
              AND OLDER_AVG IS NOT NULL
              AND ABS(RECENT_AVG - OLDER_AVG) > 5
            ORDER BY ABS(TREND) DESC
            LIMIT 5
        """
        
        trends = session.sql(trend_query).collect()
        
        for row in trends:
            asset = row['ASSET_ID']
            trend = round(row['TREND'], 2)
            recent = round(row['RECENT_AVG'], 2)
            older = round(row['OLDER_AVG'], 2)
            
            if trend > 0:
                summary = f"Quality improving: +{trend}% over last week"
                severity = "INFO"
            else:
                summary = f"Quality degrading: {trend}% over last week"
                severity = "WARNING"
            
            details = {
                "bullets": [
                    f"Recent average (3 days): {recent}%",
                    f"Previous average (4-7 days ago): {older}%",
                    f"Trend: {'+' if trend > 0 else ''}{trend}%"
                ],
                "evidence": {
                    "metric_name": "dq_score",
                    "recent_avg": recent,
                    "older_avg": older,
                    "trend": trend
                }
            }
            
            session.sql(f"""
                INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_AI_INSIGHTS (
                    ASSET_ID, INSIGHT_TYPE, SUMMARY, DETAILS, SEVERITY,
                    IS_ACTIONABLE, CONFIDENCE_SCORE, EXPIRES_AT
                ) VALUES (
                    '{asset}',
                    'TREND',
                    '{summary}',
                    PARSE_JSON('{json.dumps(details)}'),
                    '{severity}',
                    {str(trend < 0).upper()},
                    75.0,
                    DATEADD(day, 30, CURRENT_TIMESTAMP())
                )
            """).collect()
            
            insights_generated += 1
        
        # 3. FRESHNESS ALERTS: Stale data detection
        # (Add more insight types as needed)
        
        return f"SUCCESS: Generated {insights_generated} insights"
        
    except Exception as e:
        return f"ERROR: {str(e)}"
$$;



CREATE OR REPLACE PROCEDURE DATA_QUALITY_DB.DQ_ENGINE.SP_GENERATE_PLATFORM_REPORT(
    P_REPORT_DATE DATE,
    P_FORMAT STRING,
    P_SCOPE STRING,
    P_INCLUDE_INSIGHTS BOOLEAN,
    P_INCLUDE_TREND BOOLEAN
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    V_REPORT_ID VARCHAR;
    V_STAGE_PATH VARCHAR;
    V_FILE_PATH VARCHAR;
    V_TIMESTAMP TIMESTAMP_NTZ;
    V_METADATA VARIANT;
    V_JSON_OUTPUT VARIANT;
    V_TOTAL_ASSETS INTEGER;
    V_TOTAL_METRICS INTEGER;
    V_OVERALL_SCORE FLOAT;
    V_SQL STRING;
    res RESULTSET;
BEGIN
    V_REPORT_ID := UUID_STRING();
    V_TIMESTAMP := CURRENT_TIMESTAMP();
    
    -- Gather basic header metrics from DB_METRICS
    -- Defaulting to 0/Nul if empty
    SELECT COUNT(DISTINCT ASSET_ID), COUNT(*) 
    INTO :V_TOTAL_ASSETS, :V_TOTAL_METRICS
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_METRICS
    WHERE METRIC_TIME::DATE = :P_REPORT_DATE;
    
    SELECT AVG(METRIC_VALUE)
    INTO :V_OVERALL_SCORE
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_METRICS
    WHERE METRIC_TIME::DATE = :P_REPORT_DATE 
      AND METRIC_NAME = 'DQ_SCORE';

    V_OVERALL_SCORE := COALESCE(V_OVERALL_SCORE, 0);
    
    IF (UPPER(P_FORMAT) = 'JSON') THEN
        V_FILE_PATH := 'reports/' || TO_CHAR(:P_REPORT_DATE, 'YYYY/MM/DD') || '/' || :V_REPORT_ID || '.json';
        
        -- Build complex JSON
        V_SQL := '
            CREATE OR REPLACE TEMPORARY TABLE DATA_QUALITY_DB.DQ_ENGINE.TMP_JSON_REPORT AS
            WITH header AS (
                SELECT OBJECT_CONSTRUCT(
                    ''Platform'', ''Pi_QLense'',
                    ''Report Type'', ''' || :P_SCOPE || ''',
                    ''Report Date'', ''' || TO_CHAR(:P_REPORT_DATE, 'YYYY-MM-DD') || ''',
                    ''Generated At'', ''' || TO_CHAR(:V_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') || ''',
                    ''Generated By'', CURRENT_USER(),
                    ''Total Assets'', ' || :V_TOTAL_ASSETS || ',
                    ''Total Metrics'', ' || :V_TOTAL_METRICS || ',
                    ''Overall DQ Score'', ' || :V_OVERALL_SCORE || '
                ) as header_meta
            ),
            exec_summary AS (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                    ''Asset'', ASSET_ID,
                    ''Metric'', METRIC_NAME,
                    ''Value'', METRIC_VALUE
                )) as summary_arr
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_METRICS
                WHERE METRIC_TIME::DATE = ''' || :P_REPORT_DATE || '''::DATE
                  AND METRIC_NAME IN (''DQ_SCORE'',''FAILURE_RATE'',''TOTAL_FAILED'',''TOTAL_CHECKS'')
            ),
            insights AS (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                    ''Asset'', ASSET_ID,
                    ''Type'', INSIGHT_TYPE,
                    ''Summary'', SUMMARY,
                    ''Severity'', SEVERITY
                )) as insights_arr
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_AI_INSIGHTS
                WHERE IS_ACTIONABLE = TRUE AND DATE_TRUNC(''DAY'', CREATED_AT) = ''' || :P_REPORT_DATE || '''::DATE
            )
            SELECT OBJECT_CONSTRUCT(
                ''Header'', (SELECT header_meta FROM header),
                ''ExecSummary'', COALESCE((SELECT summary_arr FROM exec_summary), ARRAY_CONSTRUCT()),
                ''Insights'', COALESCE((SELECT insights_arr FROM insights), ARRAY_CONSTRUCT())
            ) as json_body
        ';
        
        EXECUTE IMMEDIATE V_SQL;
        
        -- Export to stage
        V_SQL := 'COPY INTO @DATA_QUALITY_DB.DQ_CONFIG.PI_QLENSE_REPORT_STAGE/' || :V_FILE_PATH || ' 
                  FROM DATA_QUALITY_DB.DQ_ENGINE.TMP_JSON_REPORT 
                  FILE_FORMAT = (TYPE = JSON COMPRESSION=NONE)
                  OVERWRITE = TRUE
                  SINGLE = TRUE';
        EXECUTE IMMEDIATE V_SQL;
        
    ELSE
        -- CSV Format (Multiple files representing sections in a directory)
        V_STAGE_PATH := 'reports/' || TO_CHAR(:P_REPORT_DATE, 'YYYY/MM/DD') || '/' || :V_REPORT_ID || '/';
        V_FILE_PATH := V_STAGE_PATH; -- Base path for dir
        
        -- Section 2: Exec Summary
        V_SQL := 'COPY INTO @DATA_QUALITY_DB.DQ_CONFIG.PI_QLENSE_REPORT_STAGE/' || V_STAGE_PATH || 'exec_summary.csv ' ||
                 'FROM (
                     SELECT ASSET_ID, METRIC_NAME, METRIC_VALUE 
                     FROM DATA_QUALITY_DB.DQ_METRICS.DQ_METRICS 
                     WHERE METRIC_TIME::DATE = ''' || :P_REPORT_DATE || '''::DATE
                       AND METRIC_NAME IN (''DQ_SCORE'',''FAILURE_RATE'',''TOTAL_FAILED'',''TOTAL_CHECKS'')
                 ) ' ||
                 'FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY=''"'' COMPRESSION=NONE) HEADER = TRUE OVERWRITE = TRUE SINGLE = TRUE';
        EXECUTE IMMEDIATE V_SQL;
        
        -- Section 3: Metric Matrix
        V_SQL := 'COPY INTO @DATA_QUALITY_DB.DQ_CONFIG.PI_QLENSE_REPORT_STAGE/' || V_STAGE_PATH || 'metric_matrix.csv ' ||
                 'FROM (
                     SELECT ASSET_ID, METRIC_NAME, METRIC_VALUE, CREATED_AT 
                     FROM DATA_QUALITY_DB.DQ_METRICS.DQ_METRICS 
                     WHERE METRIC_TIME::DATE = ''' || :P_REPORT_DATE || '''::DATE
                 ) ' ||
                 'FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY=''"'' COMPRESSION=NONE) HEADER = TRUE OVERWRITE = TRUE SINGLE = TRUE';
        EXECUTE IMMEDIATE V_SQL;
        
        IF (P_INCLUDE_INSIGHTS = TRUE) THEN
            V_SQL := 'COPY INTO @DATA_QUALITY_DB.DQ_CONFIG.PI_QLENSE_REPORT_STAGE/' || V_STAGE_PATH || 'insights.csv ' ||
                     'FROM (
                         SELECT ASSET_ID, INSIGHT_TYPE, SUMMARY, SEVERITY 
                         FROM DATA_QUALITY_DB.DQ_METRICS.DQ_AI_INSIGHTS 
                         WHERE IS_ACTIONABLE = TRUE AND DATE_TRUNC(''DAY'', CREATED_AT) = ''' || :P_REPORT_DATE || '''::DATE
                     ) ' ||
                     'FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY=''"'' COMPRESSION=NONE) HEADER = TRUE OVERWRITE = TRUE SINGLE = TRUE';
            EXECUTE IMMEDIATE V_SQL;
        END IF;
    END IF;

    -- Insert Metadata Tracking row
    INSERT INTO DATA_QUALITY_DB.DQ_CONFIG.DQ_REPORTS 
    (REPORT_ID, REPORT_TYPE, SCOPE, REPORT_DATE, GENERATED_AT, GENERATED_BY, FORMAT, FILE_PATH, STATUS, METADATA, DOWNLOAD_COUNT)
    SELECT 
        :V_REPORT_ID, 
        'PLATFORM', 
        :P_SCOPE, 
        :P_REPORT_DATE, 
        :V_TIMESTAMP, 
        CURRENT_USER(), 
        UPPER(:P_FORMAT), 
        :V_FILE_PATH, 
        'COMPLETED',
        :V_METADATA,
        0;
    
    RETURN OBJECT_CONSTRUCT('report_id', :V_REPORT_ID, 'file_path', :V_FILE_PATH)::VARCHAR;
EXCEPTION
    WHEN OTHER THEN
        V_SQL := object_construct('Error', sqlerrm, 'Code', sqlcode, 'State', sqlstate)::VARCHAR;
        
        SELECT OBJECT_CONSTRUCT('error', :V_SQL) INTO :V_METADATA;

        -- Insert Failure Tracking row
        INSERT INTO DATA_QUALITY_DB.DQ_CONFIG.DQ_REPORTS 
        (REPORT_ID, REPORT_TYPE, SCOPE, REPORT_DATE, GENERATED_AT, GENERATED_BY, FORMAT, STATUS, METADATA)
        SELECT 
            :V_REPORT_ID, 
            'PLATFORM', 
            :P_SCOPE, 
            :P_REPORT_DATE, 
            :V_TIMESTAMP, 
            CURRENT_USER(), 
            UPPER(:P_FORMAT), 
            'FAILED',
            :V_METADATA;
        
        RETURN 'Error: ' || sqlerrm;
END;
$$;


-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

GRANT USAGE ON SCHEMA DB_METRICS TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA DB_METRICS TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL VIEWS IN SCHEMA DB_METRICS TO ROLE ACCOUNTADMIN;
GRANT INSERT, UPDATE, DELETE ON TABLE DQ_METRICS TO ROLE ACCOUNTADMIN;
GRANT INSERT, UPDATE ON TABLE DQ_AI_INSIGHTS TO ROLE ACCOUNTADMIN;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA DB_METRICS TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Verify schema and objects
SHOW SCHEMAS IN DATABASE DATA_QUALITY_DB;
SHOW TABLES IN SCHEMA DATA_QUALITY_DB.DB_METRICS;
SHOW VIEWS IN SCHEMA DATA_QUALITY_DB.DB_METRICS;
SHOW PROCEDURES IN SCHEMA DATA_QUALITY_DB.DB_METRICS;

-- Test schema registry
SELECT * FROM DATA_QUALITY_DB.DB_METRICS.V_SCHEMA_REGISTRY LIMIT 10;

-- ============================================================================
-- INITIAL SETUP & TESTING
-- ============================================================================

-- 1. Backfill metrics from existing data (last 30 days)
-- CALL DATA_QUALITY_DB.DB_METRICS.SP_BACKFILL_METRICS(30);

-- 2. Generate initial insights
-- CALL DATA_QUALITY_DB.DB_METRICS.SP_GENERATE_AI_INSIGHTS(NULL);

-- 3. View generated insights
-- SELECT * FROM DATA_QUALITY_DB.DB_METRICS.DQ_AI_INSIGHTS ORDER BY CREATED_AT DESC LIMIT 10;

-- 4. View unified metrics
-- SELECT * FROM DATA_QUALITY_DB.DB_METRICS.V_UNIFIED_METRICS ORDER BY METRIC_TIME DESC LIMIT 20;

-- 5. Test metric ingestion
-- CALL DATA_QUALITY_DB.DB_METRICS.SP_INGEST_METRIC(
--     'BANKING_DW.BRONZE.STG_CUSTOMER',
--     'EMAIL',
--     'VALIDITY_RATE',
--     95.5,
--     'PASSED',
--     'CUSTOM_CHECK',
--     'DQ_RUN_20260122_101500',
--     PARSE_JSON('{"rule_type": "VALIDITY", "threshold": 95}')
-- );

-- ============================================================================
-- OBSERVABILITY SYSTEM SETUP COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Run backfill: CALL SP_BACKFILL_METRICS(30);
-- 2. Generate insights: CALL SP_GENERATE_AI_INSIGHTS(NULL);
-- 3. Integrate with Antigravity UI for visualization
-- 4. Set up periodic insight generation (daily task)
-- 5. Configure alerts for CRITICAL insights
-- ============================================================================



-- =====================================================
-- Pi-Qualytics Reporting Tables
-- =====================================================
-- Purpose: Store report metadata and scheduled report configurations
-- Created: 2026-01-27
-- =====================================================

USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DB_METRICS;

-- =====================================================
-- 1. Reports Table
-- =====================================================
-- Stores metadata for all generated reports
CREATE TABLE IF NOT EXISTS DQ_REPORTS (
    REPORT_ID VARCHAR(36) PRIMARY KEY,
    REPORT_TYPE VARCHAR(50) NOT NULL,  -- 'PLATFORM' | 'DATASET' | 'INCIDENT'
    SCOPE VARCHAR(100),                 -- 'PLATFORM' or dataset identifier (DB.SCHEMA.TABLE)
    REPORT_DATE DATE NOT NULL,
    GENERATED_BY VARCHAR(100),
    GENERATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FORMAT VARCHAR(10),                 -- 'PDF' | 'CSV' | 'JSON'
    FILE_PATH VARCHAR(500),             -- Path to stored file
    FILE_SIZE_BYTES NUMBER,
    DOWNLOAD_COUNT NUMBER DEFAULT 0,
    SHARE_TOKEN VARCHAR(100),           -- For shareable links
    SHARE_EXPIRES_AT TIMESTAMP_NTZ,     -- Link expiry
    METADATA VARIANT,                   -- JSON metadata (filters, parameters, etc.)
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comments for documentation
COMMENT ON TABLE DQ_REPORTS IS 'Stores metadata for all generated data quality reports';
COMMENT ON COLUMN DQ_REPORTS.REPORT_ID IS 'Unique identifier for the report (UUID)';
COMMENT ON COLUMN DQ_REPORTS.REPORT_TYPE IS 'Type of report: PLATFORM, DATASET, or INCIDENT';
COMMENT ON COLUMN DQ_REPORTS.SCOPE IS 'Scope of report - PLATFORM or specific dataset identifier';
COMMENT ON COLUMN DQ_REPORTS.REPORT_DATE IS 'Date for which the report was generated';
COMMENT ON COLUMN DQ_REPORTS.GENERATED_BY IS 'User who generated the report';
COMMENT ON COLUMN DQ_REPORTS.FORMAT IS 'Export format: PDF, CSV, or JSON';
COMMENT ON COLUMN DQ_REPORTS.FILE_PATH IS 'Storage path for the generated report file';
COMMENT ON COLUMN DQ_REPORTS.DOWNLOAD_COUNT IS 'Number of times report has been downloaded';
COMMENT ON COLUMN DQ_REPORTS.SHARE_TOKEN IS 'Secure token for shareable links';
COMMENT ON COLUMN DQ_REPORTS.SHARE_EXPIRES_AT IS 'Expiry timestamp for shareable links';
COMMENT ON COLUMN DQ_REPORTS.METADATA IS 'Additional metadata in JSON format';



-- =====================================================
-- 2. Scheduled Reports Table
-- =====================================================
-- Stores configurations for scheduled report delivery
CREATE TABLE IF NOT EXISTS DQ_SCHEDULED_REPORTS (
    SCHEDULE_ID VARCHAR(36) PRIMARY KEY,
    REPORT_TYPE VARCHAR(50) NOT NULL,   -- 'PLATFORM' | 'DATASET'
    SCOPE VARCHAR(100),                  -- 'PLATFORM' or dataset identifier
    FREQUENCY VARCHAR(20) NOT NULL,      -- 'DAILY' | 'WEEKLY' | 'MONTHLY'
    SCHEDULE_TIME TIME,                  -- Time of day to run (e.g., '09:00:00')
    DAY_OF_WEEK NUMBER,                  -- For weekly: 0=Sunday, 6=Saturday
    DAY_OF_MONTH NUMBER,                 -- For monthly: 1-31
    RECIPIENTS VARIANT NOT NULL,         -- JSON array of email addresses
    FORMAT VARCHAR(10) DEFAULT 'PDF',    -- 'PDF' | 'CSV' | 'JSON'
    ENABLED BOOLEAN DEFAULT TRUE,
    LAST_RUN_AT TIMESTAMP_NTZ,
    LAST_RUN_STATUS VARCHAR(20),         -- 'SUCCESS' | 'FAILED'
    LAST_RUN_ERROR VARCHAR(1000),
    NEXT_RUN_AT TIMESTAMP_NTZ,
    RUN_COUNT NUMBER DEFAULT 0,
    CREATED_BY VARCHAR(100),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comments
COMMENT ON TABLE DQ_SCHEDULED_REPORTS IS 'Configurations for scheduled report delivery';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.SCHEDULE_ID IS 'Unique identifier for the schedule (UUID)';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.REPORT_TYPE IS 'Type of report to generate';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.FREQUENCY IS 'How often to run: DAILY, WEEKLY, or MONTHLY';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.SCHEDULE_TIME IS 'Time of day to generate report';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.RECIPIENTS IS 'JSON array of recipient email addresses';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.ENABLED IS 'Whether the schedule is active';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.LAST_RUN_AT IS 'Timestamp of last execution';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.NEXT_RUN_AT IS 'Timestamp of next scheduled execution';

-- =====================================================
-- 3. Report Delivery History Table
-- =====================================================
-- Tracks each delivery attempt for scheduled reports
CREATE TABLE IF NOT EXISTS DQ_REPORT_DELIVERIES (
    DELIVERY_ID VARCHAR(36) PRIMARY KEY,
    SCHEDULE_ID VARCHAR(36) NOT NULL,
    REPORT_ID VARCHAR(36),               -- Link to generated report
    DELIVERY_METHOD VARCHAR(20),         -- 'EMAIL' | 'LINK'
    RECIPIENTS VARIANT,                  -- JSON array of recipients
    STATUS VARCHAR(20),                  -- 'SUCCESS' | 'FAILED' | 'PENDING'
    ERROR_MESSAGE VARCHAR(1000),
    DELIVERED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE DQ_REPORT_DELIVERIES IS 'Tracks delivery history for scheduled reports';
COMMENT ON COLUMN DQ_REPORT_DELIVERIES.SCHEDULE_ID IS 'Reference to scheduled report configuration';
COMMENT ON COLUMN DQ_REPORT_DELIVERIES.REPORT_ID IS 'Reference to generated report';
COMMENT ON COLUMN DQ_REPORT_DELIVERIES.STATUS IS 'Delivery status: SUCCESS, FAILED, or PENDING';


-- =====================================================
-- 4. Sample Data (Optional - for testing)
-- =====================================================
-- Uncomment to insert sample scheduled report
/*
INSERT INTO DQ_SCHEDULED_REPORTS (
    SCHEDULE_ID,
    REPORT_TYPE,
    SCOPE,
    FREQUENCY,
    SCHEDULE_TIME,
    RECIPIENTS,
    FORMAT,
    ENABLED,
    NEXT_RUN_AT,
    CREATED_BY
) VALUES (
    'sample-schedule-001',
    'PLATFORM',
    'PLATFORM',
    'DAILY',
    '09:00:00',
    PARSE_JSON('["admin@company.com", "dq-team@company.com"]'),
    'PDF',
    TRUE,
    DATEADD(day, 1, CURRENT_TIMESTAMP()),
    'system'
);
*/

-- =====================================================
-- 5. Verification Queries
-- =====================================================
-- Verify tables were created
SELECT 'DQ_REPORTS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM DQ_REPORTS
UNION ALL
SELECT 'DQ_SCHEDULED_REPORTS', COUNT(*) FROM DQ_SCHEDULED_REPORTS
UNION ALL
SELECT 'DQ_REPORT_DELIVERIES', COUNT(*) FROM DQ_REPORT_DELIVERIES;

-- Show table structures
DESCRIBE TABLE DQ_REPORTS;
DESCRIBE TABLE DQ_SCHEDULED_REPORTS;
DESCRIBE TABLE DQ_REPORT_DELIVERIES;






-- Governance Module Tables
-- Run this in Snowflake to enable Data Governance features

USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_CONFIG;

-- 1. Data Ownership
-- Links datasets to owners and stewards
CREATE TABLE IF NOT EXISTS DQ_DATASET_OWNERSHIP (
    OWNERSHIP_ID VARCHAR(36) DEFAULT UUID_STRING(),
    DATASET_NAME VARCHAR(200) NOT NULL, -- Logical link to source table
    SCHEMA_NAME VARCHAR(100) NOT NULL,
    DATABASE_NAME VARCHAR(100) NOT NULL,
    DATA_OWNER VARCHAR(100),
    DATA_STEWARD VARCHAR(100),
    CRITICALITY VARCHAR(20), -- 'HIGH', 'MEDIUM', 'LOW'
    CONTACT_EMAIL VARCHAR(100),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(100),
    PRIMARY KEY (DATABASE_NAME, SCHEMA_NAME, DATASET_NAME)
);

-- 2. SLA Configuration
-- Defines expectations for datasets
CREATE TABLE IF NOT EXISTS DQ_SLA_CONFIG (
    SLA_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    DATASET_NAME VARCHAR(200),
    SCHEMA_NAME VARCHAR(100),
    DATABASE_NAME VARCHAR(100),
    SLA_TYPE VARCHAR(50), -- 'FRESHNESS', 'QUALITY', 'AVAILABILITY'
    THRESHOLD_VALUE VARCHAR(50),
    WINDOW_HOURS INT,
    ENABLED BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(100)
);

-- 3. Governance Policies
-- Organization-wide policies
CREATE TABLE IF NOT EXISTS DQ_GOVERNANCE_POLICIES (
    POLICY_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    POLICY_NAME VARCHAR(200) NOT NULL,
    DESCRIPTION TEXT,
    SCOPE VARCHAR(100), -- 'GLOBAL', 'PII', 'FINANCE'
    IS_ENFORCED BOOLEAN DEFAULT FALSE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Audit Log
-- Tracks all governance changes
CREATE TABLE IF NOT EXISTS DQ_GOVERNANCE_AUDIT (
    AUDIT_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    ENTITY_TYPE VARCHAR(50), -- 'OWNERSHIP', 'SLA', 'POLICY'
    ENTITY_ID VARCHAR(36),
    ACTION VARCHAR(50), -- 'CREATE', 'UPDATE', 'DELETE'
    CHANGED_BY VARCHAR(100),
    CHANGED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    OLD_VALUE VARIANT,
    NEW_VALUE VARIANT
);

-- Seed Initial Policies
INSERT INTO DQ_GOVERNANCE_POLICIES (POLICY_NAME, DESCRIPTION, SCOPE, IS_ENFORCED)
SELECT 'PII Data Masking', 'All Personally Identifiable Information must be masked in lower environments.', 'GLOBAL', TRUE
WHERE NOT EXISTS (SELECT 1 FROM DQ_GOVERNANCE_POLICIES WHERE POLICY_NAME = 'PII Data Masking');

INSERT INTO DQ_GOVERNANCE_POLICIES (POLICY_NAME, DESCRIPTION, SCOPE, IS_ENFORCED)
SELECT 'Bronze Layer Retention', 'Raw data in Bronze layer must be retained for 7 years.', 'COMPLIANCE', FALSE
WHERE NOT EXISTS (SELECT 1 FROM DQ_GOVERNANCE_POLICIES WHERE POLICY_NAME = 'Bronze Layer Retention');





-- ============================================================================
-- LOAD HISTORY TRACKING - PRODUCTION SETUP
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Track table load operations for observability metrics
-- Prerequisites: 01_Environment_Setup.sql and 06_Metrics_Tables.sql executed
-- Version: 1.0.0
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_METRICS;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- ============================================================================
-- SECTION 1: CREATE LOAD HISTORY TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS TABLE_LOAD_HISTORY (
    LOAD_ID                     VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    DATABASE_NAME               VARCHAR(100) NOT NULL,
    SCHEMA_NAME                 VARCHAR(100) NOT NULL,
    TABLE_NAME                  VARCHAR(100) NOT NULL,
    LOAD_TYPE                   VARCHAR(50) COMMENT 'INSERT | UPDATE | MERGE | TRUNCATE_INSERT | COPY',
    LOAD_STATUS                 VARCHAR(50) COMMENT 'SUCCESS | FAILED | PARTIAL',
    ROWS_LOADED                 NUMBER DEFAULT 0,
    ROWS_UPDATED                NUMBER DEFAULT 0,
    ROWS_DELETED                NUMBER DEFAULT 0,
    BYTES_LOADED                NUMBER DEFAULT 0,
    LOAD_START_TIME             TIMESTAMP_NTZ,
    LOAD_END_TIME               TIMESTAMP_NTZ,
    DURATION_SECONDS            NUMBER(10,2),
    ERROR_MESSAGE               VARCHAR(4000),
    TRIGGERED_BY                VARCHAR(100) COMMENT 'User or system that triggered the load',
    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Tracks table load operations for observability and reliability metrics';

-- Create clustering for performance
ALTER TABLE TABLE_LOAD_HISTORY CLUSTER BY (LOAD_START_TIME, TABLE_NAME);

-- ============================================================================
-- SECTION 2: INSERT SAMPLE LOAD HISTORY DATA
-- ============================================================================
-- This creates realistic load history for demo/testing purposes

-- Helper function to generate random timestamps over the last 30 days
-- We'll insert load history for the Bronze layer tables

INSERT INTO TABLE_LOAD_HISTORY (
    DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, LOAD_TYPE, LOAD_STATUS,
    ROWS_LOADED, BYTES_LOADED, LOAD_START_TIME, LOAD_END_TIME, 
    DURATION_SECONDS, TRIGGERED_BY
)
SELECT 
    'BANKING_DW' AS DATABASE_NAME,
    'BRONZE' AS SCHEMA_NAME,
    table_name,
    'INSERT' AS LOAD_TYPE,
    CASE 
        WHEN UNIFORM(1, 100, RANDOM()) <= 95 THEN 'SUCCESS'  -- 95% success rate
        ELSE 'FAILED'
    END AS LOAD_STATUS,
    UNIFORM(10000, 500000, RANDOM()) AS ROWS_LOADED,
    UNIFORM(1000000, 50000000, RANDOM()) AS BYTES_LOADED,
    DATEADD(day, -day_offset, DATEADD(hour, -hour_offset, CURRENT_TIMESTAMP())) AS LOAD_START_TIME,
    DATEADD(minute, UNIFORM(1, 15, RANDOM()), 
        DATEADD(day, -day_offset, DATEADD(hour, -hour_offset, CURRENT_TIMESTAMP()))
    ) AS LOAD_END_TIME,
    UNIFORM(60, 900, RANDOM()) / 60.0 AS DURATION_SECONDS,
    'ETL_PIPELINE' AS TRIGGERED_BY
FROM (
    SELECT 'STG_CUSTOMER' AS table_name UNION ALL
    SELECT 'STG_ACCOUNT' UNION ALL
    SELECT 'STG_TRANSACTION' UNION ALL
    SELECT 'STG_DAILY_BALANCE' UNION ALL
    SELECT 'STG_FX_RATE'
) tables
CROSS JOIN (
    -- Generate loads for the last 30 days (daily loads)
    SELECT SEQ4() AS day_offset
    FROM TABLE(GENERATOR(ROWCOUNT => 30))
) days
CROSS JOIN (
    -- Some tables load multiple times per day
    SELECT SEQ4() AS hour_offset
    FROM TABLE(GENERATOR(ROWCOUNT => 2))
) hours
WHERE UNIFORM(1, 100, RANDOM()) <= 90; -- Not every scheduled load runs

-- Add some failed loads with error messages
INSERT INTO TABLE_LOAD_HISTORY (
    DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, LOAD_TYPE, LOAD_STATUS,
    ROWS_LOADED, BYTES_LOADED, LOAD_START_TIME, LOAD_END_TIME, 
    DURATION_SECONDS, ERROR_MESSAGE, TRIGGERED_BY
)
VALUES
    ('BANKING_DW', 'BRONZE', 'STG_TRANSACTION', 'INSERT', 'FAILED', 
     0, 0, DATEADD(day, -5, CURRENT_TIMESTAMP()), DATEADD(day, -5, CURRENT_TIMESTAMP()),
     0.5, 'Connection timeout to source system', 'ETL_PIPELINE'),
    ('BANKING_DW', 'BRONZE', 'STG_DAILY_BALANCE', 'INSERT', 'FAILED',
     0, 0, DATEADD(day, -12, CURRENT_TIMESTAMP()), DATEADD(day, -12, CURRENT_TIMESTAMP()),
     1.2, 'Source file not found', 'ETL_PIPELINE'),
    ('BANKING_DW', 'BRONZE', 'STG_ACCOUNT', 'INSERT', 'FAILED',
     0, 0, DATEADD(day, -20, CURRENT_TIMESTAMP()), DATEADD(day, -20, CURRENT_TIMESTAMP()),
     0.3, 'Schema mismatch in source data', 'ETL_PIPELINE');

-- ============================================================================
-- SECTION 3: CREATE HELPER PROCEDURE TO LOG LOADS
-- ============================================================================
-- This procedure can be called from ETL processes to log load operations

CREATE OR REPLACE PROCEDURE LOG_TABLE_LOAD(
    P_DATABASE VARCHAR,
    P_SCHEMA VARCHAR,
    P_TABLE VARCHAR,
    P_LOAD_TYPE VARCHAR,
    P_LOAD_STATUS VARCHAR,
    P_ROWS_LOADED NUMBER,
    P_BYTES_LOADED NUMBER,
    P_LOAD_START_TIME TIMESTAMP_NTZ,
    P_LOAD_END_TIME TIMESTAMP_NTZ,
    P_ERROR_MESSAGE VARCHAR,
    P_TRIGGERED_BY VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO DATA_QUALITY_DB.DQ_METRICS.TABLE_LOAD_HISTORY (
        DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, LOAD_TYPE, LOAD_STATUS,
        ROWS_LOADED, BYTES_LOADED, LOAD_START_TIME, LOAD_END_TIME,
        DURATION_SECONDS, ERROR_MESSAGE, TRIGGERED_BY
    )
    VALUES (
        :P_DATABASE, :P_SCHEMA, :P_TABLE, :P_LOAD_TYPE, :P_LOAD_STATUS,
        :P_ROWS_LOADED, :P_BYTES_LOADED, :P_LOAD_START_TIME, :P_LOAD_END_TIME,
        DATEDIFF('second', :P_LOAD_START_TIME, :P_LOAD_END_TIME),
        :P_ERROR_MESSAGE, :P_TRIGGERED_BY
    );
    
    RETURN 'Load logged successfully';
END;
$$;

-- ============================================================================
-- SECTION 4: CREATE VIEW FOR EASY QUERYING
-- ============================================================================

CREATE OR REPLACE VIEW VW_TABLE_LOAD_SUMMARY AS
SELECT 
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME,
    COUNT(*) AS TOTAL_LOADS,
    SUM(CASE WHEN LOAD_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESSFUL_LOADS,
    SUM(CASE WHEN LOAD_STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_LOADS,
    ROUND(
        (SUM(CASE WHEN LOAD_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0) / 
        NULLIF(COUNT(*), 0), 
        2
    ) AS SUCCESS_RATE,
    MAX(LOAD_END_TIME) AS LAST_LOAD_TIME,
    SUM(ROWS_LOADED) AS TOTAL_ROWS_LOADED,
    SUM(BYTES_LOADED) AS TOTAL_BYTES_LOADED,
    AVG(DURATION_SECONDS) AS AVG_DURATION_SECONDS
FROM TABLE_LOAD_HISTORY
WHERE LOAD_START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME;

-- ============================================================================
-- SECTION 5: VERIFICATION
-- ============================================================================

-- Verify table creation
SELECT 'Table created successfully' AS STATUS,
       COUNT(*) AS SAMPLE_RECORDS
FROM TABLE_LOAD_HISTORY;

-- Show sample load history
SELECT 
    DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS FULL_TABLE_NAME,
    LOAD_TYPE,
    LOAD_STATUS,
    ROWS_LOADED,
    LOAD_START_TIME,
    DURATION_SECONDS,
    ERROR_MESSAGE
FROM TABLE_LOAD_HISTORY
ORDER BY LOAD_START_TIME DESC
LIMIT 10;

-- Show summary by table
SELECT 
    TABLE_NAME,
    TOTAL_LOADS,
    SUCCESSFUL_LOADS,
    FAILED_LOADS,
    SUCCESS_RATE || '%' AS SUCCESS_RATE,
    LAST_LOAD_TIME
FROM VW_TABLE_LOAD_SUMMARY
ORDER BY TABLE_NAME;

-- ============================================================================
-- LOAD HISTORY TRACKING SETUP COMPLETE
-- ============================================================================

SELECT '=== LOAD HISTORY TRACKING SETUP COMPLETE ===' AS STATUS;
SELECT 'Total Load Records: ' || COUNT(*) AS INFO 
FROM TABLE_LOAD_HISTORY;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================
-- 
-- Example 1: Log a successful load
-- CALL LOG_TABLE_LOAD(
--     'BANKING_DW', 'BRONZE', 'STG_CUSTOMER', 'INSERT', 'SUCCESS',
--     50000, 25000000, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
--     NULL, 'MANUAL_LOAD'
-- );
--
-- Example 2: Log a failed load
-- CALL LOG_TABLE_LOAD(
--     'BANKING_DW', 'BRONZE', 'STG_TRANSACTION', 'INSERT', 'FAILED',
--     0, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
--     'Connection timeout', 'ETL_PIPELINE'
-- );
--
-- Example 3: Query load history for a specific table
-- SELECT * FROM TABLE_LOAD_HISTORY
-- WHERE DATABASE_NAME = 'BANKING_DW'
--   AND SCHEMA_NAME = 'BRONZE'
--   AND TABLE_NAME = 'STG_DAILY_BALANCE'
-- ORDER BY LOAD_START_TIME DESC;
-- ============================================================================
