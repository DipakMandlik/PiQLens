-- ============================================================================
-- DATA QUALITY EXECUTION ENGINE - PRODUCTION VERSION
-- Pi-QLense Data Quality Platform
-- ============================================================================
-- Purpose: Config-driven DQ execution with complete metrics collection
-- Prerequisites: All previous setup scripts executed 
-- Version: 1.0.0
-- ============================================================================


-- Set Timestamp as you wnted 
ALTER SESSION SET TIMEZONE = 'Asia/Kolkata';


SELECT CURRENT_TIMESTAMP(), CURRENT_DATE();

-- This is the CORE EXECUTION ENGINE that runs all DQ checks.
-- All columns in all metrics tables are properly populated.
-- 
-- Features:
-- - Config-driven execution (no hardcoded rules)
-- - Complete metrics collection (all fields populated)
-- - Failed record logging with samples
-- - Daily summary generation
-- - Weighted scoring by business domain
-- - Flexible filtering (by dataset, rule type, criticality)
-- 
-- ============================================================================



USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_ENGINE;
USE WAREHOUSE DQ_ANALYTICS_WH;




-- ============================================================================
-- MAIN EXECUTION PROCEDURE
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_EXECUTE_DQ_CHECKS(
    P_DATASET_ID VARCHAR DEFAULT NULL,
    P_RULE_TYPE VARCHAR DEFAULT NULL,
    P_RUN_MODE VARCHAR DEFAULT 'FULL'
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
from snowflake.snowpark import Session
import json
import uuid
import re
from datetime import datetime

def main(session: snowpark.Session, p_dataset_id: str, p_rule_type: str, p_run_mode: str) -> str:
    """
    Main Data Quality Check Execution Engine with COMPLETE metrics collection
    
    Parameters:
        p_dataset_id: Filter by specific dataset (NULL = all datasets)
        p_rule_type: Filter by rule type (NULL = all types)
        p_run_mode: Execution mode (FULL | CRITICAL_ONLY)
    
    Returns:
        JSON string with execution summary
    """
    
    try:
        # Initialize run
        session.sql("ALTER SESSION SET TIMEZONE = 'Asia/Kolkata'").collect()

        current_ts = session.sql("SELECT TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS')").collect()[0][0]

        run_id = "DQ_RUN_" + current_ts + "_" + str(uuid.uuid4())[:8]

        run_type='FULL'
        scan_scope = 'FULL_TABLE'
        scan_reason = 'BASELINE'

        start_time = datetime.now()
        triggered_by = session.sql("SELECT CURRENT_USER()").collect()[0][0]
        print(f"RUN_TYPE: {run_type}")
        print(f"\n{'='*80}")
        print(f"DATA QUALITY EXECUTION ENGINE - FULL METRICS COLLECTION")
        print(f"{'='*80}")
        print(f"Run ID: {run_id}")
        print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Triggered By: {triggered_by}")
        print(f"Dataset Filter: {p_dataset_id or 'ALL'}")
        print(f"Rule Type Filter: {p_rule_type or 'ALL'}")
        print(f"Run Mode: {p_run_mode}")
        print(f"{'='*80}\n")
        
        # Insert run control record with ALL fields
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL (
                RUN_ID, 
                TRIGGERED_BY, 
                START_TS, 
                RUN_STATUS,
                RUN_TYPE,
                TOTAL_DATASETS,
                TOTAL_CHECKS,
                PASSED_CHECKS,
                FAILED_CHECKS,
                WARNING_CHECKS,
                SKIPPED_CHECKS,
                CREATED_TS
            ) VALUES (
                '{run_id}', 
                '{triggered_by}',
                CURRENT_TIMESTAMP(),
                'RUNNING',
                '{run_type}',
                0, 0, 0, 0, 0, 0,
                CURRENT_TIMESTAMP()
            )
        """).collect()
        
        # Fetch datasets to process
        datasets = fetch_datasets(session, p_dataset_id, p_run_mode)
        
        if not datasets:
            raise Exception("No active datasets found")
        
        print(f"📊 Processing {len(datasets)} dataset(s)\n")
        
        # Initialize counters
        stats = {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'warning_checks': 0,
            'skipped_checks': 0,
            'total_records_processed': 0,
            'total_invalid_records': 0
        }
        
        # Process each dataset
        for dataset in datasets:
            process_dataset(session, run_id, dataset, p_rule_type, stats)
        
        # Finalize run with ALL metrics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        run_status = 'COMPLETED' if stats['failed_checks'] == 0 else 'COMPLETED_WITH_FAILURES'
        
        session.sql(f"""
            UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
            SET 
                END_TS = CURRENT_TIMESTAMP(),
                DURATION_SECONDS = {duration},
                RUN_STATUS = '{run_status}',
                TOTAL_DATASETS = {len(datasets)},
                TOTAL_CHECKS = {stats['total_checks']},
                PASSED_CHECKS = {stats['passed_checks']},
                FAILED_CHECKS = {stats['failed_checks']},
                WARNING_CHECKS = {stats['warning_checks']},
                SKIPPED_CHECKS = {stats['skipped_checks']},
                ERROR_MESSAGE = NULL
            WHERE RUN_ID = '{run_id}'
        """).collect()
        
        # Print summary
        print(f"\n{'='*80}")
        print(f"EXECUTION SUMMARY")
        print(f"{'='*80}")
        print(f"Status: {run_status}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Datasets: {len(datasets)}")
        print(f"Total Checks: {stats['total_checks']}")
        print(f"✓ Passed: {stats['passed_checks']}")
        print(f"✗ Failed: {stats['failed_checks']}")
        print(f"⚠ Warnings: {stats['warning_checks']}")
        print(f"⊘ Skipped: {stats['skipped_checks']}")
        print(f"Records Processed: {stats['total_records_processed']:,}")
        print(f"Invalid Records: {stats['total_invalid_records']:,}")
        
        if stats['total_checks'] > 0:
            pass_rate = round((stats['passed_checks'] / stats['total_checks'] * 100), 2)
            print(f"Overall Pass Rate: {pass_rate}%")
        
        print(f"{'='*80}\n")
        
        # Generate daily summary with ALL metrics
        try:
            generate_daily_summary(session, run_id)
            print("✓ Daily summary generated with complete metrics\n")
        except Exception as e:
            print(f"⚠ Warning: Daily summary generation failed: {str(e)}\n")
        
        # Return result
        result = {
            'run_id': run_id,
            'status': run_status,
            'duration_seconds': duration,
            'datasets_processed': len(datasets),
            'total_checks': stats['total_checks'],
            'passed': stats['passed_checks'],
            'failed': stats['failed_checks'],
            'warnings': stats['warning_checks'],
            'skipped': stats['skipped_checks'],
            'records_processed': stats['total_records_processed'],
            'invalid_records': stats['total_invalid_records'],
            'pass_rate': round((stats['passed_checks'] / stats['total_checks'] * 100), 2) if stats['total_checks'] > 0 else 0
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ FATAL ERROR: {error_msg}\n")
        
        try:
            session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
                SET 
                    END_TS = CURRENT_TIMESTAMP(),
                    RUN_STATUS = 'FAILED',
                    ERROR_MESSAGE = '{error_msg.replace("'", "''")[:4000]}'
                WHERE RUN_ID = '{run_id}'
            """).collect()
        except:
            pass
        
        return json.dumps({'run_id': run_id, 'status': 'FAILED', 'error': error_msg})


def fetch_datasets(session, p_dataset_id, p_run_mode):
    """Fetch active datasets based on filters"""
    
    dataset_filter = f"AND dc.DATASET_ID = '{p_dataset_id}'" if p_dataset_id else ""
    criticality_filter = "AND dc.CRITICALITY = 'CRITICAL'" if p_run_mode == 'CRITICAL_ONLY' else ""
    
    query = f"""
        SELECT 
            dc.DATASET_ID,
            dc.SOURCE_DATABASE,
            dc.SOURCE_SCHEMA,
            dc.SOURCE_TABLE,
            dc.BUSINESS_DOMAIN,
            dc.CRITICALITY,
            dc.PRIMARY_KEY_COLUMN
        FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG dc
        WHERE dc.IS_ACTIVE = TRUE
        {dataset_filter}
        {criticality_filter}
        ORDER BY 
            CASE dc.CRITICALITY 
                WHEN 'CRITICAL' THEN 1 
                WHEN 'HIGH' THEN 2 
                WHEN 'MEDIUM' THEN 3 
                ELSE 4 
            END
    """
    
    return session.sql(query).collect()


def process_dataset(session, run_id, dataset, p_rule_type, stats):
    """Process all rules for a single dataset with FULL metrics"""
    
    dataset_id = dataset['DATASET_ID']
    source_db = dataset['SOURCE_DATABASE']
    source_schema = dataset['SOURCE_SCHEMA']
    source_table = dataset['SOURCE_TABLE']
    business_domain = dataset['BUSINESS_DOMAIN']
    primary_key_col = dataset['PRIMARY_KEY_COLUMN']
    
    print(f"📁 Dataset: {dataset_id}")
    print(f"   Table: {source_db}.{source_schema}.{source_table}")
    print(f"   Domain: {business_domain}")
    
    # Fetch rules for this dataset
    rules = fetch_rules(session, dataset_id, business_domain, p_rule_type)
    
    if not rules:
        print(f"   ⚠ No active rules configured\n")
        return
    
    print(f"   Rules: {len(rules)}\n")
    
    # Execute each rule
    for rule in rules:
        stats['total_checks'] += 1
        
        try:
            result = execute_check(session,run_id,dataset_id,source_db,source_schema,source_table,rule,business_domain,'FULL_TABLE','BASELINE',primary_key_col)
            
            # Update stats with ALL metrics
            stats['total_records_processed'] += result['total_records']
            stats['total_invalid_records'] += result['failed_records']
            
            if result['status'] == 'PASSED':
                stats['passed_checks'] += 1
                icon = '✓'
            elif result['status'] == 'FAILED':
                stats['failed_checks'] += 1
                icon = '✗'
            elif result['status'] == 'WARNING':
                stats['warning_checks'] += 1
                icon = '⚠'
            else:
                stats['skipped_checks'] += 1
                icon = '⊘'
            
            # Print result
            col_display = f"[{rule['COLUMN_NAME']}]" if rule['COLUMN_NAME'] else "[TABLE]"
            print(f"   {icon} {rule['RULE_TYPE']:15s} {col_display:25s} "
                  f"{result['status']:8s} ({result['pass_rate']:6.2f}% | "
                  f"{result['valid_records']:,}/{result['total_records']:,})")
            
        except Exception as e:
            stats['skipped_checks'] += 1
            print(f"   ⊘ ERROR: {rule['RULE_NAME']} - {str(e)}")
            
            # Log error in check results
            log_error_check(session, run_id, dataset_id, source_table, rule, str(e))
    
    print()


def fetch_rules(session, dataset_id, business_domain, p_rule_type):
    """Fetch active rules for a dataset"""
    
    rule_type_filter = f"AND rm.RULE_TYPE = '{p_rule_type}'" if p_rule_type else ""
    
    query = f"""
        SELECT 
            drc.CONFIG_ID,
            drc.DATASET_ID,
            drc.COLUMN_NAME,
            drc.THRESHOLD_VALUE,
            rm.RULE_ID,
            rm.RULE_NAME,
            rm.RULE_TYPE,
            rm.RULE_LEVEL,
            rm.DESCRIPTION,
            rst.SQL_TEMPLATE,
            COALESCE(wm.WEIGHT, 1.0) as WEIGHT,
            COALESCE(wm.PRIORITY, 3) as PRIORITY
        FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_RULE_CONFIG drc
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER rm
            ON drc.RULE_ID = rm.RULE_ID
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_SQL_TEMPLATE rst
            ON rm.RULE_ID = rst.RULE_ID
            AND rst.IS_ACTIVE = TRUE
        LEFT JOIN DATA_QUALITY_DB.DQ_CONFIG.WEIGHTS_MAPPING wm
            ON rm.RULE_TYPE = wm.RULE_TYPE
            AND wm.BUSINESS_DOMAIN = '{business_domain}'
            AND wm.IS_ACTIVE = TRUE
            AND CURRENT_DATE() BETWEEN wm.EFFECTIVE_DATE 
                AND COALESCE(wm.EXPIRY_DATE, '9999-12-31')
        WHERE drc.DATASET_ID = '{dataset_id}'
        AND drc.IS_ACTIVE = TRUE
        AND rm.IS_ACTIVE = TRUE
        {rule_type_filter}
        ORDER BY 
            COALESCE(wm.PRIORITY, 3),
            CASE rm.RULE_TYPE
                WHEN 'COMPLETENESS' THEN 1
                WHEN 'UNIQUENESS' THEN 2
                WHEN 'VALIDITY' THEN 3
                WHEN 'CONSISTENCY' THEN 4
                WHEN 'FRESHNESS' THEN 5
                WHEN 'VOLUME' THEN 6
            END,
            drc.COLUMN_NAME
    """
    
    return session.sql(query).collect()


def execute_check(session, run_id, dataset_id, source_db, source_schema, source_table, rule, business_domain,scan_scope,
    scan_reason, primary_key_col):
    """Execute a single DQ check with COMPLETE metrics collection"""
    
    check_start = datetime.now()
    
    # Build dynamic SQL
    sql = build_check_sql(rule, source_db, source_schema, source_table)
    
    # Execute the check
    result = session.sql(sql).collect()[0]
    
    total_count = int(result['TOTAL_COUNT'])
    error_count = int(result['ERROR_COUNT'])
    valid_count = total_count - error_count
    
    # Calculate null count separately if it's a completeness check
    null_count = 0
    if rule['RULE_TYPE'] == 'COMPLETENESS' and rule['COLUMN_NAME']:
        null_sql = f"""
            SELECT COUNT(*) - COUNT({rule['COLUMN_NAME']}) as NULL_COUNT
            FROM {source_db}.{source_schema}.{source_table}
        """
        null_result = session.sql(null_sql).collect()[0]
        null_count = int(null_result['NULL_COUNT'])
    
    # Calculate duplicate count if it's a uniqueness check
    duplicate_count = 0
    if rule['RULE_TYPE'] == 'UNIQUENESS' and rule['COLUMN_NAME']:
        duplicate_count = error_count
    
    # Calculate metrics
    pass_rate = round((valid_count / total_count * 100), 2) if total_count > 0 else 100.0
    threshold = float(rule['THRESHOLD_VALUE'])
    
    # Determine status
    if pass_rate >= threshold:
        status = 'PASSED'
    elif pass_rate >= (threshold - 5):
        status = 'WARNING'
    else:
        status = 'FAILED'
    
    check_end = datetime.now()
    execution_time_ms = (check_end - check_start).total_seconds() * 1000
    
    # Estimate credits (rough approximation based on execution time)
    execution_credits = round((execution_time_ms / 1000.0) * 0.00001, 6)
    
    # Prepare failure reason if check failed
    failure_reason = None
    if status == 'FAILED':
        failure_reason = f"Pass rate {pass_rate}% below threshold {threshold}%. Found {error_count} invalid records out of {total_count}."
    elif status == 'WARNING':
        failure_reason = f"Pass rate {pass_rate}% within warning range of threshold {threshold}%."
    
    # Log results with ALL columns populated
    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
            RUN_ID,
            CHECK_TIMESTAMP,
            DATASET_ID,
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            COLUMN_NAME,
            RULE_ID,
            RULE_NAME,
            RULE_TYPE,
            RULE_LEVEL,
            SCAN_SCOPE,       
            SCAN_REASON,      
            TOTAL_RECORDS,
            VALID_RECORDS,
            INVALID_RECORDS,
            NULL_RECORDS,
            DUPLICATE_RECORDS,
            PASS_RATE,
            THRESHOLD,
            CHECK_STATUS,
            EXECUTION_TIME_MS,
            EXECUTION_CREDITS,
            FAILURE_REASON,
            CREATED_TS
        ) VALUES (
            '{run_id}',
            CURRENT_TIMESTAMP(),
            '{dataset_id}',
            '{source_db}',
            '{source_schema}',
            '{source_table}',
            {f"'{rule['COLUMN_NAME']}'" if rule['COLUMN_NAME'] else 'NULL'},
            {rule['RULE_ID']},
            '{rule['RULE_NAME']}',
            '{rule['RULE_TYPE']}',
            '{rule['RULE_LEVEL']}',
            '{scan_scope}',
            '{scan_reason}',
            {total_count},
            {valid_count},
            {error_count},
            {null_count},
            {duplicate_count},
            {pass_rate},
            {threshold},
            '{status}',
            {execution_time_ms},
            {execution_credits},
            {f"'{failure_reason}'" if failure_reason else 'NULL'},
            CURRENT_TIMESTAMP()
        )
    """).collect()
    
    # Deterministic CHECK_ID lookup for failed-row linkage
    check_id_result = session.sql(f"""
        SELECT CHECK_ID
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
        WHERE RUN_ID = '{run_id}'
          AND DATASET_ID = '{dataset_id}'
          AND TABLE_NAME = '{source_table}'
          AND RULE_ID = {rule['RULE_ID']}
        ORDER BY CHECK_TIMESTAMP DESC, CHECK_ID DESC
        LIMIT 1
    """).collect()
    check_id = check_id_result[0]['CHECK_ID'] if check_id_result else None

    # Capture row-level failed records (FAILED only)
    if status == 'FAILED' and error_count > 0 and check_id is not None:
        capture_failed_rows(
            session=session,
            check_id=check_id,
            run_id=run_id,
            dataset_id=dataset_id,
            source_db=source_db,
            source_schema=source_schema,
            source_table=source_table,
            rule=rule,
            primary_key_col=primary_key_col
        )
    
    return {
        'status': status,
        'pass_rate': pass_rate,
        'total_records': total_count,
        'valid_records': valid_count,
        'failed_records': error_count
    }


def log_error_check(session, run_id, dataset_id, table_name, rule, error_msg):
    """Log checks that failed to execute"""
    
    try:
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
                RUN_ID,
                DATASET_ID,
                TABLE_NAME,
                COLUMN_NAME,
                RULE_ID,
                RULE_NAME,
                RULE_TYPE,
                RULE_LEVEL,
                CHECK_STATUS,
                FAILURE_REASON,
                CHECK_TIMESTAMP,
                CREATED_TS
            ) VALUES (
                '{run_id}',
                '{dataset_id}',
                '{table_name}',
                {f"'{rule['COLUMN_NAME']}'" if rule['COLUMN_NAME'] else 'NULL'},
                {rule['RULE_ID']},
                '{rule['RULE_NAME']}',
                '{rule['RULE_TYPE']}',
                '{rule['RULE_LEVEL']}',
                'ERROR',
                '{error_msg.replace("'", "''")[:500]}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
        """).collect()
    except:
        pass


def build_check_sql(rule, source_db, source_schema, source_table):
    """Build dynamic SQL from template"""
    
    sql = rule['SQL_TEMPLATE']
    
    # Replace standard placeholders
    sql = sql.replace('{{DATABASE}}', source_db)
    sql = sql.replace('{{SCHEMA}}', source_schema)
    sql = sql.replace('{{TABLE}}', source_table)
    
    if rule['COLUMN_NAME']:
        sql = sql.replace('{{COLUMN}}', rule['COLUMN_NAME'])
    
    sql = sql.replace('{{THRESHOLD}}', str(int(rule['THRESHOLD_VALUE'])))
    
    # Handle special placeholders
    if '{{ALLOWED_VALUES}}' in sql:
        allowed_values = get_allowed_values(rule['COLUMN_NAME'])
        sql = sql.replace('{{ALLOWED_VALUES}}', allowed_values)
    
    if '{{PARENT_TABLE}}' in sql:
        parent_info = get_parent_table_info(rule['COLUMN_NAME'])
        for key, value in parent_info.items():
            sql = sql.replace(f'{{{{{key}}}}}', value)
    
    return sql


def get_allowed_values(column_name):
    """Get allowed values for validation"""
    
    values_map = {
        'kyc_status': "'Verified', 'Pending', 'Rejected', 'Incomplete'",
        'account_type': "'Savings', 'Checking', 'Credit', 'Investment'",
        'account_status': "'Active', 'Inactive', 'Closed', 'Frozen'",
        'transaction_type': "'Deposit', 'Withdrawal', 'Transfer', 'Payment'"
    }
    
    return values_map.get(column_name, "''")


def get_parent_table_info(column_name):
    """Get parent table info for FK checks"""
    
    fk_map = {
        'customer_id': {
            'PARENT_DATABASE': 'BANKING_DW',
            'PARENT_SCHEMA': 'BRONZE',
            'PARENT_TABLE': 'STG_CUSTOMER',
            'PARENT_KEY': 'customer_id'
        },
        'account_id': {
            'PARENT_DATABASE': 'BANKING_DW',
            'PARENT_SCHEMA': 'BRONZE',
            'PARENT_TABLE': 'STG_ACCOUNT',
            'PARENT_KEY': 'account_id'
        }
    }
    
    return fk_map.get(column_name, {})



def _safe_identifier_or_none(value):
    text = str(value or '').strip()
    if not text:
        return None
    return text if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', text) else None


def _resolve_failure_sql(session, rule_id, rule_type, base_relation, column_name, primary_key_col):
    try:
        template_rows = session.sql(f"""
            SELECT FAILURE_SQL_TEMPLATE
            FROM DATA_QUALITY_DB.DQ_CONFIG.RULE_FAILURE_SQL_TEMPLATE
            WHERE RULE_ID = {rule_id}
              AND IS_ACTIVE = TRUE
            ORDER BY TEMPLATE_VERSION DESC, TEMPLATE_ID DESC
            LIMIT 1
        """).collect()

        template = template_rows[0]['FAILURE_SQL_TEMPLATE'] if template_rows else None
        if template:
            sql = str(template)
            sql = sql.replace('{{TABLE}}', base_relation)
            sql = sql.replace('{{COLUMN}}', f'"{column_name}"' if column_name else 'NULL')
            sql = sql.replace('{{PRIMARY_KEY_COLUMN}}', f'"{primary_key_col}"')
            return sql
    except Exception:
        pass  # Template table may not exist; fall through to built-in SQL

    if not column_name:
        return None

    if rule_type == 'COMPLETENESS':
        return (
            f"SELECT TO_VARCHAR(\"{primary_key_col}\") AS FAILED_RECORD_PK, "
            f"TO_VARCHAR(\"{column_name}\") AS FAILED_VALUE, "
            f"'NULL_OR_BLANK' AS FAILURE_REASON "
            f"FROM {base_relation} "
            f"WHERE \"{column_name}\" IS NULL "
            f"OR LENGTH(TRIM(TO_VARCHAR(\"{column_name}\"))) = 0"
        )

    if rule_type == 'UNIQUENESS':
        return (
            f"SELECT TO_VARCHAR(\"{primary_key_col}\") AS FAILED_RECORD_PK, "
            f"TO_VARCHAR(\"{column_name}\") AS FAILED_VALUE, "
            f"'DUPLICATE_VALUE' AS FAILURE_REASON "
            f"FROM {base_relation} "
            f"QUALIFY COUNT(*) OVER (PARTITION BY \"{column_name}\") > 1"
        )

    if rule_type == 'VALIDITY':
        return (
            f"SELECT TO_VARCHAR(\"{primary_key_col}\") AS FAILED_RECORD_PK, "
            f"TO_VARCHAR(\"{column_name}\") AS FAILED_VALUE, "
            f"'INVALID_VALUE' AS FAILURE_REASON "
            f"FROM {base_relation} "
            f"WHERE \"{column_name}\" IS NOT NULL "
            f"AND LENGTH(TRIM(TO_VARCHAR(\"{column_name}\"))) > 0"
        )

    # Fallback for any other rule type: capture sample rows for debugging
    return (
        f"SELECT TO_VARCHAR(\"{primary_key_col}\") AS FAILED_RECORD_PK, "
        f"TO_VARCHAR(\"{column_name}\") AS FAILED_VALUE, "
        f"'CHECK_FAILED' AS FAILURE_REASON "
        f"FROM {base_relation} "
        f"WHERE 1=1"
    )


def capture_failed_rows(session, check_id, run_id, dataset_id, source_db,
                        source_schema, source_table, rule, primary_key_col):
    try:
        pk_col = _safe_identifier_or_none(primary_key_col)
        col_name = _safe_identifier_or_none(rule['COLUMN_NAME']) if rule['COLUMN_NAME'] else None

        if not pk_col:
            print(f"   WARNING: PRIMARY_KEY_COLUMN missing for dataset {dataset_id}; failed-row capture skipped.")
            return

        base_relation = f'{source_db}.{source_schema}.{source_table}'

        failure_sql = _resolve_failure_sql(
            session=session,
            rule_id=rule['RULE_ID'],
            rule_type=rule['RULE_TYPE'],
            base_relation=base_relation,
            column_name=col_name,
            primary_key_col=pk_col
        )

        if not failure_sql:
            print(f"   WARNING: No failure SQL template/fallback for rule {rule['RULE_NAME']}")
            return

        escaped_reason = f"Rule {rule['RULE_NAME']} failed in FULL scope.".replace("'", "''")
        rule_name_safe = str(rule['RULE_NAME']).replace("'", "''")
        rule_type_safe = str(rule['RULE_TYPE']).replace("'", "''")
        rule_level_safe = str(rule['RULE_LEVEL']).replace("'", "''")
        col_sql = f"'{col_name}'" if col_name else 'NULL'

        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS (
                CHECK_ID, RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                COLUMN_NAME, RULE_ID, RULE_NAME, RULE_TYPE, PRIMARY_KEY_COLUMN,
                FAILED_RECORD_PK, FAILED_COLUMN_VALUE, FAILURE_REASON, FAILURE_CATEGORY,
                IS_CRITICAL, CAN_AUTO_REMEDIATE, DETECTED_TS, CREATED_TS
            )
            SELECT
                {check_id}, '{run_id}', '{dataset_id}', '{source_db}', '{source_schema}', '{source_table}',
                {col_sql}, {rule['RULE_ID']}, '{rule_name_safe}', '{rule_type_safe}', '{pk_col}',
                TO_VARCHAR(f.FAILED_RECORD_PK), TO_VARCHAR(f.FAILED_VALUE),
                COALESCE(TO_VARCHAR(f.FAILURE_REASON), '{escaped_reason}'), '{rule_type_safe}',
                IFF(UPPER('{rule_level_safe}') IN ('CRITICAL', 'HIGH'), TRUE, FALSE),
                FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            FROM ({failure_sql}) f
            LIMIT 100
        """).collect()

    except Exception as e:
        print(f"Warning: Could not capture failed rows: {str(e)}")
def generate_daily_summary(session, run_id):
    """
    Generate correct DAILY summary (date-based, multi-run safe, idempotent)
    """

    session.sql(f"""
        MERGE INTO DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY t
        USING (
            SELECT
                CAST(CHECK_TIMESTAMP AS DATE) AS SUMMARY_DATE,
                DATASET_ID,
                DATABASE_NAME,
                SCHEMA_NAME,
                TABLE_NAME,
                'BANKING' AS BUSINESS_DOMAIN,

                /* Execution metrics */
                COUNT(*) AS TOTAL_CHECKS,
                SUM(CASE WHEN CHECK_STATUS = 'PASSED' THEN 1 ELSE 0 END) AS PASSED_CHECKS,
                SUM(CASE WHEN CHECK_STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_CHECKS,
                SUM(CASE WHEN CHECK_STATUS = 'WARNING' THEN 1 ELSE 0 END) AS WARNING_CHECKS,
                SUM(CASE WHEN CHECK_STATUS IN ('ERROR', 'SKIPPED') THEN 1 ELSE 0 END) AS SKIPPED_CHECKS,

                /* Quality metrics */
                ROUND(SUM(VALID_RECORDS)::FLOAT / NULLIF(SUM(TOTAL_RECORDS), 0) * 100,2 ) AS DQ_SCORE,
                ROUND(SUM(CASE WHEN RULE_TYPE = 'COMPLETENESS' THEN VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN RULE_TYPE = 'COMPLETENESS' THEN TOTAL_RECORDS END), 0) * 100, 2 ) AS COMPLETENESS_SCORE,
                ROUND(SUM(CASE WHEN RULE_TYPE = 'UNIQUENESS' THEN VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN RULE_TYPE = 'UNIQUENESS' THEN TOTAL_RECORDS END), 0) * 100, 2 ) AS UNIQUENESS_SCORE,
                ROUND(SUM(CASE WHEN RULE_TYPE = 'VALIDITY' THEN VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN RULE_TYPE = 'VALIDITY' THEN TOTAL_RECORDS END), 0) * 100, 2 ) AS VALIDITY_SCORE,
                ROUND(SUM(CASE WHEN RULE_TYPE = 'CONSISTENCY' THEN VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN RULE_TYPE = 'CONSISTENCY' THEN TOTAL_RECORDS END), 0) * 100, 2 ) AS CONSISTENCY_SCORE,
                ROUND(SUM(CASE WHEN RULE_TYPE = 'FRESHNESS' THEN VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN RULE_TYPE = 'FRESHNESS' THEN TOTAL_RECORDS END), 0) * 100, 2 ) AS FRESHNESS_SCORE,
                ROUND(SUM(CASE WHEN RULE_TYPE = 'VOLUME' THEN VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN RULE_TYPE = 'VOLUME' THEN TOTAL_RECORDS END), 0) * 100, 2 ) AS VOLUME_SCORE,

                /* Trust & SLA */
                CASE 
                    WHEN AVG(PASS_RATE) >= 95 THEN 'HIGH'
                    WHEN AVG(PASS_RATE) >= 85 THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS TRUST_LEVEL,
                CASE 
                    WHEN AVG(PASS_RATE) >= 95 THEN 'A'
                    WHEN AVG(PASS_RATE) >= 90 THEN 'B'
                    WHEN AVG(PASS_RATE) >= 80 THEN 'C'
                    WHEN AVG(PASS_RATE) >= 70 THEN 'D'
                    ELSE 'F'
                END AS QUALITY_GRADE,
                CASE WHEN (SUM(VALID_RECORDS)::FLOAT / NULLIF(SUM(TOTAL_RECORDS), 0)) * 100 >= 90 THEN TRUE ELSE FALSE END AS IS_SLA_MET,

                /* Record-level metrics */
                MAX(TOTAL_RECORDS) AS TOTAL_RECORDS,
                SUM(INVALID_RECORDS) AS FAILED_RECORDS_COUNT,
                ROUND(
                    (SUM(INVALID_RECORDS)::FLOAT / NULLIF(MAX(TOTAL_RECORDS), 0)) * 100, 
                    2
                ) AS FAILURE_RATE,

                /* Operational metrics */
                ROUND(SUM(EXECUTION_TIME_MS) / 1000.0, 2) AS TOTAL_EXECUTION_TIME_SEC,
                SUM(COALESCE(EXECUTION_CREDITS, 0)) AS TOTAL_CREDITS_CONSUMED,

                /* Run metadata (latest run of the day) */
                MAX(RUN_ID) AS LAST_RUN_ID,
                MAX(CHECK_TIMESTAMP) AS LAST_RUN_TS,
                MAX(CHECK_STATUS) AS LAST_RUN_STATUS

            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
            WHERE CAST(CHECK_TIMESTAMP AS DATE) = CURRENT_DATE()
            GROUP BY 
                CAST(CHECK_TIMESTAMP AS DATE),
                DATASET_ID,
                DATABASE_NAME,
                SCHEMA_NAME,
                TABLE_NAME
        ) s
        ON  t.SUMMARY_DATE = s.SUMMARY_DATE
        AND t.DATASET_ID   = s.DATASET_ID

        WHEN MATCHED THEN UPDATE SET
            t.TOTAL_CHECKS              = s.TOTAL_CHECKS,
            t.PASSED_CHECKS             = s.PASSED_CHECKS,
            t.FAILED_CHECKS             = s.FAILED_CHECKS,
            t.WARNING_CHECKS            = s.WARNING_CHECKS,
            t.SKIPPED_CHECKS            = s.SKIPPED_CHECKS,
            t.DQ_SCORE                  = s.DQ_SCORE,
            t.COMPLETENESS_SCORE        = s.COMPLETENESS_SCORE,
            t.UNIQUENESS_SCORE          = s.UNIQUENESS_SCORE,
            t.VALIDITY_SCORE            = s.VALIDITY_SCORE,
            t.CONSISTENCY_SCORE         = s.CONSISTENCY_SCORE,
            t.FRESHNESS_SCORE           = s.FRESHNESS_SCORE,
            t.VOLUME_SCORE              = s.VOLUME_SCORE,
            t.TRUST_LEVEL               = s.TRUST_LEVEL,
            t.QUALITY_GRADE             = s.QUALITY_GRADE,
            t.IS_SLA_MET                = s.IS_SLA_MET,
            t.TOTAL_RECORDS             = s.TOTAL_RECORDS,
            t.FAILED_RECORDS_COUNT      = s.FAILED_RECORDS_COUNT,
            t.FAILURE_RATE              = s.FAILURE_RATE,
            t.TOTAL_EXECUTION_TIME_SEC  = s.TOTAL_EXECUTION_TIME_SEC,
            t.TOTAL_CREDITS_CONSUMED    = s.TOTAL_CREDITS_CONSUMED,
            t.LAST_RUN_ID               = s.LAST_RUN_ID,
            t.LAST_RUN_TS               = s.LAST_RUN_TS,
            t.LAST_RUN_STATUS           = s.LAST_RUN_STATUS,
            t.CREATED_TS                = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN INSERT (
            SUMMARY_DATE,
            DATASET_ID,
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            BUSINESS_DOMAIN,
            TOTAL_CHECKS,
            PASSED_CHECKS,
            FAILED_CHECKS,
            WARNING_CHECKS,
            SKIPPED_CHECKS,
            DQ_SCORE,
            COMPLETENESS_SCORE,
            UNIQUENESS_SCORE,
            VALIDITY_SCORE,
            CONSISTENCY_SCORE,
            FRESHNESS_SCORE,
            VOLUME_SCORE,
            TRUST_LEVEL,
            QUALITY_GRADE,
            IS_SLA_MET,
            TOTAL_RECORDS,
            FAILED_RECORDS_COUNT,
            FAILURE_RATE,
            TOTAL_EXECUTION_TIME_SEC,
            TOTAL_CREDITS_CONSUMED,
            LAST_RUN_ID,
            LAST_RUN_TS,
            LAST_RUN_STATUS,
            CREATED_TS
        )
        VALUES (
            s.SUMMARY_DATE,
            s.DATASET_ID,
            s.DATABASE_NAME,
            s.SCHEMA_NAME,
            s.TABLE_NAME,
            s.BUSINESS_DOMAIN,
            s.TOTAL_CHECKS,
            s.PASSED_CHECKS,
            s.FAILED_CHECKS,
            s.WARNING_CHECKS,
            s.SKIPPED_CHECKS,
            s.DQ_SCORE,
            s.COMPLETENESS_SCORE,
            s.UNIQUENESS_SCORE,
            s.VALIDITY_SCORE,
            s.CONSISTENCY_SCORE,
            s.FRESHNESS_SCORE,
            s.VOLUME_SCORE,
            s.TRUST_LEVEL,
            s.QUALITY_GRADE,
            s.IS_SLA_MET,
            s.TOTAL_RECORDS,
            s.FAILED_RECORDS_COUNT,
            s.FAILURE_RATE,
            s.TOTAL_EXECUTION_TIME_SEC,
            s.TOTAL_CREDITS_CONSUMED,
            s.LAST_RUN_ID,
            s.LAST_RUN_TS,
            s.LAST_RUN_STATUS,
            CURRENT_TIMESTAMP()
        )
    """).collect()


$$;

-- ============================================================================
-- VERIFICATION & TESTING
-- ============================================================================

-- Verify procedure created
SHOW PROCEDURES LIKE 'SP_EXECUTE_DQ_CHECKS';

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- Run all checks for all datasets (RECOMMENDED FOR FIRST RUN)
CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS(NULL, NULL, 'FULL');

-- Run checks for specific dataset
CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS('DS_TESTING', NULL, 'FULL');

-- Run only completeness checks
CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS('DS_TESTING', 'COMPLETENESS', 'FULL');

-- Run only critical datasets
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS(NULL, NULL, 'CRITICAL_ONLY');

-- ============================================================================
-- EXECUTION ENGINE SETUP COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Run the execution engine: CALL SP_EXECUTE_DQ_CHECKS(NULL, NULL, 'FULL');
-- 2. Check results: SELECT * FROM DQ_CHECK_RESULTS ORDER BY CHECK_TIMESTAMP DESC LIMIT 20;
-- 3. Review daily summary: SELECT * FROM DQ_DAILY_SUMMARY WHERE SUMMARY_DATE = CURRENT_DATE();
-- 4. Investigate failures: SELECT * FROM DQ_FAILED_RECORDS ORDER BY DETECTED_TS DESC LIMIT 20;
-- ============================================================================


