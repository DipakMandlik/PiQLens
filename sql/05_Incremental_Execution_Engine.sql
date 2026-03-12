-- ============================================================================
-- INCREMENTAL DATA QUALITY EXECUTION ENGINE (v1.0)
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_ENGINE;
USE WAREHOUSE DQ_ANALYTICS_WH;




-- ============================================================================
-- MAIN INCREMENTAL EXECUTION PROCEDURE (v3.0)
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_EXECUTE_DQ_CHECKS_INCREMENTAL(
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

# =============================================================================
# CONSTANTS
# =============================================================================
INCREMENTAL_RUN_TYPE = 'INCREMENTAL'
SCAN_SCOPE_FULL = 'FULL'
SCAN_SCOPE_INCREMENTAL = 'INCREMENTAL'


def main(session: snowpark.Session, p_dataset_id: str, p_rule_type: str, p_run_mode: str) -> str:
    """
    Incremental Data Quality Check Execution Engine (v3.0)
    
    Configuration-driven deterministic execution.
    
    For each dataset:
        1. Read INCREMENTAL_COLUMN from DATASET_CONFIG
        2. If NULL Ã¢â€ â€™ execute full table logic (SCAN_SCOPE = 'FULL')
        3. If NOT NULL Ã¢â€ â€™ count rows where column >= CURRENT_DATE()
           a. Rows > 0 Ã¢â€ â€™ execute incremental logic (SCAN_SCOPE = 'INCREMENTAL')
           b. Rows = 0 Ã¢â€ â€™ skip dataset gracefully
    
    Parameters:
        p_dataset_id: Filter by specific dataset (NULL = all datasets)
        p_rule_type: Filter by rule type (NULL = all types)
        p_run_mode: Execution mode (FULL | CRITICAL_ONLY)
    
    Returns:
        JSON string with execution summary
    """
    
    run_id = None
    
    try:
        # -----------------------------------------------------------------
        # 1. Initialize run
        # -----------------------------------------------------------------
        session.sql("ALTER SESSION SET TIMEZONE = 'Asia/Kolkata'").collect()
        current_ts = session.sql("SELECT TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS')").collect()[0][0]

        run_id = "DQ_RUN_" + current_ts + "_" + str(uuid.uuid4())[:8]

        start_time = datetime.now()
        triggered_by = session.sql("SELECT CURRENT_USER()").collect()[0][0]

        print(f"\n{'='*80}")
        print(f"INCREMENTAL DATA QUALITY EXECUTION ENGINE (v3.0)")
        print(f"{'='*80}")
        print(f"Run ID:           {run_id}")
        print(f"Run Type:         {INCREMENTAL_RUN_TYPE}")
        print(f"Start Time:       {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Triggered By:     {triggered_by}")
        print(f"Dataset Filter:   {p_dataset_id or 'ALL'}")
        print(f"Rule Type Filter: {p_rule_type or 'ALL'}")
        print(f"Run Mode:         {p_run_mode}")
        print(f"{'='*80}\n")

        # -----------------------------------------------------------------
        # 2. Insert run control record
        # -----------------------------------------------------------------
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
                '{INCREMENTAL_RUN_TYPE}',
                0, 0, 0, 0, 0, 0,
                CURRENT_TIMESTAMP()
            )
        """).collect()

        # -----------------------------------------------------------------
        # 3. Fetch datasets (with INCREMENTAL_COLUMN)
        # -----------------------------------------------------------------
        datasets = fetch_datasets(session, p_dataset_id, p_run_mode)

        if not datasets:
            raise Exception("No active datasets found")

        print(f"Ã°Å¸â€œÅ  Processing {len(datasets)} dataset(s) in INCREMENTAL mode\n")

        # -----------------------------------------------------------------
        # 4. Initialize counters (clean, minimal)
        # -----------------------------------------------------------------
        stats = {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'warning_checks': 0,
            'skipped_checks': 0,
            'total_records_processed': 0,
            'total_invalid_records': 0,
            'datasets_processed': 0,
            'datasets_skipped': 0,
            'row_level_records_processed': 0,
            'total_rows_in_scope': 0
        }

        # -----------------------------------------------------------------
        # 5. Process each dataset (deterministic flow)
        # -----------------------------------------------------------------
        for dataset in datasets:
            process_dataset_incremental(session, run_id, dataset, p_rule_type, stats)

        # -----------------------------------------------------------------
        # 6. Finalize run
        # -----------------------------------------------------------------
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        run_status = 'COMPLETED' if stats['failed_checks'] == 0 else 'COMPLETED_WITH_FAILURES'

        session.sql(f"""
            UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
            SET
                END_TS = CURRENT_TIMESTAMP(),
                DURATION_SECONDS = {duration},
                RUN_STATUS = '{run_status}',
                TOTAL_DATASETS = {stats['datasets_processed']},
                TOTAL_CHECKS = {stats['total_checks']},
                PASSED_CHECKS = {stats['passed_checks']},
                FAILED_CHECKS = {stats['failed_checks']},
                WARNING_CHECKS = {stats['warning_checks']},
                SKIPPED_CHECKS = {stats['skipped_checks']},
                ROW_LEVEL_RECORDS_PROCESSED = {stats['row_level_records_processed']},
                TOTAL_ROWS_IN_SCOPE = {stats['total_rows_in_scope']},
                ERROR_MESSAGE = NULL
            WHERE RUN_ID = '{run_id}'
        """).collect()

        # -----------------------------------------------------------------
        # 7. Print summary
        # -----------------------------------------------------------------
        print(f"\n{'='*80}")
        print(f"INCREMENTAL EXECUTION SUMMARY")
        print(f"{'='*80}")
        print(f"Status:               {run_status}")
        print(f"Duration:             {duration:.2f} seconds")
        print(f"Datasets Processed:   {stats['datasets_processed']}")
        print(f"Datasets Skipped:     {stats['datasets_skipped']}")
        print(f"Total Checks:         {stats['total_checks']}")
        print(f"Ã¢Å“â€œ Passed:             {stats['passed_checks']}")
        print(f"Ã¢Å“â€” Failed:             {stats['failed_checks']}")
        print(f"Ã¢Å¡Â  Warnings:           {stats['warning_checks']}")
        print(f"Ã¢Å Ëœ Skipped:            {stats['skipped_checks']}")
        print(f"Records Processed:    {stats['total_records_processed']:,}")
        print(f"Row-Level Processed:  {stats['row_level_records_processed']:,}")
        print(f"Invalid Records:      {stats['total_invalid_records']:,}")
        print(f"Rows in Scope:        {stats['total_rows_in_scope']:,}")

        if stats['total_checks'] > 0:
            pass_rate = round((stats['passed_checks'] / stats['total_checks'] * 100), 2)
            print(f"Overall Pass Rate:    {pass_rate}%")

        print(f"{'='*80}\n")

        # -----------------------------------------------------------------
        # 8. Generate daily summary (MERGE-based, idempotent)
        # -----------------------------------------------------------------
        try:
            generate_daily_summary_incremental(session, run_id)
            print("Ã¢Å“â€œ Daily summary updated (MERGE-based, multi-run safe)\n")
        except Exception as e:
            print(f"Ã¢Å¡Â  Warning: Daily summary generation failed: {str(e)}\n")

        # -----------------------------------------------------------------
        # 9. Return result
        # -----------------------------------------------------------------
        result = {
            'run_id': run_id,
            'run_type': INCREMENTAL_RUN_TYPE,
            'status': run_status,
            'duration_seconds': duration,
            'datasets_processed': stats['datasets_processed'],
            'datasets_skipped': stats['datasets_skipped'],
            'total_checks': stats['total_checks'],
            'passed': stats['passed_checks'],
            'failed': stats['failed_checks'],
            'warnings': stats['warning_checks'],
            'skipped': stats['skipped_checks'],
            'records_processed': stats['total_records_processed'],
            'invalid_records': stats['total_invalid_records'],
            'row_level_records_processed': stats['row_level_records_processed'],
            'total_rows_in_scope': stats['total_rows_in_scope'],
            'pass_rate': round((stats['passed_checks'] / stats['total_checks'] * 100), 2) if stats['total_checks'] > 0 else 0
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        error_msg = str(e)
        print(f"\nÃ¢ÂÅ’ FATAL ERROR: {error_msg}\n")

        if run_id:
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

        return json.dumps({
            'run_id': run_id or 'UNKNOWN',
            'run_type': INCREMENTAL_RUN_TYPE,
            'status': 'FAILED',
            'error': error_msg
        })


# =============================================================================
# DATASET FETCHING (with INCREMENTAL_COLUMN)
# =============================================================================

def fetch_datasets(session, p_dataset_id, p_run_mode):
    """
    Fetch active datasets with their INCREMENTAL_COLUMN configuration.
    
    This is the single source of truth for incremental behavior:
    - INCREMENTAL_COLUMN IS NULL Ã¢â€ â€™ full scan
    - INCREMENTAL_COLUMN IS NOT NULL Ã¢â€ â€™ incremental scan
    """

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
            dc.INCREMENTAL_COLUMN,
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


# =============================================================================
# DATASET PROCESSING (v3.0 Ã¢â‚¬â€ Deterministic Two-Branch Logic)
# =============================================================================

def process_dataset_incremental(session, run_id, dataset, p_rule_type, stats):
    """
    Process all rules for a single dataset Ã¢â‚¬â€ INCREMENTAL mode (v3.0).
    
    Deterministic flow:
        1. Read INCREMENTAL_COLUMN from dataset config
        2. If NULL Ã¢â€ â€™ full table scan (SCAN_SCOPE = 'FULL')
        3. If NOT NULL Ã¢â€ â€™ validate column exists + datatype is DATE/TIMESTAMP
        4. Count rows >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP())
           a. Rows > 0 Ã¢â€ â€™ incremental execution (SCAN_SCOPE = 'INCREMENTAL')
           b. Rows = 0 Ã¢â€ â€™ skip dataset
    
    No inference. No fallback. No metadata-driven override.
    Configuration errors are logged as ERROR records in DQ_CHECK_RESULTS.
    """

    dataset_id      = dataset['DATASET_ID']
    source_db       = dataset['SOURCE_DATABASE']
    source_schema   = dataset['SOURCE_SCHEMA']
    source_table    = dataset['SOURCE_TABLE']
    business_domain = dataset['BUSINESS_DOMAIN']
    incremental_col = dataset['INCREMENTAL_COLUMN']
    primary_key_col = dataset['PRIMARY_KEY_COLUMN']

    fqn = f"{source_db}.{source_schema}.{source_table}"

    print(f"Ã°Å¸â€œÂ Dataset: {dataset_id}")
    print(f"   Table:   {fqn}")
    print(f"   Domain:  {business_domain}")
    print(f"   Incremental Column: {incremental_col or 'NULL (full scan)'}")

    # =====================================================================
    # CONDITION 1: INCREMENTAL_COLUMN IS NULL Ã¢â€ â€™ Full Table Scan
    # =====================================================================
    if incremental_col is None:
        print(f"   Ã°Å¸â€Â Mode: FULL SCAN (no incremental column configured)")
        scan_scope = SCAN_SCOPE_FULL

        # Count total rows in table
        try:
            row_count_result = session.sql(f"""
                SELECT COUNT(*) AS ROW_COUNT FROM {fqn}
            """).collect()[0]
            rows_in_scope = int(row_count_result['ROW_COUNT'])
        except Exception as e:
            print(f"   Ã¢ÂÅ’ ERROR: Could not count rows in {fqn}: {str(e)}")
            stats['datasets_skipped'] += 1
            print()
            return

        print(f"   Ã°Å¸â€œÅ  Rows in scope: {rows_in_scope:,} (full table)")
        stats['total_rows_in_scope'] += rows_in_scope
        stats['row_level_records_processed'] += rows_in_scope
        stats['datasets_processed'] += 1

        # Fetch and execute rules
        rules = fetch_rules(session, dataset_id, business_domain, p_rule_type)
        if not rules:
            print(f"   Ã¢Å¡Â  No active rules configured\n")
            return

        print(f"   Rules: {len(rules)}\n")

        for rule in rules:
            execute_rule(
                session, run_id, dataset_id,
                source_db, source_schema, source_table,
                rule, business_domain, stats,
                scan_scope, fqn,
                incremental_col=None, use_incremental_filter=False, primary_key_col=primary_key_col
            )

        print()
        return

    # =====================================================================
    # CONDITION 2: INCREMENTAL_COLUMN IS NOT NULL Ã¢â€ â€™ Incremental Logic
    # =====================================================================

    # Step 2a-PRE: Validate column name is safe (no SQL injection via config)
    if not re.match(r'^[A-Za-z0-9_]+$', incremental_col):
        error_msg = f"INCREMENTAL_COLUMN '{incremental_col}' contains invalid characters. Only [A-Za-z0-9_] allowed."
        print(f"   Ã¢ÂÅ’ ERROR: {error_msg}")
        log_config_error(session, run_id, dataset_id, source_db, source_schema,
                         source_table, error_msg, SCAN_SCOPE_INCREMENTAL)
        stats['datasets_skipped'] += 1
        print()
        return

    # Step 2a: Validate that the configured column exists AND has valid datatype
    try:
        col_meta = session.sql(f"""
            SELECT DATA_TYPE
            FROM {source_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE UPPER(TABLE_SCHEMA) = UPPER('{source_schema}')
              AND UPPER(TABLE_NAME) = UPPER('{source_table}')
              AND UPPER(COLUMN_NAME) = UPPER('{incremental_col}')
        """).collect()

        # --- Column does not exist ---
        if len(col_meta) == 0:
            error_msg = f"Configured INCREMENTAL_COLUMN '{incremental_col}' does not exist in {fqn}"
            print(f"   Ã¢ÂÅ’ ERROR: {error_msg}")
            log_config_error(session, run_id, dataset_id, source_db, source_schema,
                             source_table, error_msg, SCAN_SCOPE_INCREMENTAL)
            stats['datasets_skipped'] += 1
            print()
            return

        # --- Datatype validation (DATE / TIMESTAMP only) ---
        ALLOWED_TYPES = ('DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ')
        col_datatype = col_meta[0]['DATA_TYPE']
        if col_datatype not in ALLOWED_TYPES:
            error_msg = (f"INCREMENTAL_COLUMN '{incremental_col}' has invalid datatype "
                         f"'{col_datatype}' in {fqn}. Allowed: {', '.join(ALLOWED_TYPES)}")
            print(f"   Ã¢ÂÅ’ ERROR: {error_msg}")
            log_config_error(session, run_id, dataset_id, source_db, source_schema,
                             source_table, error_msg, SCAN_SCOPE_INCREMENTAL)
            stats['datasets_skipped'] += 1
            print()
            return

        print(f"   Ã¢Å“â€œ Column validated: {incremental_col} ({col_datatype})")

    except Exception as e:
        error_msg = f"Could not validate column '{incremental_col}': {str(e)}"
        print(f"   Ã¢ÂÅ’ ERROR: {error_msg}")
        log_config_error(session, run_id, dataset_id, source_db, source_schema,
                         source_table, error_msg, SCAN_SCOPE_INCREMENTAL)
        stats['datasets_skipped'] += 1
        print()
        return

    # Step 2b: Count rows where incremental_column >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP())
    try:
        row_count_result = session.sql(f"""
            SELECT COUNT(*) AS ROW_COUNT
            FROM IDENTIFIER('{source_db}.{source_schema}.{source_table}')
            WHERE {incremental_col} IS NOT NULL
              AND {incremental_col} >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP())
        """).collect()[0]
        incremental_row_count = int(row_count_result['ROW_COUNT'])
    except Exception as e:
        error_msg = f"Incremental row count failed for column '{incremental_col}': {str(e)}"
        print(f"   Ã¢ÂÅ’ ERROR: {error_msg}")
        log_config_error(session, run_id, dataset_id, source_db, source_schema,
                         source_table, error_msg, SCAN_SCOPE_INCREMENTAL)
        stats['datasets_skipped'] += 1
        print()
        return

    # -----------------------------------------------------------------
    # Case B: No Rows Found Ã¢â€ â€™ Skip
    # -----------------------------------------------------------------
    if incremental_row_count == 0:
        print(f"   Ã¢Å Ëœ SKIPPED - No rows found where {incremental_col} >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP())")
        stats['datasets_skipped'] += 1
        print()
        return

    # -----------------------------------------------------------------
    # Case A: Rows Found Ã¢â€ â€™ Execute Incremental
    # -----------------------------------------------------------------
    scan_scope = SCAN_SCOPE_INCREMENTAL
    print(f"   Ã°Å¸â€Â Mode: INCREMENTAL (column: {incremental_col})")
    print(f"   Ã°Å¸â€œÅ  Rows in scope: {incremental_row_count:,} (today's data)")

    stats['total_rows_in_scope'] += incremental_row_count
    stats['row_level_records_processed'] += incremental_row_count
    stats['datasets_processed'] += 1

    # Fetch and execute rules
    rules = fetch_rules(session, dataset_id, business_domain, p_rule_type)
    if not rules:
        print(f"   Ã¢Å¡Â  No active rules configured\n")
        return

    print(f"   Rules: {len(rules)}\n")

    for rule in rules:
        execute_rule(
            session, run_id, dataset_id,
            source_db, source_schema, source_table,
            rule, business_domain, stats,
            scan_scope, fqn,
            incremental_col=incremental_col, use_incremental_filter=True, primary_key_col=primary_key_col
        )

    print()


# =============================================================================
# RULE EXECUTION
# =============================================================================

def execute_rule(session, run_id, dataset_id, source_db, source_schema,
                 source_table, rule, business_domain, stats,
                 scan_scope, fqn, incremental_col, use_incremental_filter, primary_key_col):
    """
    Execute a single DQ rule and record the result.
    
    Parameters:
        scan_scope: 'FULL' or 'INCREMENTAL'
        fqn: Fully qualified table name
        incremental_col: Column name for incremental filtering (or None)
        use_incremental_filter: Whether to apply incremental subquery wrapping
    """

    stats['total_checks'] += 1
    check_start = datetime.now()

    try:
        # Build base SQL from template
        base_sql = build_check_sql(rule, source_db, source_schema, source_table)

        # Apply incremental wrapping if needed
        if use_incremental_filter and incremental_col:
            exec_sql = wrap_sql_with_incremental_subquery(base_sql, fqn, incremental_col)
        else:
            exec_sql = base_sql

        # Execute the check
        result = session.sql(exec_sql).collect()[0]

        total_count = int(result['TOTAL_COUNT'])
        error_count = int(result['ERROR_COUNT'])

        # -------------------------------------------------------------
        # Zero qualifying rows Ã¢â€ â€™ Skip silently (no record logged)
        # Per Zero Inference Policy: Do NOT create zero-row check records.
        # -------------------------------------------------------------
        if total_count == 0:
            stats['skipped_checks'] += 1
            col_display = f"[{rule['COLUMN_NAME']}]" if rule['COLUMN_NAME'] else "[TABLE]"
            print(f"   Ã¢Å Ëœ {rule['RULE_TYPE']:15s} {col_display:25s} SKIPPED  (0 qualifying rows)")
            return

        # -------------------------------------------------------------
        # Normal metrics calculation
        # -------------------------------------------------------------
        valid_count = total_count - error_count

        # Null count for COMPLETENESS checks
        null_count = 0
        if rule['RULE_TYPE'] == 'COMPLETENESS' and rule['COLUMN_NAME']:
            if use_incremental_filter and incremental_col:
                null_sql = f"""
                    SELECT COUNT(*) - COUNT({rule['COLUMN_NAME']}) AS NULL_COUNT
                    FROM (
                        SELECT * FROM {fqn}
                        WHERE {incremental_col} IS NOT NULL
                          AND {incremental_col} >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP())
                    ) inc
                """
            else:
                null_sql = f"""
                    SELECT COUNT(*) - COUNT({rule['COLUMN_NAME']}) AS NULL_COUNT
                    FROM {fqn}
                """
            null_result = session.sql(null_sql).collect()[0]
            null_count = int(null_result['NULL_COUNT'])

        # Duplicate count for UNIQUENESS checks
        duplicate_count = 0
        if rule['RULE_TYPE'] == 'UNIQUENESS' and rule['COLUMN_NAME']:
            duplicate_count = error_count

        # Pass rate and status
        pass_rate = round((valid_count / total_count * 100), 2) if total_count > 0 else 100.0
        threshold = float(rule['THRESHOLD_VALUE'])

        if pass_rate >= threshold:
            status = 'PASSED'
        elif pass_rate >= (threshold - 5):
            status = 'WARNING'
        else:
            status = 'FAILED'

        check_end = datetime.now()
        execution_time_ms = (check_end - check_start).total_seconds() * 1000
        execution_credits = round((execution_time_ms / 1000.0) * 0.00001, 6)

        # Failure reason
        failure_reason = None
        if status == 'FAILED':
            failure_reason = f"[{scan_scope}] Pass rate {pass_rate}% below threshold {threshold}%. Found {error_count} invalid records out of {total_count}."
        elif status == 'WARNING':
            failure_reason = f"[{scan_scope}] Pass rate {pass_rate}% within warning range of threshold {threshold}%."

        # -------------------------------------------------------------
        # Insert check result
        # -------------------------------------------------------------
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
                {f"'{failure_reason.replace(chr(39), chr(39)+chr(39))}'" if failure_reason else 'NULL'},
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

        # Capture row-level failure samples (FAILED only)
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
                primary_key_col=primary_key_col,
                fqn=fqn,
                scan_scope=scan_scope,
                incremental_col=incremental_col,
                use_incremental_filter=use_incremental_filter
            )

        # Update stats (row_level_records_processed counted at dataset level, NOT here)
        stats['total_records_processed'] += total_count
        stats['total_invalid_records'] += error_count

        if status == 'PASSED':
            stats['passed_checks'] += 1
            icon = 'Ã¢Å“â€œ'
        elif status == 'FAILED':
            stats['failed_checks'] += 1
            icon = 'Ã¢Å“â€”'
        elif status == 'WARNING':
            stats['warning_checks'] += 1
            icon = 'Ã¢Å¡Â '
        else:
            stats['skipped_checks'] += 1
            icon = 'Ã¢Å Ëœ'

        col_display = f"[{rule['COLUMN_NAME']}]" if rule['COLUMN_NAME'] else "[TABLE]"
        print(f"   {icon} {rule['RULE_TYPE']:15s} {col_display:25s} "
              f"{status:8s} ({pass_rate:6.2f}% | "
              f"{valid_count:,}/{total_count:,})")

    except Exception as e:
        stats['skipped_checks'] += 1
        print(f"   Ã¢Å Ëœ ERROR: {rule['RULE_NAME']} - {str(e)}")
        log_error_check(session, run_id, dataset_id, source_db, source_schema,
                        source_table, rule, str(e), scan_scope)


# =============================================================================
# RULE FETCHING (IDENTICAL TO FULL ENGINE)
# =============================================================================

def fetch_rules(session, dataset_id, business_domain, p_rule_type):
    """Fetch active rules for a dataset (same as FULL engine)"""

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


# =============================================================================
# SQL BUILDING & WRAPPING
# =============================================================================

def build_check_sql(rule, source_db, source_schema, source_table):
    """Build SQL from template (identical to FULL engine)"""
    
    sql_template = rule['SQL_TEMPLATE']
    
    # Replace placeholders
    sql = sql_template.replace('{{DATABASE}}', source_db)
    sql = sql.replace('{{SCHEMA}}', source_schema)
    sql = sql.replace('{{TABLE}}', source_table)
    
    if rule['COLUMN_NAME']:
        sql = sql.replace('{{COLUMN}}', rule['COLUMN_NAME'])
    
    if rule['THRESHOLD_VALUE']:
        sql = sql.replace('{{THRESHOLD}}', str(rule['THRESHOLD_VALUE']))
    
    return sql


def wrap_sql_with_incremental_subquery(base_sql, fqn, incremental_column):
    """
    Wrap SQL with incremental subquery using the configured column.
    
    Replaces all occurrences of the fully qualified table name with a derived
    subquery that pre-filters to today's incremental data.
    
    Example (if incremental_column = 'LOAD_TIMESTAMP'):
        FROM BANKING_DW.BRONZE.STG_CUSTOMER
        Ã¢â€ â€™
        FROM (SELECT * FROM BANKING_DW.BRONZE.STG_CUSTOMER 
              WHERE LOAD_TIMESTAMP IS NOT NULL 
              AND LOAD_TIMESTAMP >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP())) inc
    
    No hardcoded column names. Uses the configured INCREMENTAL_COLUMN.
    Uses DATE_TRUNC('DAY', CURRENT_TIMESTAMP()) for timestamp safety.
    """
    
    incremental_subquery = (
        f"(SELECT * FROM {fqn} "
        f"WHERE {incremental_column} IS NOT NULL "
        f"AND {incremental_column} >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP())) inc"
    )

    modified_sql = _case_insensitive_replace(base_sql, fqn, incremental_subquery)

    return modified_sql


def _case_insensitive_replace(source, target, replacement):
    """Case-insensitive string replacement."""
    source_upper = source.upper()
    target_upper = target.upper()

    result = []
    start = 0

    while True:
        pos = source_upper.find(target_upper, start)
        if pos == -1:
            result.append(source[start:])
            break
        result.append(source[start:pos])
        result.append(replacement)
        start = pos + len(target)

    return ''.join(result)


# =============================================================================
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

    # Fallback for any other rule type: return rows where the column has any value
    # (the actual failure condition is rule-specific, but we capture sample rows)
    return (
        f"SELECT TO_VARCHAR(\"{primary_key_col}\") AS FAILED_RECORD_PK, "
        f"TO_VARCHAR(\"{column_name}\") AS FAILED_VALUE, "
        f"'CHECK_FAILED' AS FAILURE_REASON "
        f"FROM {base_relation} "
        f"WHERE 1=1"
    )

def capture_failed_rows(session, check_id, run_id, dataset_id, source_db, source_schema,
                        source_table, rule, primary_key_col, fqn, scan_scope,
                        incremental_col, use_incremental_filter):
    try:
        pk_col = _safe_identifier_or_none(primary_key_col)
        col_name = _safe_identifier_or_none(rule['COLUMN_NAME']) if rule['COLUMN_NAME'] else None

        if not pk_col:
            print(f"   WARNING: PRIMARY_KEY_COLUMN missing for dataset {dataset_id}; failed-row capture skipped.")
            return

        if use_incremental_filter and incremental_col:
            incr_col = _safe_identifier_or_none(incremental_col)
            if not incr_col:
                return
            base_relation = (
                f"(SELECT * FROM {fqn} "
                f"WHERE \"{incr_col}\" IS NOT NULL "
                f"AND \"{incr_col}\" >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP())) base_src"
            )
        else:
            base_relation = fqn

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

        escaped_reason = f"Rule {rule['RULE_NAME']} failed in {scan_scope} scope.".replace("'", "''")
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

    except Exception as capture_err:
        print(f"   WARNING: Failed-row capture skipped: {str(capture_err)}")

# ERROR LOGGING
# =============================================================================

def log_error_check(session, run_id, dataset_id, source_db, source_schema,
                    source_table, rule, error_msg, scan_scope):
    """Log a rule-level check that failed due to execution error."""
    
    try:
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
                RUN_ID, CHECK_TIMESTAMP, DATASET_ID, DATABASE_NAME, SCHEMA_NAME,
                TABLE_NAME, COLUMN_NAME,
                RULE_ID, RULE_NAME, RULE_TYPE, RULE_LEVEL,
                SCAN_SCOPE,
                TOTAL_RECORDS, VALID_RECORDS, INVALID_RECORDS,
                PASS_RATE, THRESHOLD, CHECK_STATUS,
                EXECUTION_TIME_MS, EXECUTION_CREDITS,
                FAILURE_REASON, CREATED_TS
            ) VALUES (
                '{run_id}', CURRENT_TIMESTAMP(), '{dataset_id}',
                '{source_db}', '{source_schema}', '{source_table}',
                {f"'{rule['COLUMN_NAME']}'" if rule['COLUMN_NAME'] else 'NULL'},
                {rule['RULE_ID']}, '{rule['RULE_NAME']}', '{rule['RULE_TYPE']}',
                '{rule['RULE_LEVEL']}',
                '{scan_scope}',
                0, 0, 0, 0.0, {float(rule['THRESHOLD_VALUE'])},
                'ERROR', 0, 0,
                '{error_msg.replace(chr(39), chr(39)+chr(39))[:4000]}',
                CURRENT_TIMESTAMP()
            )
        """).collect()
    except:
        pass  # Silent fail to avoid cascading errors


def log_config_error(session, run_id, dataset_id, source_db, source_schema,
                     source_table, error_msg, scan_scope):
    """
    Log a dataset-level configuration error as an ERROR record in DQ_CHECK_RESULTS.
    
    This ensures configuration errors (missing column, invalid datatype, filter failure)
    are VISIBLE in the metrics table and not silently swallowed.
    
    ERROR observability requirement: silent skipping is not allowed.
    """
    
    try:
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
                RUN_ID, CHECK_TIMESTAMP, DATASET_ID, DATABASE_NAME, SCHEMA_NAME,
                TABLE_NAME, COLUMN_NAME,
                RULE_ID, RULE_NAME, RULE_TYPE, RULE_LEVEL,
                SCAN_SCOPE,
                TOTAL_RECORDS, VALID_RECORDS, INVALID_RECORDS,
                PASS_RATE, THRESHOLD, CHECK_STATUS,
                EXECUTION_TIME_MS, EXECUTION_CREDITS,
                FAILURE_REASON, CREATED_TS
            ) VALUES (
                '{run_id}', CURRENT_TIMESTAMP(), '{dataset_id}',
                '{source_db}', '{source_schema}', '{source_table}',
                NULL,
                NULL, 'CONFIG_VALIDATION', 'CONFIGURATION', 'TABLE',
                '{scan_scope}',
                0, 0, 0, 0.0, 0.0,
                'ERROR', 0, 0,
                '{error_msg.replace(chr(39), chr(39)+chr(39))[:4000]}',
                CURRENT_TIMESTAMP()
            )
        """).collect()
        print(f"   Ã°Å¸â€œâ€¹ ERROR recorded in DQ_CHECK_RESULTS")
    except Exception as log_err:
        print(f"   Ã¢Å¡Â  Could not log config error: {str(log_err)}")


# =============================================================================
# DAILY SUMMARY GENERATION (MERGE Ã¢â‚¬â€ Idempotent, Multi-Run Safe)
# =============================================================================

def generate_daily_summary_incremental(session, run_id):
    """
    Generate daily summary using MERGE (idempotent, multi-run safe).
    
    Aggregates check results from the current run and merges them into
    DQ_DAILY_SUMMARY. Multiple incremental runs on the same day will be
    correctly aggregated without duplication.
    """
    
    session.sql(f"""
        MERGE INTO DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY AS target
        USING (
            SELECT
                CURRENT_DATE() AS SUMMARY_DATE,
                cr.DATASET_ID,
                cr.DATABASE_NAME,
                cr.SCHEMA_NAME,
                cr.TABLE_NAME,
                dc.BUSINESS_DOMAIN,
                
                -- Check counts
                COUNT(*) AS TOTAL_CHECKS,
                SUM(CASE WHEN cr.CHECK_STATUS = 'PASSED' THEN 1 ELSE 0 END) AS PASSED_CHECKS,
                SUM(CASE WHEN cr.CHECK_STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_CHECKS,
                SUM(CASE WHEN cr.CHECK_STATUS = 'WARNING' THEN 1 ELSE 0 END) AS WARNING_CHECKS,
                SUM(CASE WHEN cr.CHECK_STATUS = 'SKIPPED' THEN 1 ELSE 0 END) AS SKIPPED_CHECKS,
                
                -- Overall DQ score
                ROUND(SUM(cr.VALID_RECORDS)::FLOAT / NULLIF(SUM(cr.TOTAL_RECORDS), 0) * 100, 2 ) AS DQ_SCORE,
                
                -- Dimension scores
                ROUND(SUM(CASE WHEN cr.RULE_TYPE = 'COMPLETENESS' THEN cr.VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN cr.RULE_TYPE = 'COMPLETENESS' THEN cr.TOTAL_RECORDS END), 0) * 100, 2 ) AS COMPLETENESS_SCORE,
                ROUND(SUM(CASE WHEN cr.RULE_TYPE = 'UNIQUENESS' THEN cr.VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN cr.RULE_TYPE = 'UNIQUENESS' THEN cr.TOTAL_RECORDS END), 0) * 100, 2 ) AS UNIQUENESS_SCORE,
                ROUND(SUM(CASE WHEN cr.RULE_TYPE = 'VALIDITY' THEN cr.VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN cr.RULE_TYPE = 'VALIDITY' THEN cr.TOTAL_RECORDS END), 0) * 100, 2 ) AS VALIDITY_SCORE,
                ROUND(SUM(CASE WHEN cr.RULE_TYPE = 'CONSISTENCY' THEN cr.VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN cr.RULE_TYPE = 'CONSISTENCY' THEN cr.TOTAL_RECORDS END), 0) * 100, 2 ) AS CONSISTENCY_SCORE,
                ROUND(SUM(CASE WHEN cr.RULE_TYPE = 'FRESHNESS' THEN cr.VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN cr.RULE_TYPE = 'FRESHNESS' THEN cr.TOTAL_RECORDS END), 0) * 100, 2 ) AS FRESHNESS_SCORE,
                ROUND(SUM(CASE WHEN cr.RULE_TYPE = 'VOLUME' THEN cr.VALID_RECORDS END)::FLOAT / NULLIF(SUM(CASE WHEN cr.RULE_TYPE = 'VOLUME' THEN cr.TOTAL_RECORDS END), 0) * 100, 2 ) AS VOLUME_SCORE,
                
            
                
                -- Trust level
                CASE
                    WHEN AVG(cr.PASS_RATE) >= 95 THEN 'HIGH'
                    WHEN AVG(cr.PASS_RATE) >= 85 THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS TRUST_LEVEL,
                
                -- Quality grade
                CASE
                    WHEN AVG(cr.PASS_RATE) >= 95 THEN 'A'
                    WHEN AVG(cr.PASS_RATE) >= 85 THEN 'B'
                    WHEN AVG(cr.PASS_RATE) >= 75 THEN 'C'
                    WHEN AVG(cr.PASS_RATE) >= 65 THEN 'D'
                    ELSE 'F'
                END AS QUALITY_GRADE,
                
                -- SLA compliance
                CASE WHEN ( SUM(cr.VALID_RECORDS)::FLOAT / NULLIF(SUM(cr.TOTAL_RECORDS), 0) * 100) >= 90 THEN TRUE  ELSE FALSE END AS IS_SLA_MET,
                
                -- Record metrics
                SUM(cr.TOTAL_RECORDS) AS TOTAL_RECORDS,
                SUM(cr.INVALID_RECORDS) AS FAILED_RECORDS_COUNT,
                ROUND((SUM(cr.INVALID_RECORDS)::FLOAT / NULLIF(SUM(cr.TOTAL_RECORDS), 0) * 100), 2) AS FAILURE_RATE,
                
                -- Execution metrics
                ROUND(SUM(cr.EXECUTION_TIME_MS) / 1000.0, 2) AS TOTAL_EXECUTION_TIME_SEC,
                ROUND(SUM(cr.EXECUTION_CREDITS), 6) AS TOTAL_CREDITS_CONSUMED,
                
                -- Run metadata
                '{run_id}' AS LAST_RUN_ID,
                MAX(cr.CHECK_TIMESTAMP) AS LAST_RUN_TS,
                CASE
                    WHEN SUM(CASE WHEN cr.CHECK_STATUS = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN 'COMPLETED_WITH_FAILURES'
                    ELSE 'COMPLETED'
                END AS LAST_RUN_STATUS
                
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
            INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG dc
                ON cr.DATASET_ID = dc.DATASET_ID
            WHERE cr.RUN_ID = '{run_id}'
            GROUP BY
                cr.DATASET_ID, cr.DATABASE_NAME, cr.SCHEMA_NAME,
                cr.TABLE_NAME, dc.BUSINESS_DOMAIN
        ) AS source
        ON target.SUMMARY_DATE = source.SUMMARY_DATE
        AND target.DATASET_ID = source.DATASET_ID
        WHEN MATCHED THEN UPDATE SET
            target.TOTAL_CHECKS = source.TOTAL_CHECKS,
            target.PASSED_CHECKS = source.PASSED_CHECKS,
            target.FAILED_CHECKS = source.FAILED_CHECKS,
            target.WARNING_CHECKS = source.WARNING_CHECKS,
            target.SKIPPED_CHECKS = source.SKIPPED_CHECKS,
            target.DQ_SCORE = source.DQ_SCORE,
            target.COMPLETENESS_SCORE = source.COMPLETENESS_SCORE,
            target.UNIQUENESS_SCORE = source.UNIQUENESS_SCORE,
            target.VALIDITY_SCORE = source.VALIDITY_SCORE,
            target.CONSISTENCY_SCORE = source.CONSISTENCY_SCORE,
            target.FRESHNESS_SCORE = source.FRESHNESS_SCORE,
            target.VOLUME_SCORE = source.VOLUME_SCORE,
            target.TRUST_LEVEL = source.TRUST_LEVEL,
            target.QUALITY_GRADE = source.QUALITY_GRADE,
            target.IS_SLA_MET = source.IS_SLA_MET,
            target.TOTAL_RECORDS = source.TOTAL_RECORDS,
            target.FAILED_RECORDS_COUNT = source.FAILED_RECORDS_COUNT,
            target.FAILURE_RATE = source.FAILURE_RATE,
            target.TOTAL_EXECUTION_TIME_SEC = source.TOTAL_EXECUTION_TIME_SEC,
            target.TOTAL_CREDITS_CONSUMED = source.TOTAL_CREDITS_CONSUMED,
            target.LAST_RUN_ID = source.LAST_RUN_ID,
            target.LAST_RUN_TS = source.LAST_RUN_TS,
            target.LAST_RUN_STATUS = source.LAST_RUN_STATUS,
            target.UPDATED_TS = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            SUMMARY_DATE, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
            BUSINESS_DOMAIN,
            TOTAL_CHECKS, PASSED_CHECKS, FAILED_CHECKS, WARNING_CHECKS, SKIPPED_CHECKS,
            DQ_SCORE, COMPLETENESS_SCORE, UNIQUENESS_SCORE, VALIDITY_SCORE,
            CONSISTENCY_SCORE, FRESHNESS_SCORE, VOLUME_SCORE,
            TRUST_LEVEL, QUALITY_GRADE, IS_SLA_MET,
            TOTAL_RECORDS, FAILED_RECORDS_COUNT, FAILURE_RATE,
            TOTAL_EXECUTION_TIME_SEC, TOTAL_CREDITS_CONSUMED,
            LAST_RUN_ID, LAST_RUN_TS, LAST_RUN_STATUS,
            CREATED_TS
        ) VALUES (
            source.SUMMARY_DATE, source.DATASET_ID, source.DATABASE_NAME, source.SCHEMA_NAME, source.TABLE_NAME,
            source.BUSINESS_DOMAIN,
            source.TOTAL_CHECKS, source.PASSED_CHECKS, source.FAILED_CHECKS, source.WARNING_CHECKS, source.SKIPPED_CHECKS,
            source.DQ_SCORE, source.COMPLETENESS_SCORE, source.UNIQUENESS_SCORE, source.VALIDITY_SCORE,
            source.CONSISTENCY_SCORE, source.FRESHNESS_SCORE, source.VOLUME_SCORE,
            source.TRUST_LEVEL, source.QUALITY_GRADE, source.IS_SLA_MET,
            source.TOTAL_RECORDS, source.FAILED_RECORDS_COUNT, source.FAILURE_RATE,
            source.TOTAL_EXECUTION_TIME_SEC, source.TOTAL_CREDITS_CONSUMED,
            source.LAST_RUN_ID, source.LAST_RUN_TS, source.LAST_RUN_STATUS,
            CURRENT_TIMESTAMP()
        )
    """).collect()

$$;


-- ============================================================================
-- VERIFICATION & TESTING
-- ============================================================================

-- Verify procedure created
SHOW PROCEDURES LIKE 'SP_EXECUTE_DQ_CHECKS_INCREMENTAL';

-- Verify INCREMENTAL_COLUMN added to DATASET_CONFIG
SELECT COLUMN_NAME, DATA_TYPE, COMMENT
FROM DATA_QUALITY_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'DQ_CONFIG'
  AND TABLE_NAME = 'DATASET_CONFIG'
  AND COLUMN_NAME = 'INCREMENTAL_COLUMN';

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- Step 1: Configure incremental columns for your datasets
-- (Only needed once per dataset. NULL = full scan only.)

-- Example: Enable incremental for datasets with LOAD_TIMESTAMP
-- UPDATE DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG
-- SET INCREMENTAL_COLUMN = 'LOAD_TIMESTAMP'
-- WHERE DATASET_ID IN ('DS_BRONZE_CUSTOMER', 'DS_BRONZE_ACCOUNT', 'DS_BRONZE_TRANSACTION');

-- Example: Leave as full scan (INCREMENTAL_COLUMN remains NULL)
-- No action needed for datasets that should always do full scans.

-- Step 2: Run incremental checks

-- Run for ALL datasets (recommended for daily automation)
CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS_INCREMENTAL(NULL, NULL, 'FULL');

-- Run for specific dataset
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS_INCREMENTAL('DS_BRONZE_CUSTOMER', NULL, 'FULL');

-- Run for specific rule type
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS_INCREMENTAL(NULL, 'COMPLETENESS', 'FULL');

-- Run only CRITICAL datasets
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS_INCREMENTAL(NULL, NULL, 'CRITICAL_ONLY');




