-- ============================================================================
-- PROFILING & CUSTOM SCANNING PROCEDURES - PRODUCTION VERSION
-- Pi-Qualytics Data Quality Platform
-- ============================================================================



USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_ENGINE;
USE WAREHOUSE DQ_ANALYTICS_WH;



-- ============================================================================
-- PROCEDURE 1: DATASET PROFILING
-- ============================================================================
-- Purpose: Profile a single dataset with all configured rules
-- Use Case: Deep analysis of one dataset, scheduled profiling jobs
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_PROFILE_DATASET(
    P_DATASET_ID VARCHAR,
    P_RULE_TYPE  VARCHAR DEFAULT NULL,
    P_RUN_MODE   VARCHAR DEFAULT 'FULL'   -- FULL | INCREMENTAL
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
import json
import uuid
import re
from datetime import datetime

SCAN_SCOPE_FULL = 'FULL'
SCAN_SCOPE_INCREMENTAL = 'INCREMENTAL'

def main(session: snowpark.Session, p_dataset_id: str, p_rule_type: str, p_run_mode: str) -> str:
    session.sql("ALTER SESSION SET TIMEZONE = 'Asia/Kolkata'").collect()
    if not p_dataset_id:
        raise Exception("p_dataset_id is required for profiling")

    run_id = f"DQ_PROFILE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    start_time = datetime.now()
    triggered_by = session.sql("SELECT CURRENT_USER()").collect()[0][0]

    try:

        

        # Insert run control
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL (
                RUN_ID, TRIGGERED_BY, START_TS, RUN_STATUS,
                RUN_TYPE,
                TOTAL_DATASETS, TOTAL_CHECKS,
                PASSED_CHECKS, FAILED_CHECKS,
                WARNING_CHECKS, SKIPPED_CHECKS,
                CREATED_TS
            ) VALUES (
                '{run_id}', '{triggered_by}', CURRENT_TIMESTAMP(),
                'RUNNING', '{p_run_mode}',
                0,0,0,0,0,0,
                CURRENT_TIMESTAMP()
            )
        """).collect()

        dataset = fetch_dataset(session, p_dataset_id)

        if not dataset:
            raise Exception(f"No active dataset found for {p_dataset_id}")

        stats = init_stats()

        process_dataset_profile(
            session,
            run_id,
            dataset,
            p_rule_type,
            p_run_mode,
            stats
        )

        finalize_run(session, run_id, start_time, stats)

        generate_daily_summary_for_run(session, run_id)

        return json.dumps(build_result(run_id, stats, start_time), indent=2)

    except Exception as e:
        error_msg = str(e)

        session.sql(f"""
            UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
            SET END_TS = CURRENT_TIMESTAMP(),
                RUN_STATUS = 'FAILED',
                ERROR_MESSAGE = '{error_msg.replace("'", "''")[:4000]}'
            WHERE RUN_ID = '{run_id}'
        """).collect()

        return json.dumps({'run_id': run_id, 'status': 'FAILED', 'error': error_msg})


# =============================================================================
# DATASET FETCH
# =============================================================================

def fetch_dataset(session, dataset_id):
    return session.sql(f"""
        SELECT DATASET_ID,
               SOURCE_DATABASE,
               SOURCE_SCHEMA,
               SOURCE_TABLE,
               BUSINESS_DOMAIN,
               INCREMENTAL_COLUMN
        FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG
        WHERE IS_ACTIVE = TRUE
          AND DATASET_ID = '{dataset_id}'
    """).collect()[0]


# =============================================================================
# DATASET PROCESSING (FULL + INCREMENTAL AWARE)
# =============================================================================

def process_dataset_profile(session, run_id, dataset, p_rule_type, p_run_mode, stats):

    dataset_id      = dataset['DATASET_ID']
    source_db       = dataset['SOURCE_DATABASE']
    source_schema   = dataset['SOURCE_SCHEMA']
    source_table    = dataset['SOURCE_TABLE']
    business_domain = dataset['BUSINESS_DOMAIN']
    incremental_col = dataset['INCREMENTAL_COLUMN']

    fqn = f"{source_db}.{source_schema}.{source_table}"

    # ---------------------------------------------------------
    # Determine SCAN SCOPE
    # ---------------------------------------------------------
    if p_run_mode.upper() == 'INCREMENTAL' and incremental_col:

        if not re.match(r'^[A-Za-z0-9_]+$', incremental_col):
            raise Exception(f"Invalid INCREMENTAL_COLUMN: {incremental_col}")

        scoped_table = f"""
            (SELECT * FROM {fqn}
             WHERE {incremental_col} IS NOT NULL
               AND {incremental_col} >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP()))
        """
        scan_scope = SCAN_SCOPE_INCREMENTAL

    else:
        scoped_table = fqn
        scan_scope = SCAN_SCOPE_FULL

    # ---------------------------------------------------------
    # Fetch rules
    # ---------------------------------------------------------
    rules = fetch_rules(session, dataset_id, business_domain, p_rule_type)

    for rule in rules:
        stats['total_checks'] += 1

        result = execute_check_profile(
            session,
            run_id,
            dataset_id,
            source_db,
            source_schema,
            source_table,
            rule,
            scoped_table,
            scan_scope
        )

        stats['total_records_processed'] += result['total_records']
        stats['total_invalid_records']   += result['failed_records']

        update_status_counters(stats, result['status'])


# =============================================================================
# RULE EXECUTION (SCOPED)
# =============================================================================

def execute_check_profile(session, run_id, dataset_id,
                          source_db, source_schema, source_table,
                          rule, scoped_table, scan_scope):

    sql = build_check_sql_profile(rule, scoped_table)

    result = session.sql(sql).collect()[0]

    total_count = int(result['TOTAL_COUNT'] or 0)
    error_count = int(result['ERROR_COUNT'] or 0)
    valid_count = total_count - error_count

    null_count = 0
    if rule['RULE_TYPE'] == 'COMPLETENESS' and rule['COLUMN_NAME']:
        null_sql = f"""
            SELECT COUNT(*) - COUNT({rule['COLUMN_NAME']}) AS NULL_COUNT
            FROM {scoped_table}
        """
        null_count = int(session.sql(null_sql).collect()[0]['NULL_COUNT'] or 0)

    duplicate_count = error_count if rule['RULE_TYPE'] == 'UNIQUENESS' else 0

    pass_rate = round((valid_count / total_count * 100), 2) if total_count > 0 else 100.0
    threshold = float(rule['THRESHOLD_VALUE'])

    if pass_rate >= threshold:
        status = 'PASSED'
    elif pass_rate >= threshold - 5:
        status = 'WARNING'
    else:
        status = 'FAILED'

    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
            RUN_ID, CHECK_TIMESTAMP,
            DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
            COLUMN_NAME,
            RULE_ID, RULE_NAME, RULE_TYPE, RULE_LEVEL,
            SCAN_SCOPE,
            TOTAL_RECORDS, VALID_RECORDS, INVALID_RECORDS,
            NULL_RECORDS, DUPLICATE_RECORDS,
            PASS_RATE, THRESHOLD,
            CHECK_STATUS,
            CREATED_TS
        ) VALUES (
            '{run_id}', CURRENT_TIMESTAMP(),
            '{dataset_id}', '{source_db}', '{source_schema}', '{source_table}',
            {f"'{rule['COLUMN_NAME']}'" if rule['COLUMN_NAME'] else 'NULL'},
            {rule['RULE_ID']}, '{rule['RULE_NAME']}', '{rule['RULE_TYPE']}', '{rule['RULE_LEVEL']}',
            '{scan_scope}',
            {total_count}, {valid_count}, {error_count},
            {null_count}, {duplicate_count},
            {pass_rate}, {threshold},
            '{status}',
            CURRENT_TIMESTAMP()
        )
    """).collect()

    return {
        'status': status,
        'total_records': total_count,
        'failed_records': error_count
    }


# =============================================================================
# HELPERS
# =============================================================================

def build_check_sql_profile(rule, scoped_table):

    sql = rule['SQL_TEMPLATE']
    sql = sql.replace('{{TABLE}}', scoped_table)

    if rule['COLUMN_NAME']:
        sql = sql.replace('{{COLUMN}}', rule['COLUMN_NAME'])

    sql = sql.replace('{{THRESHOLD}}', str(int(rule['THRESHOLD_VALUE'])))

    return sql


def fetch_rules(session, dataset_id, business_domain, p_rule_type):

    rule_type_filter = f"AND rm.RULE_TYPE = '{p_rule_type}'" if p_rule_type else ""

    return session.sql(f"""
        SELECT drc.DATASET_ID,
               drc.COLUMN_NAME,
               drc.THRESHOLD_VALUE,
               rm.RULE_ID,
               rm.RULE_NAME,
               rm.RULE_TYPE,
               rm.RULE_LEVEL,
               rst.SQL_TEMPLATE
        FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_RULE_CONFIG drc
        JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER rm
          ON drc.RULE_ID = rm.RULE_ID
        JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_SQL_TEMPLATE rst
          ON rm.RULE_ID = rst.RULE_ID
        WHERE drc.DATASET_ID = '{dataset_id}'
          AND drc.IS_ACTIVE = TRUE
          AND rm.IS_ACTIVE = TRUE
          {rule_type_filter}
    """).collect()


def init_stats():
    return {
        'total_checks': 0,
        'passed_checks': 0,
        'failed_checks': 0,
        'warning_checks': 0,
        'skipped_checks': 0,
        'total_records_processed': 0,
        'total_invalid_records': 0
    }


def update_status_counters(stats, status):
    if status == 'PASSED':
        stats['passed_checks'] += 1
    elif status == 'FAILED':
        stats['failed_checks'] += 1
    elif status == 'WARNING':
        stats['warning_checks'] += 1
    else:
        stats['skipped_checks'] += 1


def finalize_run(session, run_id, start_time, stats):
    duration = (datetime.now() - start_time).total_seconds()
    run_status = 'COMPLETED' if stats['failed_checks'] == 0 else 'COMPLETED_WITH_FAILURES'

    session.sql(f"""
        UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
        SET END_TS = CURRENT_TIMESTAMP(),
            DURATION_SECONDS = {duration},
            RUN_STATUS = '{run_status}',
            TOTAL_DATASETS = 1,
            TOTAL_CHECKS = {stats['total_checks']},
            PASSED_CHECKS = {stats['passed_checks']},
            FAILED_CHECKS = {stats['failed_checks']},
            WARNING_CHECKS = {stats['warning_checks']},
            SKIPPED_CHECKS = {stats['skipped_checks']}
        WHERE RUN_ID = '{run_id}'
    """).collect()


def build_result(run_id, stats, start_time):
    duration = (datetime.now() - start_time).total_seconds()
    return {
        'run_id': run_id,
        'status': 'COMPLETED',
        'duration_seconds': duration,
        'total_checks': stats['total_checks'],
        'passed': stats['passed_checks'],
        'failed': stats['failed_checks'],
        'warnings': stats['warning_checks'],
        'records_processed': stats['total_records_processed']
    }


def generate_daily_summary_for_run(session, run_id):
    session.sql(f"""
        DELETE FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        WHERE LAST_RUN_ID = '{run_id}'
    """).collect()

$$;


-- ============================================================================
-- SP_RUN_CUSTOM_RULE — Rewritten for robustness
-- ============================================================================
-- FIX LOG:
--   1. Template placeholders: now handles {TABLE}, {{TABLE}}, {COLUMN}, {THRESHOLD}, etc.
--   2. SQL injection: all user inputs are escaped via safe_str()
--   3. Error handling: comprehensive try/except with safe error persistence
--   4. Cleaner structure: modular helper functions, consistent patterns
-- ============================================================================

CREATE OR REPLACE PROCEDURE DATA_QUALITY_DB.DQ_ENGINE.SP_RUN_CUSTOM_RULE(
    P_DATASET_ID   VARCHAR,
    P_RULE_NAME    VARCHAR,
    P_COLUMN_NAME  VARCHAR DEFAULT NULL,
    P_THRESHOLD    FLOAT   DEFAULT NULL,
    P_RUN_MODE     VARCHAR DEFAULT 'ADHOC'
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
import json
import uuid
import re
from datetime import datetime

# ===================== CONSTANTS =====================

SCAN_SCOPE_FULL = 'FULL'
SCAN_SCOPE_INCREMENTAL = 'INCREMENTAL'

DQ_DATABASE = 'DATA_QUALITY_DB'
DQ_METRICS  = 'DQ_METRICS'
DQ_CONFIG   = 'DQ_CONFIG'
DQ_ENGINE   = 'DQ_ENGINE'


# ===================== SAFETY UTILS =====================

def safe_str(val):
    """Escape single quotes for safe SQL interpolation."""
    if val is None:
        return None
    return str(val).replace("'", "''")


def safe_identifier(name):
    """Validate that a name is a safe SQL identifier (alphanumeric + underscore only)."""
    if not name or not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', str(name)):
        raise Exception(f"Invalid identifier: {name}")
    return str(name)


def quoted_or_null(val):
    """Return a SQL-safe quoted string or NULL literal."""
    if val is None:
        return 'NULL'
    return f"'{safe_str(val)}'"


# ===================== MAIN ENTRY =====================

def main(session, p_dataset_id, p_rule_name, p_column_name, p_threshold, p_run_mode):
    session.sql("ALTER SESSION SET TIMEZONE = 'Asia/Kolkata'").collect()

    # --- Validate inputs ---
    if not p_dataset_id:
        raise Exception("p_dataset_id is required")
    if not p_rule_name:
        raise Exception("p_rule_name is required")

    # Normalize run mode
    run_mode = str(p_run_mode or 'ADHOC').upper().strip()

    # Generate run identifiers
    run_id    = f"DQ_CUSTOM_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    start_time = datetime.now()
    triggered_by = get_current_user(session)

    try:
        # --- 1. Create RUN_CONTROL entry ---
        insert_run_control(session, run_id, triggered_by, run_mode)

        # --- 2. Resolve dataset config ---
        ds = fetch_dataset_config(session, p_dataset_id)
        source_db       = ds['SOURCE_DATABASE']
        source_schema   = ds['SOURCE_SCHEMA']
        source_table    = ds['SOURCE_TABLE']
        incremental_col = ds['INCREMENTAL_COLUMN']

        fqn = f"{safe_identifier(source_db)}.{safe_identifier(source_schema)}.{safe_identifier(source_table)}"

        # --- 3. Determine scan scope ---
        scoped_table, scan_scope = resolve_scan_scope(
            fqn, run_mode, incremental_col
        )

        # --- 4. Fetch rule definition ---
        rule = resolve_rule(
            session, p_dataset_id, p_rule_name, p_column_name, p_threshold, run_mode
        )

        # --- 5. Execute the check ---
        result = execute_check(
            session, run_id, p_dataset_id,
            source_db, source_schema, source_table,
            rule, scoped_table, scan_scope
        )

        # --- 6. Finalize RUN_CONTROL ---
        duration = (datetime.now() - start_time).total_seconds()
        finalize_run_control(session, run_id, result, duration)

        return json.dumps({
            "run_id":          run_id,
            "status":          "COMPLETED" if result['status'] != 'FAILED' else "COMPLETED_WITH_FAILURES",
            "scan_scope":      scan_scope,
            "total_records":   result['total_records'],
            "valid_records":   result['valid_records'],
            "invalid_records": result['failed_records'],
            "pass_rate":       result['pass_rate'],
            "threshold":       rule['THRESHOLD_VALUE']
        })

    except Exception as e:
        error_msg = safe_str(str(e))[:4000]
        try:
            session.sql(f"""
                UPDATE {DQ_DATABASE}.{DQ_METRICS}.DQ_RUN_CONTROL
                SET END_TS        = CURRENT_TIMESTAMP(),
                    RUN_STATUS    = 'FAILED',
                    ERROR_MESSAGE = '{error_msg}'
                WHERE RUN_ID = '{safe_str(run_id)}'
            """).collect()
        except:
            pass  # Don't mask the original error
        return json.dumps({"run_id": run_id, "status": "FAILED", "error": str(e)[:4000]})


# ===================== RUN CONTROL HELPERS =====================

def get_current_user(session):
    try:
        return session.sql("SELECT CURRENT_USER()").collect()[0][0]
    except:
        return 'UNKNOWN'


def insert_run_control(session, run_id, triggered_by, run_mode):
    session.sql(f"""
        INSERT INTO {DQ_DATABASE}.{DQ_METRICS}.DQ_RUN_CONTROL (
            RUN_ID, TRIGGERED_BY, START_TS, RUN_STATUS,
            RUN_TYPE, EXECUTION_MODE,
            TOTAL_DATASETS, TOTAL_CHECKS,
            PASSED_CHECKS, FAILED_CHECKS,
            WARNING_CHECKS, SKIPPED_CHECKS,
            CREATED_TS
        )
        VALUES (
            '{safe_str(run_id)}',
            '{safe_str(triggered_by)}',
            CURRENT_TIMESTAMP(),
            'RUNNING',
            'CUSTOM_SCAN',
            '{safe_str(run_mode)}',
            1, 0, 0, 0, 0, 0,
            CURRENT_TIMESTAMP()
        )
    """).collect()


def finalize_run_control(session, run_id, result, duration):
    status = result['status']
    run_status = 'COMPLETED' if status != 'FAILED' else 'COMPLETED_WITH_FAILURES'
    passed  = 1 if status == 'PASSED'  else 0
    failed  = 1 if status == 'FAILED'  else 0
    warning = 1 if status == 'WARNING' else 0

    session.sql(f"""
        UPDATE {DQ_DATABASE}.{DQ_METRICS}.DQ_RUN_CONTROL
        SET END_TS           = CURRENT_TIMESTAMP(),
            DURATION_SECONDS = {duration:.2f},
            RUN_STATUS       = '{run_status}',
            TOTAL_CHECKS     = 1,
            PASSED_CHECKS    = {passed},
            FAILED_CHECKS    = {failed},
            WARNING_CHECKS   = {warning}
        WHERE RUN_ID = '{safe_str(run_id)}'
    """).collect()


# ===================== DATASET HELPERS =====================

def fetch_dataset_config(session, dataset_id):
    rows = session.sql(f"""
        SELECT SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE,
               BUSINESS_DOMAIN, INCREMENTAL_COLUMN
        FROM {DQ_DATABASE}.{DQ_CONFIG}.DATASET_CONFIG
        WHERE DATASET_ID = '{safe_str(dataset_id)}'
          AND IS_ACTIVE = TRUE
        LIMIT 1
    """).collect()

    if not rows:
        raise Exception(f"No active dataset found for ID: {dataset_id}")
    return rows[0]


# ===================== SCAN SCOPE =====================

def resolve_scan_scope(fqn, run_mode, incremental_col):
    """Returns (scoped_table_expression, scan_scope_label)."""

    if run_mode == 'INCREMENTAL' and incremental_col:
        col = safe_identifier(incremental_col)
        scoped_table = (
            f'(SELECT * FROM {fqn} '
            f'WHERE "{col}" IS NOT NULL '
            f'AND "{col}" >= DATE_TRUNC(\'DAY\', CURRENT_TIMESTAMP()))'
        )
        return scoped_table, SCAN_SCOPE_INCREMENTAL

    return fqn, SCAN_SCOPE_FULL


# ===================== RULE RESOLUTION =====================

def resolve_rule(session, dataset_id, rule_name, column_name, threshold, run_mode):
    """Fetch the rule definition and build a rule object."""

    if threshold is not None and run_mode == 'ADHOC':
        # Ad-hoc with explicit threshold — fetch rule definition directly
        row = fetch_rule_direct(session, rule_name)
        if not row:
            raise Exception(f"Rule '{rule_name}' not found in RULE_MASTER")
        return build_rule_object(row, dataset_id, column_name, float(threshold))

    else:
        # Fetch from dataset-rule mapping
        row = fetch_rule_config(session, dataset_id, rule_name, column_name)
        if not row:
            raise Exception(f"No active rule mapping for rule='{rule_name}', dataset='{dataset_id}'")
        effective_threshold = float(threshold) if threshold is not None else float(row['THRESHOLD_VALUE'])
        return build_rule_object(row, dataset_id, row['COLUMN_NAME'], effective_threshold)


def fetch_rule_direct(session, rule_name):
    rows = session.sql(f"""
        SELECT rm.RULE_ID, rm.RULE_NAME, rm.RULE_TYPE, rm.RULE_LEVEL,
               rst.SQL_TEMPLATE
        FROM {DQ_DATABASE}.{DQ_CONFIG}.RULE_MASTER rm
        JOIN {DQ_DATABASE}.{DQ_CONFIG}.RULE_SQL_TEMPLATE rst
          ON rm.RULE_ID = rst.RULE_ID
        WHERE rm.RULE_NAME = '{safe_str(rule_name)}'
          AND rm.IS_ACTIVE = TRUE
        LIMIT 1
    """).collect()
    return rows[0] if rows else None


def fetch_rule_config(session, dataset_id, rule_name, column_name):
    if column_name:
        col_filter = f"AND drc.COLUMN_NAME = '{safe_str(column_name)}'"
    else:
        col_filter = "AND (drc.COLUMN_NAME IS NULL OR drc.COLUMN_NAME = '')"

    rows = session.sql(f"""
        SELECT drc.COLUMN_NAME, drc.THRESHOLD_VALUE,
               rm.RULE_ID, rm.RULE_NAME, rm.RULE_TYPE, rm.RULE_LEVEL,
               rst.SQL_TEMPLATE
        FROM {DQ_DATABASE}.{DQ_CONFIG}.DATASET_RULE_CONFIG drc
        JOIN {DQ_DATABASE}.{DQ_CONFIG}.RULE_MASTER rm
          ON drc.RULE_ID = rm.RULE_ID
        JOIN {DQ_DATABASE}.{DQ_CONFIG}.RULE_SQL_TEMPLATE rst
          ON rm.RULE_ID = rst.RULE_ID
        WHERE drc.DATASET_ID = '{safe_str(dataset_id)}'
          AND rm.RULE_NAME   = '{safe_str(rule_name)}'
          {col_filter}
          AND drc.IS_ACTIVE = TRUE
        LIMIT 1
    """).collect()
    return rows[0] if rows else None


def build_rule_object(row, dataset_id, column_name, threshold):
    return {
        "DATASET_ID":      dataset_id,
        "COLUMN_NAME":     column_name,
        "THRESHOLD_VALUE": threshold,
        "RULE_ID":         row['RULE_ID'],
        "RULE_NAME":       row['RULE_NAME'],
        "RULE_TYPE":       row['RULE_TYPE'],
        "RULE_LEVEL":      row['RULE_LEVEL'],
        "SQL_TEMPLATE":    row['SQL_TEMPLATE']
    }


# ===================== CHECK EXECUTION =====================

def apply_template(sql_template, scoped_table, column_name, threshold,
                   source_db=None, source_schema=None, source_table=None):
    """
    Replace ALL placeholders in SQL template.
    Supports both {PLACEHOLDER} and {{PLACEHOLDER}} patterns.
    
    KEY LOGIC: If the template already has {DATABASE}/{SCHEMA} placeholders,
    then {TABLE} means the bare table name only. If the template only uses
    {TABLE} without database/schema placeholders, {TABLE} means the full
    qualified name (FQN) or subquery (for incremental scans).
    """
    sql = str(sql_template)

    # Detect if template uses separate DATABASE/SCHEMA placeholders
    has_db_schema = any(
        p in sql for p in [
            '{DATABASE}', '{{DATABASE}}', '{SCHEMA}', '{{SCHEMA}}',
            '{DATABASE_NAME}', '{{DATABASE_NAME}}',
            '{SCHEMA_NAME}', '{{SCHEMA_NAME}}',
            '{SOURCE_DATABASE}', '{{SOURCE_DATABASE}}',
            '{SOURCE_SCHEMA}', '{{SOURCE_SCHEMA}}'
        ]
    )

    # If template has DB/SCHEMA placeholders, TABLE = bare table name
    # If template only has TABLE, TABLE = full FQN or subquery
    if has_db_schema:
        table_value = source_table or scoped_table
    else:
        table_value = scoped_table

    # Build replacement map
    replacements = {
        'TABLE':            table_value,
        'TABLE_NAME':       table_value,
        'SOURCE_TABLE':     source_table or '',
        'COLUMN':           f'"{column_name}"' if column_name else 'NULL',
        'COLUMN_NAME':      f'"{column_name}"' if column_name else 'NULL',
        'THRESHOLD':        str(float(threshold)),
        'DATABASE':         source_db or '',
        'DATABASE_NAME':    source_db or '',
        'SOURCE_DATABASE':  source_db or '',
        'SCHEMA':           source_schema or '',
        'SCHEMA_NAME':      source_schema or '',
        'SOURCE_SCHEMA':    source_schema or '',
        'FQN':              scoped_table,
        'FULL_TABLE':       scoped_table,
        'FILTER':           '1=1',
        'WHERE_CLAUSE':     '1=1',
        'CONDITION':        '1=1',
    }

    # First replace double-brace patterns {{WORD}}, then single-brace {WORD}
    for pattern_fmt in ['{{%s}}', '{%s}']:
        for key, val in replacements.items():
            placeholder = pattern_fmt % key
            sql = sql.replace(placeholder, val)

    # Safety net: catch any remaining {WORD} placeholders via regex
    remaining = re.findall(r'\{([A-Z_]+)\}', sql)
    if remaining:
        for r in remaining:
            sql = sql.replace('{' + r + '}', '')
            sql = sql.replace('{{' + r + '}}', '')

    return sql


def execute_check(session, run_id, dataset_id,
                   source_db, source_schema, source_table,
                   rule, scoped_table, scan_scope):
    """Execute the DQ check SQL and record results."""

    # Build the final SQL from the template
    sql = apply_template(
        rule['SQL_TEMPLATE'],
        scoped_table,
        rule['COLUMN_NAME'],
        rule['THRESHOLD_VALUE'],
        source_db,
        source_schema,
        source_table
    )

    # Execute the check
    try:
        result_rows = session.sql(sql).collect()
    except Exception as e:
        # Include the generated SQL in the error for debugging
        raise Exception(
            f"Check SQL failed for rule '{rule['RULE_NAME']}': {str(e)[:500]}\n"
            f"--- GENERATED SQL ---\n{sql[:2000]}"
        )

    if not result_rows:
        raise Exception(f"Check SQL returned no rows for rule '{rule['RULE_NAME']}'")

    row = result_rows[0]

    # Extract counts — handle both uppercase and mixed-case column names
    total_count = extract_int(row, ['TOTAL_COUNT', 'total_count'], 0)
    error_count = extract_int(row, ['ERROR_COUNT', 'error_count'], 0)
    valid_count = total_count - error_count

    # Compute pass rate and status
    pass_rate = round((valid_count / total_count * 100), 2) if total_count > 0 else 100.0
    threshold = float(rule['THRESHOLD_VALUE'])

    if pass_rate >= threshold:
        status = 'PASSED'
    elif pass_rate >= (threshold - 5):
        status = 'WARNING'
    else:
        status = 'FAILED'

    # Record the check result
    insert_check_result(
        session, run_id, dataset_id,
        source_db, source_schema, source_table,
        rule, scan_scope,
        total_count, valid_count, error_count,
        pass_rate, threshold, status
    )

    return {
        "status":         status,
        "total_records":  total_count,
        "valid_records":  valid_count,
        "failed_records": error_count,
        "pass_rate":      pass_rate
    }


def extract_int(row, key_names, default=0):
    """Safely extract an integer from a Row object, trying multiple key names."""
    for key in key_names:
        try:
            val = row[key]
            if val is not None:
                return int(val)
        except (KeyError, IndexError):
            continue
    return default


def insert_check_result(session, run_id, dataset_id,
                        source_db, source_schema, source_table,
                        rule, scan_scope,
                        total_count, valid_count, error_count,
                        pass_rate, threshold, status):
    session.sql(f"""
        INSERT INTO {DQ_DATABASE}.{DQ_METRICS}.DQ_CHECK_RESULTS (
            RUN_ID, CHECK_TIMESTAMP,
            DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
            COLUMN_NAME,
            RULE_ID, RULE_NAME, RULE_TYPE, RULE_LEVEL,
            SCAN_SCOPE,
            TOTAL_RECORDS, VALID_RECORDS, INVALID_RECORDS,
            PASS_RATE, THRESHOLD,
            CHECK_STATUS,
            CREATED_TS
        ) VALUES (
            '{safe_str(run_id)}',
            CURRENT_TIMESTAMP(),
            '{safe_str(dataset_id)}',
            '{safe_str(source_db)}',
            '{safe_str(source_schema)}',
            '{safe_str(source_table)}',
            {quoted_or_null(rule['COLUMN_NAME'])},
            {int(rule['RULE_ID'])},
            '{safe_str(rule['RULE_NAME'])}',
            '{safe_str(rule['RULE_TYPE'])}',
            '{safe_str(rule['RULE_LEVEL'])}',
            '{safe_str(scan_scope)}',
            {total_count}, {valid_count}, {error_count},
            {pass_rate}, {threshold},
            '{status}',
            CURRENT_TIMESTAMP()
        )
    """).collect()

$$;

-- ============================================================================
-- GRANT EXECUTE
-- ============================================================================
GRANT USAGE ON PROCEDURE DATA_QUALITY_DB.DQ_ENGINE.SP_RUN_CUSTOM_RULE(
    VARCHAR, VARCHAR, VARCHAR, FLOAT, VARCHAR
) TO ROLE PUBLIC;


-- ============================================================================
-- GRANT EXECUTE
-- ============================================================================
GRANT USAGE ON PROCEDURE DATA_QUALITY_DB.DQ_ENGINE.SP_RUN_CUSTOM_RULE(
    VARCHAR, VARCHAR, VARCHAR, FLOAT, VARCHAR
) TO ROLE PUBLIC;


-- ============================================================================
-- GRANT EXECUTE
-- ============================================================================
GRANT USAGE ON PROCEDURE DATA_QUALITY_DB.DQ_ENGINE.SP_RUN_CUSTOM_RULE(
    VARCHAR, VARCHAR, VARCHAR, FLOAT, VARCHAR
) TO ROLE PUBLIC;



-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

GRANT USAGE ON PROCEDURE SP_PROFILE_DATASET(VARCHAR, VARCHAR, VARCHAR) TO ROLE ACCOUNTADMIN;
GRANT USAGE ON PROCEDURE SP_RUN_CUSTOM_RULE(VARCHAR, VARCHAR, VARCHAR, FLOAT, VARCHAR) TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SHOW PROCEDURES LIKE 'SP_%';

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- PROFILING EXAMPLES:
-- Profile entire customer dataset
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_PROFILE_DATASET('DS_BRONZE_CUSTOMER', NULL, 'FULL');

-- Profile only completeness checks
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_PROFILE_DATASET('DS_BRONZE_CUSTOMER', 'COMPLETENESS', 'FULL');

-- CUSTOM RULE EXAMPLES:
-- Run email validation with config threshold
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_RUN_CUSTOM_RULE('DS_BRONZE_CUSTOMER', 'VALIDITY_EMAIL_FORMAT', 'email', NULL, 'ADHOC');

-- Run email validation with custom threshold (98%)
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_RUN_CUSTOM_RULE('DS_BRONZE_CUSTOMER', 'VALIDITY_EMAIL_FORMAT', 'email', 98.0, 'ADHOC');

-- ============================================================================
-- PROFILING & CUSTOM SCANNING SETUP COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Profile a dataset: CALL SP_PROFILE_DATASET('DS_BRONZE_CUSTOMER', NULL, 'FULL');
-- 2. Run custom check: CALL SP_RUN_CUSTOM_RULE('DS_BRONZE_CUSTOMER', 'VALIDITY_EMAIL_FORMAT', 'email', 95.0, 'ADHOC');
-- 3. Check results: SELECT * FROM DQ_CHECK_RESULTS ORDER BY CHECK_TIMESTAMP DESC LIMIT 20;
-- ============================================================================
