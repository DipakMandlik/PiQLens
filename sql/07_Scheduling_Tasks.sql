-- =====================================================
-- Pi-Qualytics Production Migration
-- Version: 17
-- Title: Native Task Scheduler Cutover + Timestamp Integrity
-- =====================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_CONFIG;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- =====================================================
-- Preflight
-- =====================================================
SHOW PARAMETERS LIKE 'TIMEZONE';
SELECT CURRENT_TIMESTAMP() AS CURRENT_TS;

-- =====================================================
-- Canonical Schedule Table (SCAN_SCHEDULES)
-- =====================================================

CREATE TABLE IF NOT EXISTS DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES (
  SCHEDULE_ID VARCHAR(36) PRIMARY KEY,
  DATASET_ID VARCHAR(255),
  DATABASE_NAME VARCHAR(255) NOT NULL,
  SCHEMA_NAME VARCHAR(255) NOT NULL,
  TABLE_NAME VARCHAR(255) NOT NULL,
  SCAN_TYPE VARCHAR(50) NOT NULL DEFAULT 'full',
  RUN_TYPE VARCHAR(30) NOT NULL DEFAULT 'FULL_SCAN',
  EXECUTION_MODE VARCHAR(20) NOT NULL DEFAULT 'SCHEDULED',
  FREQUENCY_TYPE VARCHAR(20) NOT NULL DEFAULT 'DAILY',
  CRON_EXPRESSION VARCHAR(128),
  SCHEDULE_TYPE VARCHAR(20) NOT NULL DEFAULT 'daily',
  SCHEDULE_TIME VARCHAR(10),
  SCHEDULE_DAYS VARIANT,
  TIMEZONE VARCHAR(64) NOT NULL DEFAULT 'Asia/Kolkata',
  NEXT_RUN_AT TIMESTAMP_TZ,
  LAST_RUN_AT TIMESTAMP_TZ,
  STATUS VARCHAR(20) NOT NULL DEFAULT 'active',
  IS_ACTIVE BOOLEAN NOT NULL DEFAULT TRUE,
  IS_RECURRING BOOLEAN NOT NULL DEFAULT TRUE,
  RUN_ONCE BOOLEAN NOT NULL DEFAULT FALSE,
  START_DATE DATE,
  END_DATE DATE,
  SKIP_IF_RUNNING BOOLEAN NOT NULL DEFAULT FALSE,
  ON_FAILURE_ACTION VARCHAR(20) NOT NULL DEFAULT 'continue',
  MAX_FAILURES NUMBER(10,0) NOT NULL DEFAULT 3,
  FAILURE_COUNT NUMBER(10,0) NOT NULL DEFAULT 0,
  NOTIFY_ON_FAILURE BOOLEAN NOT NULL DEFAULT FALSE,
  NOTIFY_ON_SUCCESS BOOLEAN NOT NULL DEFAULT FALSE,
  RETRY_ENABLED BOOLEAN NOT NULL DEFAULT TRUE,
  RETRY_DELAY_MINUTES NUMBER(10,0) NOT NULL DEFAULT 5,
  CUSTOM_CONFIG VARIANT,
  CREATED_BY VARCHAR(255),
  CREATED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS DATASET_ID VARCHAR(255);
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS RUN_TYPE VARCHAR(30) DEFAULT 'FULL_SCAN';
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS EXECUTION_MODE VARCHAR(20) DEFAULT 'SCHEDULED';
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS FREQUENCY_TYPE VARCHAR(20) DEFAULT 'DAILY';
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS CRON_EXPRESSION VARCHAR(128);
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS TIMEZONE VARCHAR(64) DEFAULT 'Asia/Kolkata';
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS NEXT_RUN_AT TIMESTAMP_TZ;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS LAST_RUN_AT TIMESTAMP_TZ;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS FAILURE_COUNT NUMBER(10,0) DEFAULT 0;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS IS_RECURRING BOOLEAN DEFAULT TRUE;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS SCHEDULE_TYPE VARCHAR(20) DEFAULT 'daily';
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS SCHEDULE_DAYS VARIANT;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS CUSTOM_CONFIG VARIANT;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS STATUS VARCHAR(20) DEFAULT 'active';
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS IS_ACTIVE BOOLEAN DEFAULT TRUE;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS RUN_ONCE BOOLEAN DEFAULT FALSE;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS RETRY_ENABLED BOOLEAN DEFAULT TRUE;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS RETRY_DELAY_MINUTES NUMBER(10,0) DEFAULT 5;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP();
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES ADD COLUMN IF NOT EXISTS CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP();

-- =====================================================
-- Execution Journal (idempotency + audit)
-- =====================================================
CREATE TABLE IF NOT EXISTS DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION (
  EXECUTION_ID VARCHAR(36) PRIMARY KEY,
  SCHEDULE_ID VARCHAR(36) NOT NULL,
  DUE_AT TIMESTAMP_TZ NOT NULL,
  IDEMPOTENCY_KEY VARCHAR(255) NOT NULL,
  STATUS VARCHAR(20) NOT NULL,
  RUN_ID VARCHAR(100),
  ATTEMPT_NO NUMBER(10,0) NOT NULL DEFAULT 1,
  ERROR_MESSAGE VARCHAR(4000),
  LOCK_OWNER VARCHAR(255),
  LOCK_EXPIRES_AT TIMESTAMP_TZ,
  STARTED_AT TIMESTAMP_TZ,
  FINISHED_AT TIMESTAMP_TZ,
  CREATED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION ADD COLUMN IF NOT EXISTS DUE_AT TIMESTAMP_TZ;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION ADD COLUMN IF NOT EXISTS LOCK_EXPIRES_AT TIMESTAMP_TZ;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION ADD COLUMN IF NOT EXISTS STARTED_AT TIMESTAMP_TZ;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION ADD COLUMN IF NOT EXISTS FINISHED_AT TIMESTAMP_TZ;
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION ADD COLUMN IF NOT EXISTS CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP();
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION ADD COLUMN IF NOT EXISTS UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP();

-- Direct storage without timezone conversions

ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION CLUSTER BY (STATUS, LOCK_EXPIRES_AT, SCHEDULE_ID);
ALTER TABLE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES CLUSTER BY (IS_ACTIVE, NEXT_RUN_AT);

-- =====================================================
-- Procedure: SP_RUN_DATA_PROFILING
-- Scheduler-safe wrapper that stamps scheduled execution source.
-- =====================================================
USE SCHEMA DQ_METRICS;

CREATE OR REPLACE PROCEDURE SP_RUN_DATA_PROFILING(
    P_DATASET_ID VARCHAR,
    P_DATABASE   VARCHAR,
    P_SCHEMA     VARCHAR,
    P_TABLE      VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import json

def esc(v):
    return str(v).replace("'", "''") if v is not None else None

def main(session, p_dataset_id, p_database, p_schema, p_table):
    dataset_id = p_dataset_id
    session.sql("ALTER SESSION SET TIMEZONE = 'Asia/Kolkata'").collect()
    if not dataset_id and p_database and p_schema and p_table:
        lookup_sql = f"""
            SELECT DATASET_ID
            FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG
            WHERE UPPER(SOURCE_DATABASE) = UPPER('{esc(p_database)}')
              AND UPPER(SOURCE_SCHEMA) = UPPER('{esc(p_schema)}')
              AND UPPER(SOURCE_TABLE) = UPPER('{esc(p_table)}')
            LIMIT 1
        """
        rows = session.sql(lookup_sql).collect()
        if rows:
            dataset_id = rows[0]['DATASET_ID']

    if not dataset_id:
        return {"success": False, "error": "Missing dataset_id"}

    call_sql = f"CALL DATA_QUALITY_DB.DQ_ENGINE.SP_PROFILE_DATASET('{esc(dataset_id)}', NULL, 'FULL')"
    result_rows = session.sql(call_sql).collect()

    run_id = None
    if result_rows:
        raw = list(result_rows[0].asDict().values())[0]
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                run_id = parsed.get('run_id')
            except Exception:
                run_id = None

    if run_id:
        session.sql(f"""
            UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
            SET TRIGGERED_BY = 'SCHEDULED_TASK'
            WHERE RUN_ID = '{esc(run_id)}'
        """).collect()

    return {"success": True, "run_id": run_id, "dataset_id": dataset_id}
$$;

-- =====================================================
-- Procedure: SP_RUN_CUSTOM_CHECKS_BATCH
-- Scheduler-safe wrapper for custom checks.
-- =====================================================
CREATE OR REPLACE PROCEDURE SP_RUN_CUSTOM_CHECKS_BATCH(
    P_DATASET_ID VARCHAR,
    P_DATABASE   VARCHAR,
    P_SCHEMA     VARCHAR,
    P_TABLE      VARCHAR,
    P_CUSTOM_CONFIG VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import json

def esc(v):
    return str(v).replace("'", "''") if v is not None else None

def force_scheduled_source(session, run_id):
    session.sql("ALTER SESSION SET TIMEZONE = 'Asia/Kolkata'").collect()
    if not run_id:
        return
    session.sql(f"""
        UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
        SET TRIGGERED_BY = 'SCHEDULED_TASK'
        WHERE RUN_ID = '{esc(run_id)}'
    """).collect()

def main(session, p_dataset_id, p_database, p_schema, p_table, p_custom_config):
    session.sql("ALTER SESSION SET TIMEZONE = 'Asia/Kolkata'").collect()
    if not p_dataset_id:
        return {"success": False, "error": "Missing dataset_id"}

    selected_rule_ids = []
    if p_custom_config and str(p_custom_config).strip() and str(p_custom_config).strip().upper() != 'NULL':
        try:
            cfg = json.loads(str(p_custom_config))
            if isinstance(cfg, dict) and isinstance(cfg.get('customRules'), list):
                selected_rule_ids = [str(x) for x in cfg.get('customRules')]
        except Exception:
            selected_rule_ids = []

    run_ids = []

    if not selected_rule_ids:
        result_rows = session.sql(f"CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS('{esc(p_dataset_id)}', NULL, 'FULL')").collect()
        if result_rows:
            raw = list(result_rows[0].asDict().values())[0]
            if isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                    rid = parsed.get('run_id')
                    if rid:
                        run_ids.append(rid)
                except Exception:
                    pass
    else:
        id_list = ",".join([f"'{esc(x)}'" for x in selected_rule_ids])
        rule_rows = session.sql(f"""
            SELECT RULE_ID, RULE_NAME
            FROM DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER
            WHERE CAST(RULE_ID AS VARCHAR) IN ({id_list})
              AND IS_ACTIVE = TRUE
        """).collect()

        for r in rule_rows:
            rule_name = r['RULE_NAME']
            call_sql = f"CALL DATA_QUALITY_DB.DQ_ENGINE.SP_RUN_CUSTOM_RULE('{esc(p_dataset_id)}', '{esc(rule_name)}', NULL, NULL, 'SCHEDULED')"
            rows = session.sql(call_sql).collect()
            if rows:
                raw = list(rows[0].asDict().values())[0]
                if isinstance(raw, str):
                    try:
                        parsed = json.loads(raw)
                        rid = parsed.get('run_id')
                        if rid:
                            run_ids.append(rid)
                    except Exception:
                        pass

    for rid in run_ids:
        force_scheduled_source(session, rid)

    return {"success": True, "run_ids": run_ids, "count": len(run_ids)}
$$;

-- =====================================================
-- Procedure: SP_PROCESS_DUE_SCHEDULES
-- Main deterministic scheduler processor with idempotency journal.
-- =====================================================
CREATE OR REPLACE PROCEDURE SP_PROCESS_DUE_SCHEDULES(
    P_FORCE_SCHEDULE_ID VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import hashlib
import json
import uuid

def esc(v):
    return str(v).replace("'", "''") if v is not None else None

def safe_str(v, d=''):
    return d if v is None else str(v)

def next_run_expression(schedule_type, base_ts_sql):
    st = safe_str(schedule_type, 'daily').lower()
    if st == 'hourly':
        return f"DATEADD(hour, 1, {base_ts_sql})"
    if st == 'weekly':
        return f"DATEADD(week, 1, {base_ts_sql})"
    return f"DATEADD(day, 1, {base_ts_sql})"

def main(session, p_force_schedule_id):
    session.sql("ALTER SESSION SET TIMEZONE = 'Asia/Kolkata'").collect()
    where_force = ""
    if p_force_schedule_id:
        where_force = f" AND SCHEDULE_ID = '{esc(p_force_schedule_id)}' "

    due_sql = f"""
        SELECT
          SCHEDULE_ID,
          DATASET_ID,
          DATABASE_NAME,
          SCHEMA_NAME,
          TABLE_NAME,
          SCAN_TYPE,
          RUN_TYPE,
          SCHEDULE_TYPE,
          IS_RECURRING,
          RUN_ONCE,
          RETRY_ENABLED,
          RETRY_DELAY_MINUTES,
          MAX_FAILURES,
          FAILURE_COUNT,
          CUSTOM_CONFIG,
          NEXT_RUN_AT
        FROM DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
        WHERE COALESCE(IS_ACTIVE, TRUE) = TRUE
          AND COALESCE(STATUS, 'active') = 'active'
          AND NEXT_RUN_AT <= CURRENT_TIMESTAMP()
          AND (START_DATE IS NULL OR START_DATE <= CURRENT_DATE())
          AND (END_DATE IS NULL OR END_DATE >= CURRENT_DATE())
          {where_force}
        ORDER BY NEXT_RUN_AT ASC
        LIMIT 50
    """

    due_rows = session.sql(due_sql).collect()
    if not due_rows:
        return {
          "executed": 0,
          "skipped": 0,
          "failed": 0,
          "message": "No schedules due"
        }

    executed = 0
    skipped = 0
    failed = 0
    messages = []

    for row in due_rows:
        schedule_id = row['SCHEDULE_ID']
        dataset_id = row['DATASET_ID']
        db = row['DATABASE_NAME']
        sch = row['SCHEMA_NAME']
        tbl = row['TABLE_NAME']
        scan_type = safe_str(row['SCAN_TYPE'], 'full').lower()
        run_type = safe_str(row['RUN_TYPE'], 'FULL_SCAN').upper()
        schedule_type = safe_str(row['SCHEDULE_TYPE'], 'daily')
        is_recurring = bool(row['IS_RECURRING']) if row['IS_RECURRING'] is not None else True
        run_once = bool(row['RUN_ONCE']) if row['RUN_ONCE'] is not None else False
        max_failures = int(row['MAX_FAILURES']) if row['MAX_FAILURES'] is not None else 3
        failure_count = int(row['FAILURE_COUNT']) if row['FAILURE_COUNT'] is not None else 0
        retry_enabled = bool(row['RETRY_ENABLED']) if row['RETRY_ENABLED'] is not None else True
        retry_delay_minutes = int(row['RETRY_DELAY_MINUTES']) if row['RETRY_DELAY_MINUTES'] is not None else 5

        due_at_text = safe_str(row['NEXT_RUN_AT'])
        idem = hashlib.sha256(f"{schedule_id}|{due_at_text}".encode("utf-8")).hexdigest()
        execution_id = str(uuid.uuid4())

        existing = session.sql(f"""
            SELECT EXECUTION_ID, STATUS, ATTEMPT_NO
            FROM DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION
            WHERE IDEMPOTENCY_KEY = '{esc(idem)}'
            LIMIT 1
        """).collect()

        if existing and safe_str(existing[0]['STATUS']).upper() in ('CLAIMED', 'RUNNING', 'SUCCEEDED'):
            skipped += 1
            messages.append(f"Skipped {schedule_id}: idempotency hit")
            continue

        attempt_no = 1
        if existing:
            prior_attempt = int(existing[0]['ATTEMPT_NO']) if existing[0]['ATTEMPT_NO'] is not None else 1
            attempt_no = prior_attempt + 1
            if attempt_no > (max_failures + 1):
                skipped += 1
                messages.append(f"Skipped {schedule_id}: retry exhausted")
                continue

            session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION
                SET STATUS = 'CLAIMED',
                    ATTEMPT_NO = {attempt_no},
                    ERROR_MESSAGE = NULL,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE IDEMPOTENCY_KEY = '{esc(idem)}'
            """).collect()

            execution_id = safe_str(existing[0]['EXECUTION_ID'])
        else:
            session.sql(f"""
                INSERT INTO DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION (
                  EXECUTION_ID, SCHEDULE_ID, DUE_AT, IDEMPOTENCY_KEY,
                  STATUS, RUN_ID, ATTEMPT_NO, ERROR_MESSAGE,
                  CREATED_AT, UPDATED_AT
                ) VALUES (
                  '{esc(execution_id)}',
                  '{esc(schedule_id)}',
                  '{esc(due_at_text)}',
                  '{esc(idem)}',
                  'CLAIMED',
                  NULL,
                  1,
                  NULL,
                  CURRENT_TIMESTAMP(),
                  CURRENT_TIMESTAMP()
                )
            """).collect()

        session.sql(f"""
            UPDATE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION
            SET STATUS = 'RUNNING',
                STARTED_AT = CURRENT_TIMESTAMP(),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE EXECUTION_ID = '{esc(execution_id)}'
        """).collect()

        try:
            if not dataset_id and db and sch and tbl:
                ds_rows = session.sql(f"""
                    SELECT DATASET_ID
                    FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG
                    WHERE UPPER(SOURCE_DATABASE) = UPPER('{esc(db)}')
                      AND UPPER(SOURCE_SCHEMA) = UPPER('{esc(sch)}')
                      AND UPPER(SOURCE_TABLE) = UPPER('{esc(tbl)}')
                    LIMIT 1
                """).collect()
                if ds_rows:
                    dataset_id = ds_rows[0]['DATASET_ID']

            if not dataset_id:
                raise Exception(f"Dataset not found for schedule {schedule_id}")

            run_id = None

            if run_type == 'INCREMENTAL_SCAN':
                call_rows = session.sql(f"CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS_INCREMENTAL('{esc(dataset_id)}', NULL, 'FULL')").collect()
                if call_rows:
                    raw = list(call_rows[0].asDict().values())[0]
                    if isinstance(raw, str):
                        try:
                            parsed = json.loads(raw)
                            run_id = parsed.get('run_id')
                        except Exception:
                            run_id = None
            elif scan_type in ('profiling', 'anomalies'):
                prof_rows = session.sql(f"CALL DATA_QUALITY_DB.DQ_METRICS.SP_RUN_DATA_PROFILING('{esc(dataset_id)}', '{esc(db)}', '{esc(sch)}', '{esc(tbl)}')").collect()
                if prof_rows:
                    raw = list(prof_rows[0].asDict().values())[0]
                    if isinstance(raw, dict):
                        run_id = raw.get('run_id')
            elif scan_type in ('checks', 'custom'):
                cfg = row['CUSTOM_CONFIG']
                cfg_text = 'NULL'
                if cfg is not None:
                    cfg_text = str(cfg).replace("'", "''")
                chk_rows = session.sql(f"CALL DATA_QUALITY_DB.DQ_METRICS.SP_RUN_CUSTOM_CHECKS_BATCH('{esc(dataset_id)}', '{esc(db)}', '{esc(sch)}', '{esc(tbl)}', '{cfg_text}')").collect()
                if chk_rows:
                    raw = list(chk_rows[0].asDict().values())[0]
                    if isinstance(raw, dict):
                        run_id = raw.get('run_id')
            else:
                prof_rows = session.sql(f"CALL DATA_QUALITY_DB.DQ_METRICS.SP_RUN_DATA_PROFILING('{esc(dataset_id)}', '{esc(db)}', '{esc(sch)}', '{esc(tbl)}')").collect()
                cfg = row['CUSTOM_CONFIG']
                cfg_text = 'NULL'
                if cfg is not None:
                    cfg_text = str(cfg).replace("'", "''")
                chk_rows = session.sql(f"CALL DATA_QUALITY_DB.DQ_METRICS.SP_RUN_CUSTOM_CHECKS_BATCH('{esc(dataset_id)}', '{esc(db)}', '{esc(sch)}', '{esc(tbl)}', '{cfg_text}')").collect()
                if chk_rows:
                    raw = list(chk_rows[0].asDict().values())[0]
                    if isinstance(raw, dict):
                        run_id = raw.get('run_id')

            if run_once or (not is_recurring):
                session.sql(f"""
                    UPDATE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
                    SET LAST_RUN_AT = CURRENT_TIMESTAMP(),
                        NEXT_RUN_AT = NULL,
                        STATUS = 'completed',
                        IS_ACTIVE = FALSE,
                        FAILURE_COUNT = 0,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE SCHEDULE_ID = '{esc(schedule_id)}'
                """).collect()
            else:
                base_ts_sql = "COALESCE(NEXT_RUN_AT, CURRENT_TIMESTAMP())"
                next_expr = next_run_expression(schedule_type, base_ts_sql)
                session.sql(f"""
                    UPDATE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
                    SET LAST_RUN_AT = CURRENT_TIMESTAMP(),
                        NEXT_RUN_AT = {next_expr},
                        STATUS = 'active',
                        IS_ACTIVE = TRUE,
                        FAILURE_COUNT = 0,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE SCHEDULE_ID = '{esc(schedule_id)}'
                """).collect()

            session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION
                SET STATUS = 'SUCCEEDED',
                    RUN_ID = {"NULL" if not run_id else "'" + esc(run_id) + "'"},
                    FINISHED_AT = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE EXECUTION_ID = '{esc(execution_id)}'
            """).collect()

            executed += 1
            messages.append(f"Executed {schedule_id}")

        except Exception as e:
            err = safe_str(e)[:3500].replace("'", "")
            failed += 1
            messages.append(f"Failed {schedule_id}: {err}")

            if retry_enabled and failure_count < max_failures:
                next_failure_expr = f"DATEADD(minute, {retry_delay_minutes}, CURRENT_TIMESTAMP())"
            else:
                next_failure_expr = next_run_expression(schedule_type, "COALESCE(NEXT_RUN_AT, CURRENT_TIMESTAMP())")

            session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
                SET FAILURE_COUNT = COALESCE(FAILURE_COUNT, 0) + 1,
                    NEXT_RUN_AT = {next_failure_expr},
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE SCHEDULE_ID = '{esc(schedule_id)}'
            """).collect()

            session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION
                SET STATUS = 'FAILED',
                    ERROR_MESSAGE = '{esc(err)}',
                    FINISHED_AT = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE EXECUTION_ID = '{esc(execution_id)}'
            """).collect()

    return {
      "executed": executed,
      "skipped": skipped,
      "failed": failed,
      "message": "; ".join(messages[:10]) if messages else "Done"
    }
$$;

-- =====================================================
-- Snowflake Native Task
-- =====================================================
-- CREATE OR REPLACE TASK DQ_SCHEDULE_PROCESSOR_TASK
--     WAREHOUSE = DQ_ANALYTICS_WH
--     SCHEDULE = '1 MINUTE'
--     ALLOW_OVERLAPPING_EXECUTION = FALSE
--     COMMENT = 'Native scheduler task: processes due SCAN_SCHEDULES rows every minute'
-- AS
--     CALL DATA_QUALITY_DB.DQ_METRICS.SP_PROCESS_DUE_SCHEDULES(NULL);

-- ALTER TASK DQ_SCHEDULE_PROCESSOR_TASK RESUME;

-- =====================================================
-- Grants
-- =====================================================
GRANT USAGE ON PROCEDURE DATA_QUALITY_DB.DQ_METRICS.SP_RUN_DATA_PROFILING(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE ACCOUNTADMIN;
GRANT USAGE ON PROCEDURE DATA_QUALITY_DB.DQ_METRICS.SP_RUN_CUSTOM_CHECKS_BATCH(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE ACCOUNTADMIN;
GRANT USAGE ON PROCEDURE DATA_QUALITY_DB.DQ_METRICS.SP_PROCESS_DUE_SCHEDULES(VARCHAR) TO ROLE ACCOUNTADMIN;
GRANT OPERATE ON TASK DATA_QUALITY_DB.DQ_METRICS.DQ_SCHEDULE_PROCESSOR_TASK TO ROLE ACCOUNTADMIN;

-- =====================================================
-- Verification
-- =====================================================
SHOW TASKS LIKE 'DQ_SCHEDULE_PROCESSOR_TASK';

SELECT *
FROM DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
WHERE STATUS = 'active'
ORDER BY NEXT_RUN_AT;

SELECT *
FROM DATA_QUALITY_DB.DQ_CONFIG.DQ_SCHEDULE_EXECUTION
ORDER BY CREATED_AT DESC
LIMIT 50;

SELECT
  NAME,
  STATE,
  SCHEDULED_TIME,
  QUERY_ID,
  ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'DQ_SCHEDULE_PROCESSOR_TASK'
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;

-- =====================================================
-- Rollback Notes (manual)
-- =====================================================
-- ALTER TASK DATA_QUALITY_DB.DQ_METRICS.DQ_SCHEDULE_PROCESSOR_TASK SUSPEND;
-- Keep SCAN_SCHEDULES and DQ_SCHEDULE_EXECUTION for forensic analysis.
