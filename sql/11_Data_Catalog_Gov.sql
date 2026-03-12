-- ==============================================================================
-- PI-QUALYTICS ENTERPRISE DATA CATALOG GOVERNANCE SCRIPT
-- Purpose:     Initializes the governance metadata layer, activity logs, 
--              and role-based access control for the Data Catalog module.
-- Execution:   Run as ACCOUNTADMIN or SYSADMIN.
-- Safe to run: YES (Idempotent - uses IF NOT EXISTS where appropriate)
-- ==============================================================================

USE ROLE ACCOUNTADMIN;

-- ==============================================================================
-- 1. DATABASE & SCHEMA SETUP
-- ==============================================================================
-- Using the existing DATA_QUALITY_DB
USE DATABASE DATA_QUALITY_DB;

CREATE SCHEMA IF NOT EXISTS DATA_CATALOG
    COMMENT = 'Core governance metadata and activity tracking for the Data Catalog';

-- ==============================================================================
-- 2. ENUM/LOOKUP TABLES (Optional but recommended for strictness)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS DATA_CATALOG.CERTIFICATION_LEVELS (
    LEVEL_ID VARCHAR(50) PRIMARY KEY,
    DESCRIPTION VARCHAR(255)
);

-- Insert defaults if table is empty
INSERT INTO DATA_CATALOG.CERTIFICATION_LEVELS (LEVEL_ID, DESCRIPTION)
    SELECT 'NONE', 'Uncertified/Raw Data' WHERE NOT EXISTS (SELECT 1 FROM DATA_CATALOG.CERTIFICATION_LEVELS WHERE LEVEL_ID = 'NONE');
INSERT INTO DATA_CATALOG.CERTIFICATION_LEVELS (LEVEL_ID, DESCRIPTION)
    SELECT 'BRONZE', 'Basic schema-on-read' WHERE NOT EXISTS (SELECT 1 FROM DATA_CATALOG.CERTIFICATION_LEVELS WHERE LEVEL_ID = 'BRONZE');
INSERT INTO DATA_CATALOG.CERTIFICATION_LEVELS (LEVEL_ID, DESCRIPTION)
    SELECT 'SILVER', 'Cleaned and filtered' WHERE NOT EXISTS (SELECT 1 FROM DATA_CATALOG.CERTIFICATION_LEVELS WHERE LEVEL_ID = 'SILVER');
INSERT INTO DATA_CATALOG.CERTIFICATION_LEVELS (LEVEL_ID, DESCRIPTION)
    SELECT 'GOLD', 'Business-ready, certified' WHERE NOT EXISTS (SELECT 1 FROM DATA_CATALOG.CERTIFICATION_LEVELS WHERE LEVEL_ID = 'GOLD');

-- ==============================================================================
-- 3. CORE GOVERNANCE METADATA TABLE
-- ==============================================================================
-- This table maps directly to the UI's Extended Governance Panels
CREATE TABLE IF NOT EXISTS DATA_CATALOG.DATASET_GOVERNANCE (
    FULLY_QUALIFIED_NAME VARCHAR(255) PRIMARY KEY COMMENT 'Format: DB.SCHEMA.TABLE',
    DATABASE_NAME VARCHAR(100) NOT NULL,
    SCHEMA_NAME VARCHAR(100) NOT NULL,
    TABLE_NAME VARCHAR(100) NOT NULL,
    
    -- Ownership
    BUSINESS_OWNER VARCHAR(100) COMMENT 'User or Role responsible strictly for business context',
    DATA_STEWARD VARCHAR(100) COMMENT 'User responsible for data quality and metadata maintenance',
    
    -- Health & Context
    CERTIFICATION_STATUS VARCHAR(50) DEFAULT 'NONE' COMMENT 'Links to CERTIFICATION_LEVELS',
    SENSITIVITY_LEVEL VARCHAR(50) DEFAULT 'INTERNAL' COMMENT 'PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED',
    SLA_TIER VARCHAR(50) DEFAULT 'TIER_3' COMMENT 'TIER_1 (Critical), TIER_2 (High), TIER_3 (Standard)',
    RETENTION_POLICY VARCHAR(100) DEFAULT 'AUTO' COMMENT 'Data retention rules',
    
    -- Semantic Linking
    GLOSSARY_TERMS ARRAY COMMENT 'Array of IDs linking to a business glossary',
    DOMAIN_TAG VARCHAR(100) DEFAULT 'CORE',
    
    -- Audit
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(100)
);

-- ==============================================================================
-- 4. ACTIVITY LOG TABLE
-- ==============================================================================
-- Required by spec: Every metadata change must insert a record here.
CREATE SEQUENCE IF NOT EXISTS DATA_CATALOG.LOG_SEQ START = 1 INCREMENT = 1;

CREATE TABLE IF NOT EXISTS DATA_CATALOG.ACTIVITY_LOG (
    LOG_ID NUMBER DEFAULT DATA_CATALOG.LOG_SEQ.NEXTVAL PRIMARY KEY,
    TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    USER_ID VARCHAR(100) NOT NULL COMMENT 'Application user who triggered the action',
    USER_ROLE VARCHAR(50) NOT NULL COMMENT 'The role (e.g., DATA_STEWARD) they were acting as',
    TARGET_DATASET VARCHAR(255) NOT NULL COMMENT 'Format: DB.SCHEMA.TABLE',
    ACTION_TYPE VARCHAR(50) NOT NULL COMMENT 'e.g., UPDATE_OWNER, MODIFY_DESC, CHANGE_CERTIFICATION',
    PREVIOUS_VALUE VARIANT COMMENT 'JSON representation of value before change',
    NEW_VALUE VARIANT COMMENT 'JSON representation of value after change'
);

-- ==============================================================================
-- 5. ROLE CREATION & HIERARCHY
-- ==============================================================================
-- Strictly enforced roles mapping to UI visibility matrix

-- UI Interaction Roles
CREATE ROLE IF NOT EXISTS DATA_VIEWER_ROLE     COMMENT = 'Read-only access to overview and schema';
CREATE ROLE IF NOT EXISTS DATA_ANALYST_ROLE    COMMENT = 'Can view usage and lineage, no edits';
CREATE ROLE IF NOT EXISTS DATA_STEWARD_ROLE    COMMENT = 'Can edit descriptions, comments, and sensitivity';
CREATE ROLE IF NOT EXISTS DATA_OWNER_ROLE      COMMENT = 'Can assign stewards, certify, and grant SELECT';
CREATE ROLE IF NOT EXISTS PLATFORM_ADMIN_ROLE  COMMENT = 'Full governance control and global reassignment';

-- Service Implementation Role (Used by Next.js Backend)
CREATE ROLE IF NOT EXISTS CATALOG_SERVICE_ROLE COMMENT = 'Executing service role for Pi-Qualytics APIs';

-- Hierarchy
GRANT ROLE DATA_VIEWER_ROLE TO ROLE DATA_ANALYST_ROLE;
GRANT ROLE DATA_ANALYST_ROLE TO ROLE DATA_STEWARD_ROLE;
GRANT ROLE DATA_STEWARD_ROLE TO ROLE DATA_OWNER_ROLE;
GRANT ROLE DATA_OWNER_ROLE TO ROLE PLATFORM_ADMIN_ROLE;

-- The service role inherits all abilities to act on behalf of the application logic
GRANT ROLE PLATFORM_ADMIN_ROLE TO ROLE CATALOG_SERVICE_ROLE;

-- ==============================================================================
-- 6. PERMISSION GRANTS TO CATALOG_SERVICE_ROLE
-- ==============================================================================
-- Allow the service account to manage the governance schema
GRANT USAGE ON DATABASE DATA_QUALITY_DB TO ROLE CATALOG_SERVICE_ROLE;
GRANT USAGE, CREATE MASKING POLICY, CREATE ROW ACCESS POLICY ON SCHEMA DATA_QUALITY_DB.DATA_CATALOG TO ROLE CATALOG_SERVICE_ROLE;

-- DML Rights on Governance Tables
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE DATA_QUALITY_DB.DATA_CATALOG.DATASET_GOVERNANCE TO ROLE CATALOG_SERVICE_ROLE;
GRANT SELECT, INSERT ON TABLE DATA_QUALITY_DB.DATA_CATALOG.ACTIVITY_LOG TO ROLE CATALOG_SERVICE_ROLE;
GRANT USAGE ON SEQUENCE DATA_QUALITY_DB.DATA_CATALOG.LOG_SEQ TO ROLE CATALOG_SERVICE_ROLE;

-- Add required SNOWFLAKE global read permissions (for Schema discovery, Grants, Usage)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE CATALOG_SERVICE_ROLE;


-- ==============================================================================
-- SUMMARY
-- ==============================================================================
-- Run this script to establish the metadata backend. 
-- The Next.js API endpoints (`app/api/dq/datasets/*`) must execute queries 
-- using the `CATALOG_SERVICE_ROLE`.
-- 
-- The API logic should strictly validate the incoming user's simulated AppRole 
-- *before* executing mutations against the tables created above.


-- ==============================================================================
-- 7. STORED PROCEDURES (GOVERNANCE API)
-- ==============================================================================
-- These procedures encapsulate the logic for assigning roles and updating metadata
-- rather than allowing the application to execute raw DML directly.

CREATE OR REPLACE PROCEDURE DATA_QUALITY_DB.DATA_CATALOG.SP_ASSIGN_ROLES(
    P_DATASET_FQN VARCHAR,
    P_BUSINESS_OWNER VARCHAR,
    P_DATA_STEWARD VARCHAR,
    P_ACTING_USER VARCHAR,
    P_ACTING_ROLE VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    UPDATE DATA_QUALITY_DB.DATA_CATALOG.DATASET_GOVERNANCE
    SET BUSINESS_OWNER = COALESCE(:P_BUSINESS_OWNER, BUSINESS_OWNER),
        DATA_STEWARD = COALESCE(:P_DATA_STEWARD, DATA_STEWARD),
        UPDATED_AT = CURRENT_TIMESTAMP(),
        UPDATED_BY = :P_ACTING_USER
    WHERE FULLY_QUALIFIED_NAME = :P_DATASET_FQN;

    IF (SQLROWCOUNT = 0) THEN
        INSERT INTO DATA_QUALITY_DB.DATA_CATALOG.DATASET_GOVERNANCE (
            FULLY_QUALIFIED_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
            BUSINESS_OWNER, DATA_STEWARD, UPDATED_BY
        )
        SELECT 
            :P_DATASET_FQN, SPLIT_PART(:P_DATASET_FQN, '.', 1), SPLIT_PART(:P_DATASET_FQN, '.', 2), SPLIT_PART(:P_DATASET_FQN, '.', 3),
            :P_BUSINESS_OWNER, :P_DATA_STEWARD, :P_ACTING_USER;
    END IF;

    INSERT INTO DATA_QUALITY_DB.DATA_CATALOG.ACTIVITY_LOG (
        USER_ID, USER_ROLE, TARGET_DATASET, ACTION_TYPE, NEW_VALUE
    ) VALUES (
        :P_ACTING_USER, :P_ACTING_ROLE, :P_DATASET_FQN, 'ASSIGN_ROLES',
        OBJECT_CONSTRUCT('BUSINESS_OWNER', :P_BUSINESS_OWNER, 'DATA_STEWARD', :P_DATA_STEWARD)::VARIANT
    );

    RETURN 'SUCCESS';
END;
$$;

CREATE OR REPLACE PROCEDURE DATA_QUALITY_DB.DATA_CATALOG.SP_UPDATE_GOVERNANCE_METADATA(
    P_DATASET_FQN VARCHAR,
    P_CERT_STATUS VARCHAR,
    P_SENSITIVITY VARCHAR,
    P_SLA_TIER VARCHAR,
    P_ACTING_USER VARCHAR,
    P_ACTING_ROLE VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    UPDATE DATA_QUALITY_DB.DATA_CATALOG.DATASET_GOVERNANCE
    SET CERTIFICATION_STATUS = COALESCE(:P_CERT_STATUS, CERTIFICATION_STATUS),
        SENSITIVITY_LEVEL = COALESCE(:P_SENSITIVITY, SENSITIVITY_LEVEL),
        SLA_TIER = COALESCE(:P_SLA_TIER, SLA_TIER),
        UPDATED_AT = CURRENT_TIMESTAMP(),
        UPDATED_BY = :P_ACTING_USER
    WHERE FULLY_QUALIFIED_NAME = :P_DATASET_FQN;

    IF (SQLROWCOUNT = 0) THEN
        INSERT INTO DATA_QUALITY_DB.DATA_CATALOG.DATASET_GOVERNANCE (
            FULLY_QUALIFIED_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
            CERTIFICATION_STATUS, SENSITIVITY_LEVEL, SLA_TIER, UPDATED_BY
        )
        SELECT 
            :P_DATASET_FQN, SPLIT_PART(:P_DATASET_FQN, '.', 1), SPLIT_PART(:P_DATASET_FQN, '.', 2), SPLIT_PART(:P_DATASET_FQN, '.', 3),
            :P_CERT_STATUS, :P_SENSITIVITY, :P_SLA_TIER, :P_ACTING_USER;
    END IF;

    INSERT INTO DATA_QUALITY_DB.DATA_CATALOG.ACTIVITY_LOG (USER_ID, USER_ROLE, TARGET_DATASET, ACTION_TYPE, NEW_VALUE)
    VALUES (:P_ACTING_USER, :P_ACTING_ROLE, :P_DATASET_FQN, 'UPDATE_METADATA', OBJECT_CONSTRUCT('CERT', :P_CERT_STATUS, 'SENSITIVITY', :P_SENSITIVITY, 'SLA', :P_SLA_TIER)::VARIANT);

    RETURN 'SUCCESS';
END;
$$;

GRANT USAGE ON PROCEDURE DATA_QUALITY_DB.DATA_CATALOG.SP_ASSIGN_ROLES(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE CATALOG_SERVICE_ROLE;
GRANT USAGE ON PROCEDURE DATA_QUALITY_DB.DATA_CATALOG.SP_UPDATE_GOVERNANCE_METADATA(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE CATALOG_SERVICE_ROLE;

-- ==============================================================================
-- SUMMARY
-- ==============================================================================
-- Run this script to establish the metadata backend. 
-- The Next.js API endpoints (`app/api/dq/datasets/*`) must execute queries 
-- using the `CATALOG_SERVICE_ROLE`.
-- 
-- The API logic should strictly validate the incoming user's simulated AppRole 
-- *before* executing mutations against the tables created above.