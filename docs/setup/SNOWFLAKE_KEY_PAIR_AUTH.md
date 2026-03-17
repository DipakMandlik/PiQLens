# Snowflake Key Pair Authentication Setup (PI_QLens)

## Objective
Prepare Snowflake key pair authentication for service user PIQLENS_APP_USER without changing application code or current password authentication.

## Scope
- Generate RSA keys
- Attach public key to PIQLENS_APP_USER
- Verify key registration
- Store private key in local secure directory
- Validate RBAC access remains intact

## Prerequisites
- Role with permission to alter users (SECURITYADMIN or ACCOUNTADMIN)
- OpenSSL installed
- Optional: SnowSQL CLI installed for script-driven execution

Windows install examples:
- OpenSSL: `winget install ShiningLight.OpenSSL.Light`
- SnowSQL: follow Snowflake CLI installation docs

## Step 1: Generate RSA key pair
From workspace root:

```powershell
./scripts/setup_snowflake_keypair.ps1
```

Generated files (local only):
- keys/piqlens_private_key.pem
- keys/piqlens_private_key_pk8.pem
- keys/piqlens_public_key.pem
- keys/piqlens_public_key_body.txt

## Step 2: Attach public key to Snowflake user
Option A (Snowsight worksheet):
1. Open sql/12_Key_Pair_Auth_Setup.sql
2. Replace placeholder <<PASTE_PUBLIC_KEY_WITHOUT_HEADERS>> with the content of keys/piqlens_public_key_body.txt
3. Execute script as SECURITYADMIN/ACCOUNTADMIN

Option B (CLI automation):

```powershell
$env:SNOWSQL_ACCOUNT = "<account_identifier>"
$env:SNOWSQL_USER = "<security_admin_user>"
./scripts/setup_snowflake_keypair.ps1 -RunSnowflakeValidation
```

## Step 3: Verify key configuration
Verification commands included in sql/12_Key_Pair_Auth_Setup.sql:

```sql
DESC USER PIQLENS_APP_USER;
SHOW PARAMETERS LIKE 'RSA_PUBLIC_KEY' FOR USER PIQLENS_APP_USER;
SHOW PARAMETERS LIKE 'RSA_PUBLIC_KEY_FP' FOR USER PIQLENS_APP_USER;
```

## Step 4: Secure private key storage
Private keys are stored under keys/ and excluded by .gitignore.

Recommended path for app integration phase:
- /keys/piqlens_private_key_pk8.pem

## Application Integration Environment Variables (Phase 2)
The runtime now supports either password auth or key-pair auth.

Password auth (existing):
- SNOWFLAKE_PASSWORD

Key-pair auth (new):
- SNOWFLAKE_PRIVATE_KEY_PATH
- SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (optional; also supports SNOWFLAKE_PRIVATE_KEY_PASS)
- SNOWFLAKE_PUBLIC_KEY_FINGERPRINT (optional)

Required in both modes:
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_USER
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA

## Step 5: Validate RBAC integrity
Validation checks in sql/12_Key_Pair_Auth_Setup.sql confirm:
- Role inheritance for PIQLENS_APP_USER
- Warehouse usage for DQ_ANALYTICS_WH
- Database and schema access for DATA_QUALITY_DB:
  - DATA_CATALOG
  - DB_METRICS
  - DQ_CONFIG
  - DQ_ENGINE
  - DQ_METRICS

## Non-Goals (this phase)
- No application code updates
- No password auth removal
- No RBAC grant changes

## Success Criteria
- RSA public key accepted by Snowflake for PIQLENS_APP_USER
- Existing role and warehouse access still functional
- Private key stored securely and not committed
- Platform remains operational with existing authentication flow
