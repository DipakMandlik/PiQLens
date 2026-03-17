param(
    [string]$KeysDir = "./keys",
    [string]$ServiceUser = "PIQLENS_APP_USER",
    [switch]$RunSnowflakeValidation,
    [string]$SnowflakeAccount = $env:SNOWSQL_ACCOUNT,
    [string]$SnowflakeAdminUser = $env:SNOWSQL_USER,
    [string]$SnowflakeAdminRole = "SECURITYADMIN",
    [string]$SnowflakeWarehouse = "DQ_ANALYTICS_WH"
)

$ErrorActionPreference = "Stop"

function Require-Command {
    param([Parameter(Mandatory = $true)][string]$Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' not found in PATH. Install it and rerun."
    }
}

function New-RsaKeyPairDotNet {
    param(
        [Parameter(Mandatory = $true)][string]$PrivateKeyPath,
        [Parameter(Mandatory = $true)][string]$PrivateKeyPk8Path,
        [Parameter(Mandatory = $true)][string]$PublicKeyPath
    )

    $rsa = [System.Security.Cryptography.RSA]::Create(2048)
    try {
        $pkcs1Bytes = $rsa.ExportRSAPrivateKey()
        $pkcs8Bytes = $rsa.ExportPkcs8PrivateKey()
        $spkiBytes = $rsa.ExportSubjectPublicKeyInfo()

        $pkcs1Pem = "-----BEGIN RSA PRIVATE KEY-----`n$([System.Convert]::ToBase64String($pkcs1Bytes, 'InsertLineBreaks'))`n-----END RSA PRIVATE KEY-----`n"
        $pkcs8Pem = "-----BEGIN PRIVATE KEY-----`n$([System.Convert]::ToBase64String($pkcs8Bytes, 'InsertLineBreaks'))`n-----END PRIVATE KEY-----`n"
        $pubPem = "-----BEGIN PUBLIC KEY-----`n$([System.Convert]::ToBase64String($spkiBytes, 'InsertLineBreaks'))`n-----END PUBLIC KEY-----`n"

        Set-Content -Path $PrivateKeyPath -Value $pkcs1Pem -Encoding ascii
        Set-Content -Path $PrivateKeyPk8Path -Value $pkcs8Pem -Encoding ascii
        Set-Content -Path $PublicKeyPath -Value $pubPem -Encoding ascii
    }
    finally {
        $rsa.Dispose()
    }
}

function New-RsaKeyPairNode {
    param(
        [Parameter(Mandatory = $true)][string]$PrivateKeyPath,
        [Parameter(Mandatory = $true)][string]$PrivateKeyPk8Path,
        [Parameter(Mandatory = $true)][string]$PublicKeyPath
    )

    if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
        throw "Neither OpenSSL nor Node.js is available for key generation."
    }

    $privatePathJs = ($PrivateKeyPath -replace '\\', '\\\\' -replace "'", "\\'")
    $privatePk8PathJs = ($PrivateKeyPk8Path -replace '\\', '\\\\' -replace "'", "\\'")
    $publicPathJs = ($PublicKeyPath -replace '\\', '\\\\' -replace "'", "\\'")

    $nodeScript = @"
const fs = require('fs');
const crypto = require('crypto');

const privateKeyPath = '$privatePathJs';
const privateKeyPk8Path = '$privatePk8PathJs';
const publicKeyPath = '$publicPathJs';

const { privateKey: privateKeyPkcs1, publicKey } = crypto.generateKeyPairSync('rsa', {
  modulusLength: 2048,
  publicKeyEncoding: { type: 'spki', format: 'pem' },
  privateKeyEncoding: { type: 'pkcs1', format: 'pem' },
});

const privateKeyPk8 = crypto.createPrivateKey(privateKeyPkcs1).export({ type: 'pkcs8', format: 'pem' });

fs.writeFileSync(privateKeyPath, privateKeyPkcs1, 'ascii');
fs.writeFileSync(privateKeyPk8Path, privateKeyPk8.toString(), 'ascii');
fs.writeFileSync(publicKeyPath, publicKey, 'ascii');
"@

    & node -e $nodeScript
    if ($LASTEXITCODE -ne 0) {
        throw "Node.js key generation failed with exit code $LASTEXITCODE"
    }
}

function Protect-PrivateKeyFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    $acl = Get-Acl -Path $Path
    $acl.SetAccessRuleProtection($true, $false)

    foreach ($existingRule in @($acl.Access)) {
        $null = $acl.RemoveAccessRule($existingRule)
    }

    $identityRefs = @(
        "$env:USERDOMAIN\$env:USERNAME",
        "BUILTIN\Administrators",
        "NT AUTHORITY\SYSTEM"
    )

    foreach ($identity in $identityRefs) {
        $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
            $identity,
            [System.Security.AccessControl.FileSystemRights]::FullControl,
            [System.Security.AccessControl.AccessControlType]::Allow
        )
        $acl.SetAccessRule($rule)
    }

    Set-Acl -Path $Path -AclObject $acl
}

function Get-PublicKeyBody {
    param([Parameter(Mandatory = $true)][string]$PemPath)

    $pem = Get-Content -Raw -Path $PemPath
    $body = ($pem -split "`n" |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ -and $_ -notmatch "^-----BEGIN PUBLIC KEY-----$" -and $_ -notmatch "^-----END PUBLIC KEY-----$" }) -join ""

    if ([string]::IsNullOrWhiteSpace($body)) {
        throw "Failed to parse public key body from $PemPath"
    }

    return $body
}

Write-Host "[1/5] Validating prerequisites..."
$hasOpenSsl = $null -ne (Get-Command openssl -ErrorAction SilentlyContinue)
if ($hasOpenSsl) {
    Write-Host "    OpenSSL detected."
}
else {
    Write-Host "    OpenSSL not found. Falling back to Node.js key generation."
}

$resolvedKeysDir = Resolve-Path -Path $KeysDir -ErrorAction SilentlyContinue
if (-not $resolvedKeysDir) {
    New-Item -ItemType Directory -Path $KeysDir | Out-Null
    $resolvedKeysDir = Resolve-Path -Path $KeysDir
}
$resolvedKeysDir = $resolvedKeysDir.Path

$privateKeyPath = Join-Path $resolvedKeysDir "piqlens_private_key.pem"
$privateKeyPk8Path = Join-Path $resolvedKeysDir "piqlens_private_key_pk8.pem"
$publicKeyPath = Join-Path $resolvedKeysDir "piqlens_public_key.pem"
$publicKeyBodyPath = Join-Path $resolvedKeysDir "piqlens_public_key_body.txt"

Write-Host "[2/5] Generating RSA key pair..."
if ($hasOpenSsl) {
    & openssl genrsa 2048 | Out-File -FilePath $privateKeyPath -Encoding ascii
    & openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in $privateKeyPath -out $privateKeyPk8Path
    & openssl rsa -in $privateKeyPath -pubout -out $publicKeyPath
}
else {
    New-RsaKeyPairNode -PrivateKeyPath $privateKeyPath -PrivateKeyPk8Path $privateKeyPk8Path -PublicKeyPath $publicKeyPath
}

# Restrict access to current user for private keys.
Write-Host "[3/5] Applying filesystem ACL restrictions..."
Protect-PrivateKeyFile -Path $privateKeyPath
Protect-PrivateKeyFile -Path $privateKeyPk8Path

$publicKeyBody = Get-PublicKeyBody -PemPath $publicKeyPath
Set-Content -Path $publicKeyBodyPath -Value $publicKeyBody -Encoding ascii

Write-Host "[4/5] Key material ready."
Write-Host "    Private key (PKCS1):  $privateKeyPath"
Write-Host "    Private key (PKCS8):  $privateKeyPk8Path"
Write-Host "    Public key (PEM):     $publicKeyPath"
Write-Host "    Public key body:      $publicKeyBodyPath"

if ($RunSnowflakeValidation) {
    Write-Host "[5/5] Running Snowflake key attach + RBAC validation..."
    Require-Command -Name "snowsql"

    if ([string]::IsNullOrWhiteSpace($SnowflakeAccount) -or [string]::IsNullOrWhiteSpace($SnowflakeAdminUser)) {
        throw "Set SNOWSQL_ACCOUNT and SNOWSQL_USER (or pass -SnowflakeAccount and -SnowflakeAdminUser)."
    }

    $tempSql = Join-Path $env:TEMP "piqlens_keypair_setup_$(Get-Date -Format yyyyMMddHHmmss).sql"
@"
USE ROLE $SnowflakeAdminRole;
ALTER USER $ServiceUser SET RSA_PUBLIC_KEY = '$publicKeyBody';
DESC USER $ServiceUser;
SHOW PARAMETERS LIKE 'RSA_PUBLIC_KEY' FOR USER $ServiceUser;
SHOW PARAMETERS LIKE 'RSA_PUBLIC_KEY_FP' FOR USER $ServiceUser;
SHOW GRANTS TO USER $ServiceUser;

USE ROLE PIQLENS_ENGINEER_ROLE;
USE WAREHOUSE $SnowflakeWarehouse;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DATA_CATALOG;
SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
USE SCHEMA DB_METRICS;
SELECT CURRENT_SCHEMA();
USE SCHEMA DQ_CONFIG;
SELECT CURRENT_SCHEMA();
USE SCHEMA DQ_ENGINE;
SELECT CURRENT_SCHEMA();
USE SCHEMA DQ_METRICS;
SELECT CURRENT_SCHEMA();
"@ | Set-Content -Path $tempSql -Encoding ascii

    & snowsql -a $SnowflakeAccount -u $SnowflakeAdminUser -f $tempSql
    Remove-Item -Path $tempSql -Force
}
else {
    Write-Host "[5/5] Snowflake step skipped (no CLI execution requested)."
    Write-Host "    Next: run sql/12_Key_Pair_Auth_Setup.sql in Snowsight and paste piqlens_public_key_body.txt"
}
