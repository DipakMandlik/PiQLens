# Docker Production Runbook (PI_QLens)

## Scope
This runbook covers secure production containerization for PI_QLens with Snowflake key-pair authentication support.

## Files Updated
- Dockerfile
- .dockerignore
- docker-compose.yml

## Prerequisites
- Docker Engine and Docker Compose
- Valid .env.production values
- Key file present on host:
  - keys/piqlens_private_key_pk8.pem

## Security Model
- Private keys are not baked into the image.
- keys/ is excluded from docker build context.
- Key file is mounted read-only at runtime:
  - /run/secrets/piqlens/piqlens_private_key_pk8.pem
- App can use password mode or key-pair mode.

## Build and Start
From project root:

```powershell
docker compose --env-file .env.production build --no-cache
docker compose --env-file .env.production up -d
```

## Required Environment Variables
Minimum:
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_USER
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA

Authentication (choose one):
- Password mode:
  - SNOWFLAKE_PASSWORD
- Key-pair mode:
  - SNOWFLAKE_PRIVATE_KEY_PATH=/run/secrets/piqlens/piqlens_private_key_pk8.pem
  - SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (optional)

Optional:
- SNOWFLAKE_ROLE
- SNOWFLAKE_PUBLIC_KEY_FINGERPRINT

## Health and Runtime Validation
Check container status:

```powershell
docker compose ps
```

Check app health endpoint from inside container:

```powershell
docker compose exec app node -e "fetch('http://127.0.0.1:3000/api/snowflake/status').then(r=>r.text()).then(t=>console.log(t))"
```

Check logs:

```powershell
docker compose logs app --tail=200
docker compose logs nginx --tail=200
```

## Key-Pair Verification
- Connect via UI using PIQLENS_APP_USER.
- Confirm `/api/snowflake/status` returns auth method key-pair.
- Confirm dataset hierarchy shows expected source schemas/tables.

## Rollback
Stop and remove current deployment:

```powershell
docker compose down
```

Rebuild and start from known-good commit/tag.

## Operational Notes
- Keep password fallback only during migration.
- For final cutover, remove SNOWFLAKE_PASSWORD from production env.
- Rotate private key periodically and update Snowflake RSA public key accordingly.
