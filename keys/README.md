# Local Key Storage (Do Not Commit Secrets)

This directory is for local Snowflake key-pair files used by PI_QLens service authentication.

Expected files after running setup:
- piqlens_private_key.pem
- piqlens_private_key_pk8.pem
- piqlens_public_key.pem
- piqlens_public_key_body.txt

Security notes:
- Private key files must remain local only.
- This repository's .gitignore excludes key files in this directory.
- Keep password authentication enabled until application migration is complete.
