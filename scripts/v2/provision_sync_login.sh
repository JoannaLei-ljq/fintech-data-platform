#!/usr/bin/env bash
# Creates or updates the least-privilege V2 sync login (default v2_sync_login), a member of the
# NOLOGIN group role v2_sync that migrations V008 (oltp) / V003 (analytics) grant to.
# The password lives only in .env (V2_SYNC_PASSWORD). If it is missing, a random one is
# generated and appended to .env. The value is never printed. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/../.."

[[ -f .env ]] || { echo ".env not found" >&2; exit 1; }
if ! grep -q '^V2_SYNC_PASSWORD=' .env; then
  printf '\n# V2 sync login (least privilege, member of v2_sync). Written by scripts/v2/provision_sync_login.sh\nV2_SYNC_USER=v2_sync_login\nV2_SYNC_PASSWORD=%s\n' \
    "$(openssl rand -hex 24)" >> .env
  echo "generated V2_SYNC_PASSWORD in .env (value not shown)"
fi
SYNC_USER=$(sed -n 's/^V2_SYNC_USER=//p' .env | tail -1); SYNC_USER=${SYNC_USER:-v2_sync_login}
SYNC_PASSWORD=$(sed -n 's/^V2_SYNC_PASSWORD=//p' .env | tail -1)
[[ "$SYNC_USER" =~ ^v2_[a-z0-9_]+$ ]] || { echo "V2_SYNC_USER must match ^v2_[a-z0-9_]+$" >&2; exit 1; }
[[ "$SYNC_PASSWORD" =~ ^[A-Za-z0-9]{24,}$ ]] || { echo "V2_SYNC_PASSWORD must be 24+ letters/digits" >&2; exit 1; }

# The SQL (with the password) goes to psql on stdin, so it never appears in a process listing or output.
docker compose exec -T postgres sh -c 'psql -q -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres' <<SQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'v2_sync') THEN
        RAISE EXCEPTION 'role v2_sync does not exist yet: run the V2 migrations first';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$SYNC_USER') THEN
        CREATE ROLE $SYNC_USER LOGIN;
    END IF;
END \$\$;
ALTER ROLE $SYNC_USER WITH LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    CONNECTION LIMIT 10 PASSWORD '$SYNC_PASSWORD';
GRANT v2_sync TO $SYNC_USER;
DO \$\$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid
               JOIN pg_roles u ON u.oid = m.member
               WHERE u.rolname = '$SYNC_USER' AND g.rolname <> 'v2_sync') THEN
        RAISE EXCEPTION '$SYNC_USER is a member of roles other than v2_sync';
    END IF;
END \$\$;
SQL
echo "sync login $SYNC_USER provisioned (member of v2_sync only)"
