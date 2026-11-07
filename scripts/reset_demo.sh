#!/usr/bin/env bash
# Drops and recreates the isolated demo databases (demo_oltp, demo_analytics)
# from the same DDL as the main databases, then loads the demo seed.
# fintech_oltp / fintech_analytics / airflow are never touched.
set -euo pipefail
cd "$(dirname "$0")/.."

psql_in() {  # psql_in <db> [psql args...]  — runs psql inside the postgres container
  local db=$1; shift
  docker compose exec -T postgres sh -c 'psql -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$0" "$@"' "$db" "$@"
}

echo "Recreating demo_oltp and demo_analytics ..."
psql_in postgres <<'SQL'
DROP DATABASE IF EXISTS demo_oltp WITH (FORCE);
DROP DATABASE IF EXISTS demo_analytics WITH (FORCE);
CREATE DATABASE demo_oltp;
CREATE DATABASE demo_analytics;
SQL

psql_in demo_oltp      -f /docker-entrypoint-initdb.d/schema/oltp_schema.sql
psql_in demo_analytics -f /docker-entrypoint-initdb.d/schema/analytics_schema.sql
psql_in demo_oltp < sql/demo/seed_demo.sql

echo "Demo source rows:"
psql_in demo_oltp -c "SELECT 'transactions' AS table_name, count(*) FROM transactions UNION ALL SELECT 'refunds', count(*) FROM refunds;"
