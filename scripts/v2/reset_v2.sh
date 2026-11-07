#!/usr/bin/env bash
# Drops and rebuilds the isolated V2 databases (v2_oltp, v2_analytics):
#   1. migrate v2_oltp to the V001 baseline (the MVP schema)
#   2. load the MVP demo seed, so later migrations run against real-shaped existing data
#   3. apply every remaining migration (conversions and backfills included)
#   4. migrate v2_analytics
# Only v2_* databases are ever dropped; the names are checked before anything runs.
set -euo pipefail
cd "$(dirname "$0")/../.."

OLTP_DB=v2_oltp
ANALYTICS_DB=v2_analytics
PYTHON=${PYTHON:-.venv/bin/python}

for db in "$OLTP_DB" "$ANALYTICS_DB"; do
  [[ "$db" =~ ^v2_[a-z0-9_]+$ ]] || { echo "refusing to reset non-v2 database: $db" >&2; exit 1; }
done

psql_in() {  # psql_in <db> [psql args...]  — runs psql inside the postgres container
  local db=$1; shift
  docker compose exec -T postgres sh -c 'psql -q -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$0" "$@"' "$db" "$@"
}

echo "Recreating $OLTP_DB and $ANALYTICS_DB ..."
psql_in postgres <<SQL
DROP DATABASE IF EXISTS $OLTP_DB WITH (FORCE);
DROP DATABASE IF EXISTS $ANALYTICS_DB WITH (FORCE);
CREATE DATABASE $OLTP_DB;
CREATE DATABASE $ANALYTICS_DB;
SQL

"$PYTHON" -m tools.migrate --target oltp --db "$OLTP_DB" --to 1
echo "Loading the MVP demo seed into the V001 baseline ..."
psql_in "$OLTP_DB" < sql/demo/seed_demo.sql
"$PYTHON" -m tools.migrate --target oltp --db "$OLTP_DB"
"$PYTHON" -m tools.migrate --target analytics --db "$ANALYTICS_DB"

echo "Migration status:"
"$PYTHON" -m tools.migrate --target oltp --db "$OLTP_DB" --status
"$PYTHON" -m tools.migrate --target analytics --db "$ANALYTICS_DB" --status
