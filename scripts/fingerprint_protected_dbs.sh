#!/usr/bin/env bash
# Read-only content fingerprint (row count + hash of all rows) of the original
# databases. Run before and after any work and diff the output.
set -euo pipefail
cd "$(dirname "$0")/.."

fingerprint() {  # fingerprint <db> <table...>
  local db=$1; shift
  for t in "$@"; do
    docker compose exec -T postgres sh -c "psql -X -At -U \"\$POSTGRES_USER\" -d $db -c \"SELECT '$db.$t rows=' || count(*) || ' md5=' || coalesce(left(md5(string_agg(x::text, '|' ORDER BY x::text)), 12), '-') FROM $t AS x\""
  done
}

fingerprint fintech_oltp users merchants transactions refunds
fingerprint fintech_analytics raw.users raw.merchants raw.transactions raw.refunds meta.sync_checkpoints
