"""Incremental sync: fintech_oltp (source) -> fintech_analytics.raw (target).

For each table, rows are read in (updated_at, pk) order starting after the
stored checkpoint, upserted by primary key, and the checkpoint is advanced in
the SAME target transaction as the data. A crash therefore either keeps both
the batch and its checkpoint or neither, and re-running is always safe.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg import sql

log = logging.getLogger("incremental_sync")

# Epoch-like floor for a table that has never been synced.
INITIAL_CURSOR = (datetime.fromisoformat("1970-01-01T00:00:00+00:00"), -1)


@dataclass(frozen=True)
class TableSpec:
    name: str
    pk: str
    columns: tuple[str, ...]


# Order matters only for readability of logs; raw tables carry no FKs.
TABLES: tuple[TableSpec, ...] = (
    TableSpec("users", "user_id",
              ("user_id", "email", "full_name", "country", "created_at", "updated_at")),
    TableSpec("merchants", "merchant_id",
              ("merchant_id", "merchant_name", "category", "country", "created_at", "updated_at")),
    TableSpec("transactions", "transaction_id",
              ("transaction_id", "user_id", "merchant_id", "amount", "currency", "status",
               "created_at", "updated_at")),
    TableSpec("refunds", "refund_id",
              ("refund_id", "transaction_id", "amount", "reason", "status", "created_at", "updated_at")),
)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"{name} is not set. Copy .env.example to .env and fill it in.")
    return value


def conninfo(dbname: str) -> str:
    return psycopg.conninfo.make_conninfo(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("POSTGRES_PORT", "5434"),
        user=os.getenv("POSTGRES_USER", "fintech"),
        password=require_env("POSTGRES_PASSWORD"),
        dbname=dbname,
    )


def read_checkpoint(target: psycopg.Connection, table: str) -> tuple[datetime, int]:
    row = target.execute(
        "SELECT last_updated_at, last_pk FROM meta.sync_checkpoints WHERE table_name = %s",
        (table,),
    ).fetchone()
    return (row[0], row[1]) if row else INITIAL_CURSOR


def fetch_batch(source: psycopg.Connection, spec: TableSpec,
                cursor: tuple[datetime, int], batch_size: int) -> list[tuple]:
    query = sql.SQL(
        "SELECT {cols} FROM {table} "
        "WHERE (updated_at, {pk}) > (%s, %s) "
        "ORDER BY updated_at, {pk} LIMIT %s"
    ).format(
        cols=sql.SQL(", ").join(map(sql.Identifier, spec.columns)),
        table=sql.Identifier(spec.name),
        pk=sql.Identifier(spec.pk),
    )
    return source.execute(query, (*cursor, batch_size)).fetchall()


class InjectedFailure(RuntimeError):
    """Raised on purpose (--fail-before-commit) to prove data and checkpoint roll back together."""


def upsert_and_checkpoint(target: psycopg.Connection, spec: TableSpec, rows: list[tuple],
                          fail_before_commit: bool = False) -> None:
    non_pk = [c for c in spec.columns if c != spec.pk]
    upsert = sql.SQL(
        "INSERT INTO raw.{table} ({cols}) VALUES ({vals}) "
        "ON CONFLICT ({pk}) DO UPDATE SET {sets}, _synced_at = now() "
        # Never let an older version overwrite a newer one.
        "WHERE raw.{table}.updated_at <= EXCLUDED.updated_at"
    ).format(
        table=sql.Identifier(spec.name),
        cols=sql.SQL(", ").join(map(sql.Identifier, spec.columns)),
        vals=sql.SQL(", ").join(sql.Placeholder() * len(spec.columns)),
        pk=sql.Identifier(spec.pk),
        sets=sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in non_pk
        ),
    )

    pk_idx = spec.columns.index(spec.pk)
    ts_idx = spec.columns.index("updated_at")
    last = rows[-1]

    with target.transaction():
        with target.cursor() as cur:
            cur.executemany(upsert, rows)
        target.execute(
            """
            INSERT INTO meta.sync_checkpoints (table_name, last_updated_at, last_pk, rows_last_run, checkpointed_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (table_name) DO UPDATE
               SET last_updated_at = EXCLUDED.last_updated_at,
                   last_pk         = EXCLUDED.last_pk,
                   rows_last_run   = EXCLUDED.rows_last_run,
                   checkpointed_at = EXCLUDED.checkpointed_at
            """,
            (spec.name, last[ts_idx], last[pk_idx], len(rows)),
        )
        if fail_before_commit:
            # Both the upsert and the checkpoint update have executed but not committed;
            # leaving the transaction block via an exception rolls back both.
            raise InjectedFailure(
                f"injected failure before commit on {spec.name} ({len(rows)} row(s) pending)"
            )


def mark_no_changes(target: psycopg.Connection, table: str) -> None:
    with target.transaction():
        target.execute(
            "UPDATE meta.sync_checkpoints SET rows_last_run = 0, checkpointed_at = now() "
            "WHERE table_name = %s",
            (table,),
        )


def sync_table(source: psycopg.Connection, target: psycopg.Connection,
               spec: TableSpec, batch_size: int, fail_before_commit: bool = False) -> int:
    cursor = read_checkpoint(target, spec.name)
    total = 0
    while True:
        rows = fetch_batch(source, spec, cursor, batch_size)
        if not rows:
            break
        upsert_and_checkpoint(target, spec, rows, fail_before_commit)
        total += len(rows)
        last = rows[-1]
        cursor = (last[spec.columns.index("updated_at")], last[spec.columns.index(spec.pk)])
        if len(rows) < batch_size:
            break
    if total == 0:
        mark_no_changes(target, spec.name)
    log.info("%-12s synced %d row(s); cursor=(%s, %s)", spec.name, total, *cursor)
    return total


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--fail-before-commit", choices=[t.name for t in TABLES], default=None,
                        help="demo only: raise just before committing the first batch of this table")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    oltp_db = os.getenv("OLTP_DB", "fintech_oltp")
    analytics_db = os.getenv("ANALYTICS_DB", "fintech_analytics")
    log.info("source=%s target=%s fail_before_commit=%s", oltp_db, analytics_db, args.fail_before_commit)
    with psycopg.connect(conninfo(oltp_db), autocommit=True) as source, \
         psycopg.connect(conninfo(analytics_db), autocommit=True) as target:
        for spec in TABLES:
            sync_table(source, target, spec, args.batch_size,
                       fail_before_commit=(spec.name == args.fail_before_commit))


if __name__ == "__main__":
    main()
