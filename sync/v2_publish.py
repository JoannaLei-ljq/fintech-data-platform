"""Publish validated V2 candidate marts: marts_candidate -> marts, atomically.

Publication happens only for ONE pipeline run whose dbt build and reconciliation both passed,
and only if nothing changed since: everything is verified inside the single transaction that
copies the data, while holding the locks that keep it from changing.

In one transaction (READ COMMITTED; the locks make the verified data stable):
  1. pg_advisory_xact_lock: publications are strictly serialized.
  2. LOCK the candidate marts and build manifest IN SHARE MODE: a concurrent dbt build (which
     replaces tables) waits until this transaction ends.
  3. SELECT meta.sync_state FOR SHARE: a concurrent sync cannot commit new raw data/checkpoint
     until this transaction ends; the state must be active and its last run must be the sync
     run the candidate was built from.
  4. Verify dbt's run_results.json: same dbt invocation as the build manifest, built for this
     pipeline run (--vars), every node success/pass, the three marts built.
  5. Verify a PASSING reconciliation of this pipeline run, dbt invocation and sync run, whose
     candidate fingerprint equals the candidate's content now.
  6. Refuse a pipeline run (or dbt invocation) that was already published.
  7. For each mart: DELETE the published rows and INSERT the candidate's (MVCC-safe; the
     published tables keep their identity, dependent views and grants). If the column list
     changed, recreate the published table first (refused if a view depends on it).
  8. INSERT the publication record. COMMIT.
Any failure before COMMIT rolls back all three marts and the publication record together.

Concurrency notes for consumers: a single SQL statement sees one publication in full. Several
READ COMMITTED statements can each see a different publication; use one REPEATABLE READ
transaction (optionally reading marts.current_publication) to get a consistent multi-query view.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Callable

import psycopg
from dotenv import load_dotenv
from psycopg import sql

from sync.v2_marts import (BUILD_INFO, CANDIDATE_SCHEMA, MART_TABLES, PUBLISHED_SCHEMA, candidate_fingerprint,
                           read_build_info)
from sync.v2_sync import SyncCredentials
from tools.db_guard import UnsafeDatabaseTarget

PUBLISH_LOCK = (48210003, 1)


class PublishRefused(RuntimeError):
    """Publication was refused; nothing was changed."""


def _check_run_results(path: Path, pipeline_run_id: str) -> tuple[str, dict]:
    if not path.exists():
        raise PublishRefused(f"dbt run_results not found at {path}")
    results = json.loads(path.read_text())
    invocation = results["metadata"]["invocation_id"]
    built_for = (results.get("args", {}).get("vars") or {}).get("v2_pipeline_run_id")
    if built_for != pipeline_run_id:
        raise PublishRefused(f"run_results belong to pipeline run {built_for!r}, not {pipeline_run_id!r}")
    bad = [r["unique_id"] for r in results["results"] if r["status"] not in ("success", "pass")]
    if bad:
        raise PublishRefused(f"dbt invocation {invocation} did not pass: {bad[:5]}")
    built = {r["unique_id"].split(".")[-1] for r in results["results"] if r["unique_id"].startswith("model.")}
    missing = [t for t in MART_TABLES if t not in built]
    if missing:
        raise PublishRefused(f"dbt invocation {invocation} did not build {missing}")
    return invocation, results


def _columns(conn, schema: str, table: str) -> list[tuple[str, str]]:
    return conn.execute("""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum""", (f"{schema}.{table}",)).fetchall()


def _ensure_published_table(conn, table: str) -> None:
    candidate = _columns(conn, CANDIDATE_SCHEMA, table)
    published = _columns(conn, PUBLISHED_SCHEMA, table)
    if published == candidate:
        return
    ident = sql.Identifier(PUBLISHED_SCHEMA, table)
    if published:
        dependents = conn.execute("""
            SELECT DISTINCT d.classid::regclass::text || ':' || coalesce(v.oid::regclass::text, d.objid::text)
            FROM pg_depend d
            LEFT JOIN pg_rewrite r ON d.classid = 'pg_rewrite'::regclass AND r.oid = d.objid
            LEFT JOIN pg_class v ON v.oid = r.ev_class
            WHERE d.refobjid = to_regclass(%s) AND d.deptype = 'n'
              AND coalesce(v.oid, 0) <> to_regclass(%s)::oid""",
                                  (f"{PUBLISHED_SCHEMA}.{table}", f"{PUBLISHED_SCHEMA}.{table}")).fetchall()
        if dependents:
            raise PublishRefused(f"{PUBLISHED_SCHEMA}.{table} changed columns but objects depend on it: "
                                 f"{[d for (d,) in dependents]}; migrate them explicitly")
        conn.execute(sql.SQL("DROP TABLE {}").format(ident))
    conn.execute(sql.SQL("CREATE TABLE {} (LIKE {})").format(ident, sql.Identifier(CANDIDATE_SCHEMA, table)))


def publish(analytics_db: str, pipeline_run_id: str, run_results: Path, *, lock_timeout: str = "30s",
            fail_after_table: str | None = None, before_commit: Callable[[], None] | None = None) -> dict:
    """Publish the candidate marts of `pipeline_run_id`. Raises PublishRefused (nothing changed)."""
    if not analytics_db.startswith("v2_"):
        raise UnsafeDatabaseTarget(f"publication writes v2_* databases only, got {analytics_db!r}")
    invocation, _ = _check_run_results(run_results, pipeline_run_id)

    with psycopg.connect(SyncCredentials.admin().conninfo(analytics_db)) as conn:
        assert conn.execute("SELECT current_database()").fetchone()[0] == analytics_db
        conn.execute("SET TimeZone = 'UTC'")
        conn.execute("SET DateStyle = 'ISO, YMD'")   # same rendering as v2_reconcile's fingerprint
        conn.commit()
        with conn.transaction():
            conn.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(lock_timeout)))
            conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", PUBLISH_LOCK)
            for table in (*MART_TABLES, BUILD_INFO):
                if conn.execute("SELECT to_regclass(%s) IS NULL", (f"{CANDIDATE_SCHEMA}.{table}",)).fetchone()[0]:
                    raise PublishRefused(f"candidate {CANDIDATE_SCHEMA}.{table} does not exist")
            conn.execute(sql.SQL("LOCK TABLE {} IN SHARE MODE").format(sql.SQL(", ").join(
                sql.Identifier(CANDIDATE_SCHEMA, t) for t in (*MART_TABLES, BUILD_INFO))))

            build = read_build_info(conn)
            if build is None:
                raise PublishRefused("candidate build manifest is missing or ambiguous")
            if build.pipeline_run_id != pipeline_run_id or build.dbt_invocation_id != invocation:
                raise PublishRefused(
                    f"candidate was built by pipeline run {build.pipeline_run_id!r} / dbt invocation "
                    f"{build.dbt_invocation_id}, not {pipeline_run_id!r} / {invocation}")

            state = conn.execute("SELECT status, last_run_id FROM meta.sync_state WHERE source_name = 'v2_oltp' "
                                 "FOR SHARE").fetchone()
            if state is None or state[0] != "active" or state[1] != build.sync_run_id:
                raise PublishRefused(f"sync state {state} no longer matches the candidate's sync run "
                                     f"{build.sync_run_id}: raw changed after the build")

            validation = conn.execute("""
                SELECT validation_id, passed, candidate_fingerprint FROM meta.mart_validations
                WHERE pipeline_run_id = %s AND dbt_invocation_id = %s AND sync_run_id = %s
                ORDER BY validation_id DESC LIMIT 1""", (pipeline_run_id, invocation, build.sync_run_id)).fetchone()
            if validation is None or not validation[1]:
                raise PublishRefused(f"no passing reconciliation for pipeline run {pipeline_run_id!r}, "
                                     f"dbt invocation {invocation}, sync run {build.sync_run_id}")
            fingerprint = candidate_fingerprint(conn)
            if fingerprint != validation[2]:
                raise PublishRefused("candidate marts changed after they were reconciled "
                                     f"(fingerprint {fingerprint} != validated {validation[2]})")
            if conn.execute("SELECT 1 FROM meta.mart_publications WHERE pipeline_run_id = %s OR "
                            "dbt_invocation_id = %s", (pipeline_run_id, invocation)).fetchone():
                raise PublishRefused(f"pipeline run {pipeline_run_id!r} / dbt invocation {invocation} "
                                     "was already published")

            counts = {}
            for table in MART_TABLES:
                _ensure_published_table(conn, table)
                cols = sql.SQL(", ").join(sql.Identifier(c) for c, _ in _columns(conn, CANDIDATE_SCHEMA, table))
                published, candidate = sql.Identifier(PUBLISHED_SCHEMA, table), sql.Identifier(CANDIDATE_SCHEMA, table)
                conn.execute(sql.SQL("DELETE FROM {}").format(published))
                counts[table] = conn.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                    published, cols, cols, candidate)).rowcount
                if fail_after_table == table:
                    raise RuntimeError(f"injected failure after publishing {table}")

            previous = conn.execute("SELECT max(publication_id) FROM meta.mart_publications").fetchone()[0]
            publication_id = conn.execute("""
                INSERT INTO meta.mart_publications (pipeline_run_id, dbt_invocation_id, sync_run_id, validation_id,
                    candidate_fingerprint, row_counts, previous_publication_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING publication_id""",
                (pipeline_run_id, invocation, build.sync_run_id, validation[0], fingerprint, json.dumps(counts),
                 previous)).fetchone()[0]
            if before_commit:
                before_commit()
    return {"publication_id": publication_id, "pipeline_run_id": pipeline_run_id, "dbt_invocation_id": invocation,
            "sync_run_id": build.sync_run_id, "validation_id": validation[0], "row_counts": counts,
            "previous_publication_id": previous}


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Publish validated V2 candidate marts atomically.")
    parser.add_argument("--analytics", default=os.getenv("V2_ANALYTICS_DB", "v2_analytics"))
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--run-results", type=Path, default=Path("/tmp/dbt-v2/target/run_results.json"))
    args = parser.parse_args()
    try:
        result = publish(args.analytics, args.pipeline_run_id, args.run_results)
    except PublishRefused as exc:
        print(f"PUBLISH REFUSED: {exc}", flush=True)
        sys.exit(2)
    print("PUBLISHED " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
