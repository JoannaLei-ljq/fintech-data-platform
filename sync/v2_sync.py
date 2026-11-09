"""V2 incremental sync: v2_oltp (source) -> v2_analytics raw.* (target).

Transaction-ID watermark snapshot polling (design and proof: docs/v2/PHASE1_DESIGN.md, Step 6).

One run:
  1. Takes a session advisory lock on the target, so runs are strictly serialized; a second
     concurrent attempt is rejected and recorded.
  2. Opens ONE REPEATABLE READ READ ONLY source transaction. Its first statement fixes the
     snapshot S and reads the source identity (generation id, system identifier, database
     OID and name). Every table is read from S, so parents and children are consistent.
  3. Refuses to continue incrementally if the identity changed, the stored state is blocked,
     or the stored watermark/snapshot is ahead of S (xid8 comparisons done by PostgreSQL).
  4. Reads every row with sync_xid >= W (W = pg_snapshot_xmin of the last committed run).
  5. In ONE target transaction: upserts mutable tables unconditionally (committed runs are
     applied in order; no updated_at or sync_xid comparison), inserts new events and checks
     re-read events against the stored copy, then stores W := pg_snapshot_xmin(S), S itself
     and the run's metrics. Any error rolls all of it back; the run row records the failure.

Guarantee (under the stated assumptions): after a successful run, raw.* holds the latest
state of every source row committed before S. Not captured: hard deletes, and intermediate
versions of mutable rows that changed more than once between runs (payment history is
complete through payment_events, which is append-only).

Transaction ids are never compared in Python: values travel as text and are compared with
xid8 operators and pg_snapshot functions in SQL. The numeric conversion in the lag metric is
for reporting only.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Callable

import psycopg
from dotenv import load_dotenv
from psycopg import sql

from sync.incremental_sync import require_env
from tools.db_guard import assert_connected_to, assert_writable

log = logging.getLogger("v2_sync")

V2_PREFIXES = ("v2_",)
SOURCE_NAME = "v2_oltp"
LOCK_KEY = (48210002, 1)          # (namespace, id) for pg_try_advisory_lock on the target
BATCH = 1000
META_COLUMNS = ("_sync_xid", "_run_id", "_synced_at")


@dataclass(frozen=True)
class Table:
    name: str
    pk: str
    columns: tuple[str, ...]
    append_only: bool = False


# Parents before children (raw tables carry no foreign keys; order is for readability).
TABLES: tuple[Table, ...] = (
    Table("currencies", "currency_code", ("currency_code", "minor_units", "name")),
    Table("users", "user_id", ("user_id", "email", "full_name", "country", "created_at", "updated_at")),
    Table("merchants", "merchant_id",
          ("merchant_id", "merchant_name", "category", "country", "created_at", "updated_at")),
    Table("transactions", "transaction_id",
          ("transaction_id", "user_id", "merchant_id", "amount_minor", "currency", "status",
           "last_event_sequence", "failure_code", "created_at", "updated_at", "authorized_at",
           "captured_at", "failed_at")),
    Table("refunds", "refund_id",
          ("refund_id", "transaction_id", "amount_minor", "currency", "reason", "status",
           "idempotency_key", "failure_code", "created_at", "updated_at", "requested_at",
           "succeeded_at", "failed_at")),
    Table("payment_events", "event_id",
          ("event_id", "transaction_id", "event_sequence", "event_type", "amount_minor", "currency",
           "failure_code", "source", "occurred_at", "received_at"), append_only=True),
)


class SyncError(RuntimeError):
    """Base class for refusals and failures of a sync run."""
    status = "failed"


class SyncAlreadyRunning(SyncError):
    status = "rejected"


class SyncBlocked(SyncError):
    """The target is blocked (or inconsistent) and needs an explicit reinitialization."""
    status = "blocked"


class SourceIdentityChanged(SyncBlocked):
    """The source is not the database the checkpoint belongs to (rebuild, restore, replacement)."""


class EventConflict(SyncError):
    def __init__(self, message: str, conflicts: list[dict]):
        super().__init__(message)
        self.conflicts = conflicts


class InjectedFailure(SyncError):
    """Raised by fail_before_commit after all target writes, to prove they roll back together."""


class SuperuserRefused(SyncError):
    """Normal execution must use the least-privilege sync login, not a superuser."""


class ReinitializationNotPermitted(SyncError):
    """Reinitialization truncates raw.*; the least-privilege sync role cannot, by design."""


@dataclass(frozen=True)
class SyncCredentials:
    user: str
    password: str = field(repr=False)

    @classmethod
    def from_env(cls) -> "SyncCredentials":
        """The least-privilege sync login (member of v2_sync): V2_SYNC_USER / V2_SYNC_PASSWORD."""
        return cls(os.getenv("V2_SYNC_USER", "v2_sync_login"), require_env("V2_SYNC_PASSWORD"))

    @classmethod
    def admin(cls) -> "SyncCredentials":
        """The database owner (break-glass and operator procedures such as reinitialization)."""
        return cls(os.getenv("POSTGRES_USER", "fintech"), require_env("POSTGRES_PASSWORD"))

    def conninfo(self, dbname: str) -> str:
        return psycopg.conninfo.make_conninfo(host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
                                              port=os.getenv("POSTGRES_PORT", "5434"),
                                              user=self.user, password=self.password, dbname=dbname)


def _refuse_superuser(conn: psycopg.Connection, allow_superuser: bool) -> None:
    if not allow_superuser and conn.execute(
            "SELECT rolsuper FROM pg_roles WHERE rolname = current_user").fetchone()[0]:
        raise SuperuserRefused("refusing to sync as a superuser; use the v2_sync login "
                               "(or allow_superuser / --as-admin for operator procedures)")


@dataclass
class RunResult:
    run_id: int
    mode: str
    status: str
    watermark_from: str
    watermark_to: str
    snapshot: str
    rows_read: dict[str, int] = field(default_factory=dict)
    rows_inserted: dict[str, int] = field(default_factory=dict)
    rows_updated: dict[str, int] = field(default_factory=dict)
    rows_reread: int = 0
    duplicate_events: int = 0
    watermark_lag_xids: int = 0
    oldest_open_writer_secs: float | None = None


@dataclass(frozen=True)
class SourceIdentity:
    generation: str
    system_identifier: int
    database_oid: int
    database_name: str


# --- entry point -------------------------------------------------------------------------------

def run_sync(source_db: str = "v2_oltp", target_db: str = "v2_analytics", *, reinitialize: bool = False,
             lookback_runs: int = 0, fail_before_commit: bool = False,
             credentials: SyncCredentials | None = None, allow_superuser: bool = False,
             before_target_commit: Callable[[], None] | None = None) -> RunResult:
    """Run one sync. Raises a SyncError subclass on refusal or failure (the run row records it).

    Connects as the least-privilege sync login (SyncCredentials.from_env()) unless credentials
    are given; a superuser is refused unless allow_superuser is set explicitly."""
    assert_writable(source_db, allowed_prefixes=V2_PREFIXES, purpose="V2 sync source")
    assert_writable(target_db, allowed_prefixes=V2_PREFIXES, purpose="V2 sync target")
    if lookback_runs < 0:
        raise ValueError("lookback_runs must be >= 0")
    creds = credentials or SyncCredentials.from_env()

    with psycopg.connect(creds.conninfo(target_db), autocommit=True) as target:
        assert_connected_to(target, target_db)
        _refuse_superuser(target, allow_superuser)
        if not target.execute("SELECT pg_try_advisory_lock(%s, %s)", LOCK_KEY).fetchone()[0]:
            run_id = _insert_run(target, "incremental", lookback_runs, status="rejected")
            message = "another sync run holds the target lock"
            _finish_run(target, run_id, "rejected", SyncAlreadyRunning.__name__, message)
            raise SyncAlreadyRunning(message)
        try:
            # We hold the lock, so a 'running' row can only belong to a process that died.
            target.execute("UPDATE meta.sync_runs SET status = 'abandoned', finished_at = clock_timestamp(), "
                           "error_message = 'process ended without finishing the run' "
                           "WHERE source_name = %s AND status = 'running'", (SOURCE_NAME,))
            state = _read_state(target)
            mode = "reinitialize" if reinitialize else ("initial" if state is None else "incremental")
            run_id = _insert_run(target, mode, lookback_runs)
            try:
                result = _run(source_db, creds, allow_superuser, target, run_id, mode, state, lookback_runs,
                              fail_before_commit, before_target_commit)
            except Exception as exc:
                _record_failure(target, run_id, exc)
                raise
            log.info("run %s %s: %s", run_id, mode, result)
            return result
        finally:
            target.execute("SELECT pg_advisory_unlock(%s, %s)", LOCK_KEY)


# --- one run -----------------------------------------------------------------------------------

def _run(source_db: str, creds: SyncCredentials, allow_superuser: bool, target: psycopg.Connection, run_id: int,
         mode: str, state: dict | None, lookback_runs: int, fail_before_commit: bool,
         before_target_commit: Callable[[], None] | None) -> RunResult:
    if mode == "reinitialize" and not target.execute(
            "SELECT bool_and(has_table_privilege(format('raw.%%I', t), 'TRUNCATE')) FROM unnest(%s::text[]) t",
            ([t.name for t in TABLES],)).fetchone()[0]:
        raise ReinitializationNotPermitted(
            "reinitialization truncates raw.* and is an operator procedure: rerun with the owner's credentials "
            "(python -m sync.v2_sync --as-admin --reinitialize)")
    with psycopg.connect(creds.conninfo(source_db)) as source:
        assert_connected_to(source, source_db)
        _refuse_superuser(source, allow_superuser)
        source.rollback()                       # end the check's transaction; the sync snapshot starts below
        source.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        source.read_only = True
        # The first statement of the transaction fixes the snapshot every later read uses.
        snap, xmin, lag, generation, sysid, dboid, dbname = source.execute("""
            SELECT pg_current_snapshot()::text,
                   pg_snapshot_xmin(pg_current_snapshot())::text,
                   pg_snapshot_xmax(pg_current_snapshot())::text::numeric
                     - pg_snapshot_xmin(pg_current_snapshot())::text::numeric,      -- metric only
                   (SELECT generation_id::text FROM sync_source_identity),
                   (SELECT system_identifier FROM pg_control_system()),
                   (SELECT oid::bigint FROM pg_database WHERE datname = current_database()),
                   current_database()""").fetchone()
        identity = SourceIdentity(generation, sysid, dboid, dbname)
        # Metric only, via the narrow SECURITY DEFINER helper (V008): other sessions' transaction
        # details in pg_stat_activity are hidden from the unprivileged sync role.
        oldest_writer = source.execute("SELECT sync_oldest_open_writer_secs()").fetchone()[0]

        watermark_from, prev_snapshot = _starting_point(source, target, mode, state, identity, snap, lookback_runs)

        result = RunResult(run_id, mode, "succeeded", watermark_from, xmin, snap,
                           watermark_lag_xids=int(lag),
                           oldest_open_writer_secs=float(oldest_writer) if oldest_writer is not None else None)
        with target.transaction():
            if mode == "reinitialize":
                target.execute("TRUNCATE " + ", ".join(f"raw.{t.name}" for t in TABLES))
            for table in TABLES:
                _sync_table(source, target, table, run_id, watermark_from, prev_snapshot, result)
            _save_state(target, identity, xmin, snap, run_id)
            _save_success(target, result, identity)
            if before_target_commit:
                before_target_commit()
            if fail_before_commit:
                raise InjectedFailure("injected failure before the target commit")
        source.rollback()
    return result


def _starting_point(source, target, mode, state, identity: SourceIdentity, snap: str,
                    lookback_runs: int) -> tuple[str, str | None]:
    """Returns (watermark to read from, previous snapshot) or raises if continuing is unsafe."""
    if mode == "reinitialize":
        return "0", None
    if mode == "initial":
        filled = [t.name for t in TABLES
                  if target.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM raw.{})").format(
                      sql.Identifier(t.name))).fetchone()[0]]
        if filled:
            raise SyncBlocked(f"no sync state but raw tables hold data ({', '.join(filled)}); "
                              "run an explicit reinitialization")
        return "0", None

    if state["status"] == "blocked":
        raise SyncBlocked(f"sync is blocked: {state['blocked_reason']}; run an explicit reinitialization")
    changed = [f"{name} {old!r} -> {new!r}" for name, old, new in (
        ("generation", state["source_generation"], identity.generation),
        ("system identifier", state["source_system_identifier"], identity.system_identifier),
        ("database oid", state["source_database_oid"], identity.database_oid),
        ("database name", state["source_database_name"], identity.database_name)) if old != new]
    if changed:
        raise SourceIdentityChanged("source identity changed: " + "; ".join(changed))
    # Same lineage means transaction ids only move forward: the stored watermark and snapshot
    # can never be ahead of the current snapshot. Compared by PostgreSQL as xid8 / pg_snapshot.
    behind, xmax_ok = source.execute(
        "SELECT %s::xid8 <= pg_snapshot_xmin(%s::pg_snapshot), "
        "pg_snapshot_xmax(%s::pg_snapshot) <= pg_snapshot_xmax(%s::pg_snapshot)",
        (state["watermark"], snap, state["last_snapshot"], snap)).fetchone()
    if not (behind and xmax_ok):
        raise SourceIdentityChanged(
            f"stored watermark {state['watermark']} / snapshot {state['last_snapshot']} is ahead of the source "
            f"snapshot {snap}: the source was restored or replaced")

    watermark = state["watermark"]
    if lookback_runs:
        row = target.execute(
            "SELECT watermark_from::text FROM meta.sync_runs WHERE source_name = %s AND status = 'succeeded' "
            "AND source_generation = %s ORDER BY run_id DESC OFFSET %s LIMIT 1",
            (SOURCE_NAME, identity.generation, lookback_runs - 1)).fetchone()
        watermark = row[0] if row else "0"
    return watermark, state["last_snapshot"]


def _sync_table(source, target, table: Table, run_id: int, watermark: str, prev_snapshot: str | None,
                result: RunResult) -> None:
    cols = sql.SQL(", ").join(map(sql.Identifier, table.columns))
    stage = sql.Identifier(f"stage_{table.name}")
    target.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE raw.{} INCLUDING DEFAULTS) ON COMMIT DROP").format(
        stage, sql.Identifier(table.name)))
    target.execute(sql.SQL("ALTER TABLE {} DROP COLUMN _run_id, DROP COLUMN _synced_at").format(stage))

    query = sql.SQL("""
        SELECT {cols}, sync_xid::text,
               coalesce(pg_visible_in_snapshot(sync_xid, %(prev)s::pg_snapshot), false)
        FROM {table} WHERE sync_xid >= %(w)s::xid8 ORDER BY {pk}""").format(
        cols=cols, table=sql.Identifier(table.name), pk=sql.Identifier(table.pk))
    read = reread = 0
    with source.cursor(name=f"read_{table.name}") as cur, \
            target.cursor().copy(sql.SQL("COPY {} ({}, _sync_xid) FROM STDIN").format(stage, cols)) as copy:
        cur.execute(query, {"w": watermark, "prev": prev_snapshot})
        while rows := cur.fetchmany(BATCH):
            for row in rows:
                copy.write_row(row[:-1])
                reread += row[-1]
            read += len(rows)
    result.rows_read[table.name] = read
    result.rows_reread += reread

    target_cols = sql.SQL(", ").join(map(sql.Identifier, (*table.columns, *META_COLUMNS)))
    source_cols = sql.SQL(", ").join([*map(sql.Identifier, table.columns), sql.Identifier("_sync_xid"),
                                      sql.Literal(run_id), sql.SQL("clock_timestamp()")])
    if table.append_only:
        result.duplicate_events += _insert_events(target, table, stage, cols, target_cols, source_cols, result)
        return
    updates = sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c))
        for c in (*table.columns, *META_COLUMNS) if c != table.pk)
    rows = target.execute(sql.SQL("""
        INSERT INTO raw.{table} ({target_cols}) SELECT {source_cols} FROM {stage}
        ON CONFLICT ({pk}) DO UPDATE SET {updates}
        RETURNING (xmax = 0)""").format(table=sql.Identifier(table.name), target_cols=target_cols,
                                       source_cols=source_cols, stage=stage, pk=sql.Identifier(table.pk),
                                       updates=updates)).fetchall()
    result.rows_inserted[table.name] = sum(1 for (ins,) in rows if ins)
    result.rows_updated[table.name] = sum(1 for (ins,) in rows if not ins)


def _insert_events(target, table: Table, stage, cols, target_cols, source_cols, result: RunResult) -> int:
    payload = sql.SQL(", ").join(sql.SQL("{}.{}").format(sql.Identifier(a), sql.Identifier(c))
                                 for a in ("x",) for c in table.columns)
    stored_payload = sql.SQL(", ").join(sql.SQL("r.{}").format(sql.Identifier(c)) for c in table.columns)
    # A re-read event must equal the stored copy; a different event must not claim a stored
    # (payment, sequence). Both are conflicts: the run fails instead of picking a version.
    conflicts = target.execute(sql.SQL("""
        SELECT x.event_id::text, to_jsonb(r) - '_sync_xid' - '_run_id' - '_synced_at', to_jsonb(x) - '_sync_xid'
        FROM {stage} x
        JOIN raw.payment_events r
          ON r.event_id = x.event_id
          OR (r.transaction_id = x.transaction_id AND r.event_sequence = x.event_sequence)
        WHERE ({payload}) IS DISTINCT FROM ({stored})""").format(
        stage=stage, payload=payload, stored=stored_payload)).fetchall()
    if conflicts:
        raise EventConflict(f"{len(conflicts)} re-read payment event(s) differ from the stored copy",
                            [dict(key=k, stored=s, incoming=i) for k, s, i in conflicts])
    inserted = target.execute(sql.SQL("""
        WITH ins AS (INSERT INTO raw.payment_events ({target_cols}) SELECT {source_cols} FROM {stage}
                     ON CONFLICT (event_id) DO NOTHING RETURNING 1)
        SELECT count(*) FROM ins""").format(target_cols=target_cols, source_cols=source_cols,
                                            stage=stage)).fetchone()[0]
    result.rows_inserted[table.name] = inserted
    result.rows_updated[table.name] = 0
    return result.rows_read[table.name] - inserted


# --- bookkeeping -------------------------------------------------------------------------------

def _read_state(target) -> dict | None:
    row = target.execute("""
        SELECT source_generation::text, source_system_identifier, source_database_oid::bigint,
               source_database_name, watermark::text, last_snapshot::text, status, blocked_reason
        FROM meta.sync_state WHERE source_name = %s""", (SOURCE_NAME,)).fetchone()
    if row is None:
        return None
    keys = ("source_generation", "source_system_identifier", "source_database_oid", "source_database_name",
            "watermark", "last_snapshot", "status", "blocked_reason")
    return dict(zip(keys, row))


def _insert_run(target, mode: str, lookback_runs: int, status: str = "running") -> int:
    return target.execute("INSERT INTO meta.sync_runs (source_name, mode, status, lookback_runs) "
                          "VALUES (%s, %s, %s, %s) RETURNING run_id",
                          (SOURCE_NAME, mode, status, lookback_runs)).fetchone()[0]


def _finish_run(target, run_id: int, status: str, error_class: str, message: str) -> None:
    target.execute("UPDATE meta.sync_runs SET status = %s, finished_at = clock_timestamp(), error_class = %s, "
                   "error_message = %s WHERE run_id = %s", (status, error_class, message, run_id))


def _save_state(target, identity: SourceIdentity, watermark: str, snap: str, run_id: int) -> None:
    target.execute("""
        INSERT INTO meta.sync_state (source_name, source_generation, source_system_identifier, source_database_oid,
                                     source_database_name, watermark, last_snapshot, last_run_id, status)
        VALUES (%s, %s, %s, %s, %s, %s::xid8, %s::pg_snapshot, %s, 'active')
        ON CONFLICT (source_name) DO UPDATE SET
            source_generation = EXCLUDED.source_generation,
            source_system_identifier = EXCLUDED.source_system_identifier,
            source_database_oid = EXCLUDED.source_database_oid,
            source_database_name = EXCLUDED.source_database_name,
            watermark = EXCLUDED.watermark, last_snapshot = EXCLUDED.last_snapshot,
            last_run_id = EXCLUDED.last_run_id, status = 'active', blocked_reason = NULL,
            updated_at = clock_timestamp()""",
                   (SOURCE_NAME, identity.generation, identity.system_identifier, identity.database_oid,
                    identity.database_name, watermark, snap, run_id))


def _save_success(target, r: RunResult, identity: SourceIdentity) -> None:
    target.execute("""
        UPDATE meta.sync_runs SET status = 'succeeded', finished_at = clock_timestamp(),
            source_generation = %s, watermark_from = %s::xid8, watermark_to = %s::xid8, snapshot = %s::pg_snapshot,
            rows_read = %s, rows_inserted = %s, rows_updated = %s, rows_reread = %s, duplicate_events = %s,
            watermark_lag_xids = %s, oldest_open_writer_secs = %s
        WHERE run_id = %s""",
                   (identity.generation, r.watermark_from, r.watermark_to, r.snapshot, json.dumps(r.rows_read),
                    json.dumps(r.rows_inserted), json.dumps(r.rows_updated), r.rows_reread, r.duplicate_events,
                    r.watermark_lag_xids, r.oldest_open_writer_secs, r.run_id))


def _record_failure(target, run_id: int, exc: Exception) -> None:
    """Runs after the data transaction rolled back, in its own transaction, so it survives."""
    status = exc.status if isinstance(exc, SyncError) else "failed"
    with target.transaction():
        _finish_run(target, run_id, status, type(exc).__name__, str(exc)[:2000])
        if isinstance(exc, SourceIdentityChanged):
            target.execute("UPDATE meta.sync_state SET status = 'blocked', blocked_reason = %s, "
                           "updated_at = clock_timestamp() WHERE source_name = %s", (str(exc)[:2000], SOURCE_NAME))
        if isinstance(exc, EventConflict):
            for c in exc.conflicts:
                target.execute("INSERT INTO meta.sync_conflicts (run_id, table_name, row_key, stored, incoming) "
                               "VALUES (%s, 'payment_events', %s, %s, %s)",
                               (run_id, c["key"], json.dumps(c["stored"]), json.dumps(c["incoming"])))


# --- CLI ---------------------------------------------------------------------------------------

def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="V2 incremental sync (transaction-ID watermark).")
    parser.add_argument("--source", default="v2_oltp")
    parser.add_argument("--target", default="v2_analytics")
    parser.add_argument("--reinitialize", action="store_true",
                        help="explicitly replace all raw data with a full copy of the current source")
    parser.add_argument("--lookback-runs", type=int, default=0,
                        help="also re-read the windows of the last N successful runs (safe reprocessing)")
    parser.add_argument("--fail-before-commit", action="store_true",
                        help="raise after all target writes, before commit (failure injection)")
    parser.add_argument("--as-admin", action="store_true",
                        help="connect with the owner's credentials (operator procedures such as --reinitialize); "
                             "normal runs use the least-privilege V2_SYNC_USER login")
    args = parser.parse_args()
    creds = SyncCredentials.admin() if args.as_admin else SyncCredentials.from_env()
    try:
        r = run_sync(args.source, args.target, reinitialize=args.reinitialize, lookback_runs=args.lookback_runs,
                     fail_before_commit=args.fail_before_commit, credentials=creds, allow_superuser=args.as_admin)
    except SyncError as exc:
        print(f"sync {exc.status}: {type(exc).__name__}: {exc}")
        raise SystemExit(2)
    print(f"run {r.run_id} {r.mode} {r.status} as {creds.user}: watermark {r.watermark_from} -> {r.watermark_to}, "
          f"read {r.rows_read}, inserted {r.rows_inserted}, updated {r.rows_updated}, re-read {r.rows_reread}, "
          f"duplicate events {r.duplicate_events}, lag {r.watermark_lag_xids} xids, "
          f"oldest open writer {'-' if r.oldest_open_writer_secs is None else f'{r.oldest_open_writer_secs}s'}")


if __name__ == "__main__":
    main()
