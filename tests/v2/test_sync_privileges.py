"""Least-privilege V2 sync: what the v2_sync role can and cannot do (V008 oltp, V003 analytics)."""

from __future__ import annotations

import os

import psycopg
import pytest

from sync.v2_sync import ReinitializationNotPermitted, SuperuserRefused, SyncCredentials
from sync.v2_sync import run_sync as engine_run_sync
from tests.v2.conftest import connect
from tests.v2.test_v2_sync import SYNC_LOGIN, admin_sync, dbs, raw_snapshot, run_row, run_sync, txn  # noqa: F401

SOURCE_TABLES = {"users", "merchants", "currencies", "transactions", "refunds", "payment_events", "sync_source_identity"}
TABLE_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")


def login_conn(db: str) -> psycopg.Connection:
    return psycopg.connect(SYNC_LOGIN[-1].conninfo(db), autocommit=True)


def granted(conn, role_expr: str = "current_user", schemas=("public",)) -> set[tuple[str, str]]:
    """Every (schema.table, privilege) the role holds on ordinary tables in the given schemas."""
    rows = conn.execute(f"""
        SELECT n.nspname || '.' || c.relname, p
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, unnest(%s::text[]) p
        WHERE c.relkind IN ('r', 'p', 'v') AND n.nspname = ANY(%s)
          AND has_table_privilege({role_expr}, c.oid, p)""", (list(TABLE_PRIVILEGES), list(schemas))).fetchall()
    return set(rows)


# --- normal execution ---------------------------------------------------------------------------

def test_normal_runs_use_a_non_superuser_login(dbs):
    src, tgt = dbs
    r = run_sync(src, tgt)
    assert r.status == "succeeded"
    with login_conn(src) as c:
        assert c.execute("SELECT rolsuper, rolcanlogin FROM pg_roles WHERE rolname = current_user").fetchone() == \
               (False, True)
        assert [g for (g,) in c.execute(
            "SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid "
            "WHERE m.member = (SELECT oid FROM pg_roles WHERE rolname = current_user)")] == ["v2_sync"]


def test_superuser_is_refused_unless_explicitly_allowed(dbs):
    src, tgt = dbs
    with pytest.raises(SuperuserRefused):
        engine_run_sync(src, tgt, credentials=SyncCredentials.admin())
    with connect(tgt) as c:  # refused before taking the lock or writing a run row
        assert c.execute("SELECT count(*) FROM meta.sync_runs").fetchone()[0] == 0
    assert admin_sync(src, tgt).status == "succeeded"


def test_reinitialization_is_an_operator_procedure(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    before = raw_snapshot(tgt)
    with pytest.raises(ReinitializationNotPermitted, match="--as-admin"):
        run_sync(src, tgt, reinitialize=True)
    assert (run_row(tgt)["status"], run_row(tgt)["error_class"]) == ("failed", "ReinitializationNotPermitted")
    assert raw_snapshot(tgt) == before
    assert admin_sync(src, tgt, reinitialize=True).status == "succeeded"


def test_monitoring_helper_reveals_only_the_oldest_writer_age(dbs):
    src, _ = dbs
    writer = txn(src)
    writer.execute("UPDATE users SET full_name = full_name WHERE user_id = 1")
    try:
        with login_conn(src) as c:
            # Directly, the sync role cannot see when other sessions started their transactions...
            # (backend_xid is visible to everyone; xact_start, which an age needs, is not)
            hidden, listed = c.execute(
                "SELECT count(*) FILTER (WHERE xact_start IS NOT NULL), count(*) FILTER (WHERE backend_xid IS NOT NULL) "
                "FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND usename IS DISTINCT FROM current_user").fetchone()
            assert (hidden, listed >= 1) == (0, True)
            # ...the helper returns just the one number the sync records.
            assert c.execute("SELECT sync_oldest_open_writer_secs()").fetchone()[0] >= 0
    finally:
        writer.rollback()
        writer.close()


# --- the sync role cannot change the source ---------------------------------------------------------

@pytest.mark.parametrize("statement", [
    "INSERT INTO users (user_id, email, full_name, country) VALUES (99, 'x@x', 'X', 'US')",
    "UPDATE users SET full_name = 'x'",
    "UPDATE transactions SET status = 'failed', failure_code = 'x', failed_at = now(), last_event_sequence = 2 "
    "WHERE transaction_id = 3004",
    "DELETE FROM transactions",
    "TRUNCATE transactions CASCADE",
    "INSERT INTO payment_events SELECT * FROM payment_events LIMIT 0",
    "UPDATE refunds SET reason = 'x'",
    "DELETE FROM refunds",
    "UPDATE currencies SET minor_units = 3",
    "UPDATE sync_source_identity SET reason = 'x'",
    "SELECT record_payment_event(gen_random_uuid(), 3004, 2, 'payment.authorized', now(), 'api', 2000, 'USD')",
    "SELECT request_refund('k', 3001, 1, 'USD', 'r')",
    "SELECT complete_refund(7001, 'succeeded')",
    "SELECT rotate_sync_source_generation('x')",
    "SET ROLE v2_payment_app",
    "SET ROLE v2_payment_writer",
    "SET ROLE v2_sync_stats",
    f"SET ROLE {os.getenv('POSTGRES_USER', 'fintech')}",
    "SET session_replication_role = replica",
    "ALTER TABLE transactions DISABLE TRIGGER transactions_set_sync_xid",
    "CREATE TABLE public.shadow (x int)",
])
def test_sync_role_cannot_modify_the_source(dbs, statement):
    src, _ = dbs
    with connect(src) as c:
        before = [c.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall() for t in sorted(SOURCE_TABLES)]
    with login_conn(src) as c, pytest.raises(psycopg.errors.InsufficientPrivilege):
        c.execute(statement)
    with connect(src) as c:
        assert [c.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall() for t in sorted(SOURCE_TABLES)] == before


@pytest.mark.parametrize("statement", [
    "DELETE FROM raw.users",
    "TRUNCATE raw.users",
    "UPDATE raw.payment_events SET source = 'api'",
    "DELETE FROM raw.payment_events",
    "TRUNCATE raw.payment_events",
    "DROP TABLE raw.users",
    "ALTER TABLE raw.payment_events DISABLE TRIGGER payment_events_insert_only",
    "DELETE FROM meta.sync_runs",
    "DELETE FROM meta.sync_state",
    "UPDATE meta.sync_conflicts SET row_key = 'x'",
    "CREATE TABLE raw.shadow (x int)",
])
def test_sync_role_target_writes_are_limited(dbs, statement):
    src, tgt = dbs
    run_sync(src, tgt)
    before = raw_snapshot(tgt)
    with login_conn(tgt) as c, pytest.raises(psycopg.errors.InsufficientPrivilege):
        c.execute(statement)
    assert raw_snapshot(tgt) == before


def test_privilege_catalog_is_exactly_what_the_sync_needs(dbs):
    src, tgt = dbs
    with connect(src) as c:
        assert granted(c, "'v2_sync'") == {(f"public.{t}", "SELECT") for t in SOURCE_TABLES}
        for fn in ("record_payment_event", "request_refund", "complete_refund", "rotate_sync_source_generation"):
            assert not c.execute("SELECT bool_or(has_function_privilege('v2_sync', oid, 'EXECUTE')) FROM pg_proc "
                                 "WHERE proname = %s", (fn,)).fetchone()[0], fn
        helper = c.execute("""
            SELECT p.prosecdef, pg_get_userbyid(p.proowner), p.proconfig,
                   EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0),
                   has_function_privilege('v2_sync', p.oid, 'EXECUTE')
            FROM pg_proc p WHERE p.proname = 'sync_oldest_open_writer_secs'""").fetchone()
        assert helper == (True, "v2_sync_stats", ["search_path=pg_catalog, pg_temp"], False, True)
        roles = dict((r, rest) for r, *rest in c.execute(
            "SELECT rolname, rolcanlogin, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls "
            "FROM pg_roles WHERE rolname IN ('v2_sync', 'v2_sync_stats')"))
        assert roles == {"v2_sync": [False] * 6, "v2_sync_stats": [False] * 6}
        memberships = c.execute("""
            SELECT g.rolname, u.rolname FROM pg_auth_members m
            JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles u ON u.oid = m.member
            WHERE u.rolname IN ('v2_sync', 'v2_sync_stats') OR g.rolname = 'v2_sync_stats'""").fetchall()
        assert memberships == [("pg_read_all_stats", "v2_sync_stats")]   # v2_sync belongs to nothing
    with connect(tgt) as c:
        mutable = {f"raw.{t}" for t in ("users", "merchants", "currencies", "transactions", "refunds")}
        expected = ({(t, p) for t in mutable for p in ("SELECT", "INSERT", "UPDATE")}
                    | {("raw.payment_events", p) for p in ("SELECT", "INSERT")}
                    | {(t, p) for t in ("meta.sync_state", "meta.sync_runs") for p in ("SELECT", "INSERT", "UPDATE")}
                    | {("meta.sync_conflicts", p) for p in ("SELECT", "INSERT")})
        assert granted(c, "'v2_sync'", schemas=("raw", "meta", "public")) == expected


# --- protected databases ------------------------------------------------------------------------

@pytest.mark.parametrize("db,statements", [
    ("fintech_oltp", ["SELECT count(*) FROM transactions", "SELECT count(*) FROM refunds",
                      "INSERT INTO users (user_id, email, full_name, country) VALUES (99, 'x', 'x', 'US')",
                      "CREATE TABLE public.shadow (x int)"]),
    ("fintech_analytics", ["SELECT count(*) FROM raw.transactions", "SELECT * FROM meta.sync_checkpoints",
                           "DELETE FROM raw.refunds", "CREATE TABLE public.shadow (x int)"]),
])
def test_sync_role_has_no_access_to_protected_data(db, statements, dbs):
    """Read-only probes: the sync login holds no privilege on any table in the protected databases.
    (It can still open a connection there because PUBLIC has CONNECT by default; see the docs.)"""
    with login_conn(db) as c:
        assert granted(c, schemas=("public", "raw", "meta")) == set()
        for statement in statements:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                c.execute(statement)
