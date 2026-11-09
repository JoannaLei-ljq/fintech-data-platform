"""P3: explicit refund provenance (OLTP V009 record_source, analytics V005) and timestamp flags."""

from __future__ import annotations

import uuid

import psycopg
import pytest

from payments.refunds import request_refund
from sync.v2_sync import SyncCredentials, run_sync
from tests.v2.conftest import ANALYTICS_MIGRATIONS, MVP_DEMO_SEED, OLTP_MIGRATIONS, connect, quiet, run_sql_file
from tests.v2.test_lifecycle import build
from tools.migrate import migrate


@pytest.fixture
def db(make_db) -> str:
    return build(make_db, "prov")


def app(dbname: str):
    c = connect(dbname)
    c.execute("SET ROLE v2_payment_app")
    return c


def test_migrated_refunds_are_labelled_without_changing_history(make_db):
    before = make_db("prov_v8")
    migrate(before, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(before, MVP_DEMO_SEED)
    migrate(before, OLTP_MIGRATIONS, target_version=8, log=quiet)
    with connect(before) as c:
        cols = [d.name for d in c.execute("SELECT * FROM refunds LIMIT 0").description if d.name != "sync_xid"]
        history = c.execute(f"SELECT {', '.join(cols)} FROM refunds ORDER BY refund_id").fetchall()
        xids = c.execute("SELECT sync_xid::text FROM refunds ORDER BY refund_id").fetchall()
    migrate(before, OLTP_MIGRATIONS, log=quiet)                  # V009
    with connect(before) as c:
        rows = c.execute("SELECT refund_id, record_source FROM refunds ORDER BY refund_id").fetchall()
        assert rows == [(7001, "migration_backfill"), (7002, "migration_backfill")]
        after = c.execute(f"SELECT {', '.join(cols)} FROM refunds ORDER BY refund_id").fetchall()
        # every business and lifecycle column, including updated_at and succeeded_at, is unchanged;
        # only sync_xid is re-stamped (on purpose, so the next sync carries record_source to raw)
        assert after == history
        assert c.execute("SELECT sync_xid::text FROM refunds ORDER BY refund_id").fetchall() != xids


def test_v009_refuses_to_guess_when_the_legacy_prefix_was_reused(make_db):
    db = make_db("prov_spoof")
    migrate(db, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(db, MVP_DEMO_SEED)
    migrate(db, OLTP_MIGRATIONS, target_version=8, log=quiet)
    with app(db) as c:            # before V009, an API refund could use the prefix
        request_refund(c, idempotency_key="legacy-refund:999", transaction_id=3002, amount_minor=100,
                       currency="USD", reason="r")
    with pytest.raises(psycopg.errors.RaiseException, match="refusing to guess provenance"):
        migrate(db, OLTP_MIGRATIONS, log=quiet)
    with connect(db) as c:
        assert [v for (v,) in c.execute("SELECT version FROM schema_migrations ORDER BY 1")][-1] == 8


def test_api_refunds_are_api_and_cannot_claim_migrated_history(db):
    with app(db) as c:
        r = request_refund(c, idempotency_key=f"p-{uuid.uuid4().hex}", transaction_id=3002, amount_minor=100,
                           currency="USD", reason="r")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute("UPDATE refunds SET record_source = 'migration_backfill'")
    with connect(db) as c:
        assert c.execute("SELECT record_source FROM refunds WHERE refund_id = %s", (r.refund_id,)).fetchone() == ("api",)
        with pytest.raises(psycopg.errors.RestrictViolation, match="immutable"):        # not even the owner
            c.execute("UPDATE refunds SET record_source = 'api' WHERE refund_id = 7001")
        with pytest.raises(psycopg.errors.RestrictViolation, match="reserved for migrated history"):
            c.execute("INSERT INTO refunds (transaction_id, amount_minor, currency, reason, status, idempotency_key, "
                      "record_source) VALUES (3002, 1, 'USD', 'x', 'requested', 'owner-1', 'migration_backfill')")


def test_sync_carries_record_source_into_raw(db, make_db):
    target = make_db("prov_tgt")
    migrate(target, ANALYTICS_MIGRATIONS, log=quiet)
    with app(db) as c:
        request_refund(c, idempotency_key=f"p-{uuid.uuid4().hex}", transaction_id=3002, amount_minor=100,
                       currency="USD", reason="r")
    run_sync(db, target, credentials=SyncCredentials.admin(), allow_superuser=True)
    with connect(target) as c:
        assert dict(c.execute("SELECT record_source, count(*) FROM raw.refunds GROUP BY 1").fetchall()) == \
               {"migration_backfill": 2, "api": 1}


def test_v009_restamps_every_refund_so_the_next_sync_rereads_it(make_db):
    """Upgrade path: raw loaded before V009/V005 has no record_source. V009 re-stamps the sync_xid
    of every refund (and nothing else), so the next incremental sync re-reads all refunds and fills
    the new raw column; no reinitialization is needed. Deploy order: OLTP V009, analytics V005, code."""
    db = make_db("prov_restamp")
    migrate(db, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(db, MVP_DEMO_SEED)
    migrate(db, OLTP_MIGRATIONS, target_version=8, log=quiet)
    with connect(db) as c:
        before = dict(c.execute("SELECT refund_id, sync_xid::text FROM refunds").fetchall())
        others = c.execute("SELECT md5(string_agg(sync_xid::text, ',' ORDER BY transaction_id)) FROM transactions").fetchone()
    migrate(db, OLTP_MIGRATIONS, log=quiet)
    with connect(db) as c:
        after = dict(c.execute("SELECT refund_id, sync_xid::text FROM refunds").fetchall())
        assert c.execute("SELECT bool_and(sync_xid > %s::xid8) FROM refunds", (max(before.values(), key=int),)).fetchone()[0]
        assert c.execute("SELECT md5(string_agg(sync_xid::text, ',' ORDER BY transaction_id)) FROM transactions").fetchone() == others
    assert set(after) == set(before) and all(after[k] != before[k] for k in before)
