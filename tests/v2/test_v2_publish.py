"""P1: write-audit-publish. Candidate marts reach `marts` only for one validated pipeline run,
atomically, and nothing that fails before COMMIT changes the published marts or their metadata.
Disposable v2_test_* databases; dbt runs for real."""

from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from payments.events import PaymentEvent, record_payment_event
from sync.v2_publish import PublishRefused, publish
from sync.v2_reconcile import reconcile
from sync.v2_sync import SyncCredentials, run_sync
from tests.v2.conftest import ANALYTICS_MIGRATIONS, PROJECT_ROOT, _admin, connect, quiet
from tests.v2.test_lifecycle import build
from tools.db_guard import UnsafeDatabaseTarget
from tools.migrate import migrate

DBT = PROJECT_ROOT / ".venv-dbt" / "bin" / "dbt"
pytestmark = pytest.mark.skipif(not DBT.exists(), reason="dbt virtualenv (.venv-dbt) not installed")
MARTS = ("fct_payments", "fct_daily_payments", "fct_merchant_performance")
T0 = datetime(2026, 11, 2, 9, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    suffix = uuid.uuid4().hex[:6]
    source, target = f"v2_test_pub_src_{suffix}", f"v2_test_pub_tgt_{suffix}"
    with _admin() as admin:
        for name in (source, target):
            admin.execute(f'CREATE DATABASE "{name}"')
    try:
        made = iter([source])
        build(lambda _label: next(made), "pub")
        migrate(target, ANALYTICS_MIGRATIONS, log=quiet)
        yield {"source": source, "target": target, "artifacts": tmp_path_factory.mktemp("pub"), "tid": iter(range(8000, 9000))}
    finally:
        with _admin() as admin:
            for name in (source, target):
                assert name.startswith("v2_test_")
                admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def add_payment(env, amount: int = 1000) -> int:
    tid = next(env["tid"])
    with connect(env["source"]) as c:
        c.execute("SET ROLE v2_payment_app")
        for seq, kind in enumerate(("created", "authorized", "captured"), start=1):
            extra = dict(user_id=1, merchant_id=101) if kind == "created" else {}
            record_payment_event(c, PaymentEvent(uuid.uuid4(), tid, seq, f"payment.{kind}", T0 + timedelta(minutes=seq),
                                                 "api", amount, "USD", **extra))
    return tid


def dbt_build(env, run_id: str, *, inject_failure: bool = False):
    target_path = env["artifacts"] / run_id / "target"
    result = subprocess.run(
        [str(DBT), "--no-use-colors", "build", "--profiles-dir", "..", "--target-path", str(target_path),
         "--log-path", str(env["artifacts"] / run_id / "logs"),
         "--vars", f'{{v2_pipeline_run_id: "{run_id}", v2_inject_test_failure: {str(inject_failure).lower()}}}'],
        cwd=PROJECT_ROOT / "dbt" / "v2", env={**os.environ, "DBT_V2_DBNAME": env["target"]},
        capture_output=True, text=True, timeout=300)
    return result, target_path / "run_results.json"


def validated_run(env, run_id: str, *, change: bool = True):
    """sync -> dbt build (candidate) -> recorded reconciliation; returns the run_results path."""
    if change:
        add_payment(env)
    run_sync(env["source"], env["target"], credentials=SyncCredentials.admin(), allow_superuser=True)
    result, run_results = dbt_build(env, run_id)
    assert result.returncode == 0, result.stdout[-3000:]
    report = reconcile(env["source"], env["target"], pipeline_run_id=run_id, record=True)
    assert report.failed == 0, report.results
    return run_results


def published_state(env) -> tuple:
    with connect(env["target"]) as c:
        marts = tuple(c.execute(f"SELECT md5(coalesce(string_agg(t::text, ',' ORDER BY t::text), '')) "
                                f"FROM marts.{m} t").fetchone()[0] for m in MARTS)
        pubs = c.execute("SELECT count(*), max(publication_id) FROM meta.mart_publications").fetchone()
    return marts, pubs


def candidate_equals_published(env) -> bool:
    with connect(env["target"]) as c:
        return all(c.execute(f"SELECT NOT EXISTS (SELECT * FROM marts_candidate.{m} EXCEPT ALL SELECT * FROM marts.{m}) "
                             f"AND NOT EXISTS (SELECT * FROM marts.{m} EXCEPT ALL SELECT * FROM marts_candidate.{m})"
                             ).fetchone()[0] for m in MARTS)


# --- happy path ------------------------------------------------------------------------------

def test_validated_run_is_published_atomically(env):
    run_results = validated_run(env, "run_ok_1", change=False)
    result = publish(env["target"], "run_ok_1", run_results)
    assert result["previous_publication_id"] is None and set(result["row_counts"]) == set(MARTS)
    assert candidate_equals_published(env)
    with connect(env["target"]) as c:
        row = c.execute("SELECT pipeline_run_id, sync_run_id, validation_id FROM marts.current_publication").fetchone()
        assert row == ("run_ok_1", result["sync_run_id"], result["validation_id"])


# --- identity and staleness checks (each refusal changes nothing) -------------------------------

def refused(env, run_id, run_results, match):
    before = published_state(env)
    with pytest.raises(PublishRefused, match=match):
        publish(env["target"], run_id, run_results)
    assert published_state(env) == before


def test_run_results_of_another_pipeline_run_are_refused(env):
    first = validated_run(env, "run_a")
    refused(env, "run_b", first, "belong to pipeline run 'run_a'")


def test_stale_run_results_of_an_earlier_invocation_are_refused(env):
    stale = validated_run(env, "run_c")
    kept = env["artifacts"] / "stale_run_results.json"
    shutil.copy(stale, kept)
    validated_run(env, "run_c", change=False)        # same pipeline run id, a newer dbt invocation
    refused(env, "run_c", kept, "was built by pipeline run 'run_c' / dbt invocation")


def test_missing_or_failed_validation_is_refused(env):
    add_payment(env)
    run_sync(env["source"], env["target"], credentials=SyncCredentials.admin(), allow_superuser=True)
    result, run_results = dbt_build(env, "run_d")
    assert result.returncode == 0
    refused(env, "run_d", run_results, "no passing reconciliation")
    assert reconcile(env["source"], env["target"], pipeline_run_id="run_d", record=True, inject_failure=True).failed == 1
    refused(env, "run_d", run_results, "no passing reconciliation")


def test_candidate_changed_after_validation_is_refused(env):
    run_results = validated_run(env, "run_e")
    with connect(env["target"]) as c:
        c.execute("UPDATE marts_candidate.fct_daily_payments SET captured_amount_minor = captured_amount_minor + 1 "
                  "WHERE ctid = (SELECT min(ctid) FROM marts_candidate.fct_daily_payments)")
    refused(env, "run_e", run_results, "changed after they were reconciled")


def test_a_sync_after_the_build_is_refused(env):
    run_results = validated_run(env, "run_f")
    add_payment(env)
    run_sync(env["source"], env["target"], credentials=SyncCredentials.admin(), allow_superuser=True)
    refused(env, "run_f", run_results, "no longer matches the candidate's sync run")


def test_failed_dbt_build_is_refused(env):
    add_payment(env)
    run_sync(env["source"], env["target"], credentials=SyncCredentials.admin(), allow_superuser=True)
    result, run_results = dbt_build(env, "run_g", inject_failure=True)
    assert result.returncode != 0
    refused(env, "run_g", run_results, "did not pass")


def test_non_v2_database_is_refused(env, tmp_path):
    with pytest.raises(UnsafeDatabaseTarget):
        publish("fintech_analytics", "x", tmp_path / "none.json")


# --- rollback and concurrency ------------------------------------------------------------------

def test_failure_inside_the_publish_transaction_rolls_everything_back(env):
    run_results = validated_run(env, "run_h")
    before = published_state(env)
    with pytest.raises(RuntimeError, match="injected failure after publishing fct_daily_payments"):
        publish(env["target"], "run_h", run_results, fail_after_table="fct_daily_payments")
    assert published_state(env) == before            # fct_payments was already copied: rolled back too
    publish(env["target"], "run_h", run_results)
    assert candidate_equals_published(env)


def test_concurrent_publishers_are_serialized(env):
    run_results = validated_run(env, "run_i")
    gate, outcomes = threading.Barrier(2), []

    def go():
        gate.wait()
        try:
            outcomes.append(publish(env["target"], "run_i", run_results)["publication_id"])
        except PublishRefused as exc:
            outcomes.append(str(exc))

    threads = [threading.Thread(target=go) for _ in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sum(isinstance(o, int) for o in outcomes) == 1
    assert any(isinstance(o, str) and "already published" in o for o in outcomes)
    with connect(env["target"]) as c:
        assert c.execute("SELECT count(*) FROM meta.mart_publications WHERE pipeline_run_id = 'run_i'").fetchone()[0] == 1


def test_repeatable_read_reader_keeps_its_publication_and_does_not_block(env):
    run_results = validated_run(env, "run_j")
    reader = psycopg.connect(SyncCredentials.admin().conninfo(env["target"]))
    reader.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    q = ("SELECT (SELECT publication_id FROM marts.current_publication), (SELECT count(*) FROM marts.fct_payments), "
         "(SELECT sum(captured_amount_minor) FROM marts.fct_daily_payments)")
    try:
        seen_before = reader.execute(q).fetchone()
        started = time.monotonic()
        publish(env["target"], "run_j", run_results, lock_timeout="5s")   # reader's open snapshot does not block it
        assert time.monotonic() - started < 5
        assert reader.execute(q).fetchone() == seen_before                 # same transaction: old publication
        reader.rollback()
        seen_after = reader.execute(q).fetchone()
        assert seen_after[0] > seen_before[0] and seen_after[1] == seen_before[1] + 1
    finally:
        reader.close()


def test_every_single_statement_sees_one_complete_publication(env):
    """While publications change underneath, each single statement observes all three marts and
    the publication record from the same publication. (Separate READ COMMITTED statements may each
    see a different publication; that is why consumers needing several queries use REPEATABLE READ.)"""
    stop, observations, errors = threading.Event(), [], []
    consistency = """
        SELECT (SELECT publication_id FROM marts.current_publication),
               (SELECT row_counts->>'fct_payments' FROM marts.current_publication)::int = (SELECT count(*) FROM marts.fct_payments),
               (SELECT coalesce(sum(amount_minor) FILTER (WHERE is_captured), 0) FROM marts.fct_payments)
                 = (SELECT coalesce(sum(captured_amount_minor), 0) FROM marts.fct_daily_payments),
               (SELECT coalesce(sum(captured_amount_minor), 0) FROM marts.fct_daily_payments)
                 = (SELECT coalesce(sum(captured_amount_minor), 0) FROM marts.fct_merchant_performance)"""

    def watch():
        with psycopg.connect(SyncCredentials.admin().conninfo(env["target"]), autocommit=True) as c:
            while not stop.is_set():
                try:
                    observations.append(c.execute(consistency).fetchone())
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

    watcher = threading.Thread(target=watch)
    watcher.start()
    try:
        for i in range(3):
            publish(env["target"], f"run_k{i}", validated_run(env, f"run_k{i}"))
    finally:
        stop.set()
        watcher.join()
    assert errors == []
    assert all(o[1] and o[2] and o[3] for o in observations), [o for o in observations if not all(o[1:])][:3]
    assert len({o[0] for o in observations}) >= 2          # publications really changed while watched


# --- published tables keep their identity -----------------------------------------------------------

def test_dependent_views_and_grants_survive_publication(env):
    with connect(env["target"]) as c:
        c.execute("CREATE VIEW public.consumer_payments AS SELECT transaction_id, amount_minor FROM marts.fct_payments")
        c.execute("GRANT SELECT ON marts.fct_payments TO v2_payment_app")
    try:
        publish(env["target"], "run_l", validated_run(env, "run_l"))
        with connect(env["target"]) as c:
            assert c.execute("SELECT count(*) FROM public.consumer_payments").fetchone()[0] == \
                   c.execute("SELECT count(*) FROM marts_candidate.fct_payments").fetchone()[0]
            assert c.execute("SELECT has_table_privilege('v2_payment_app', 'marts.fct_payments', 'SELECT')").fetchone()[0]
    finally:
        with connect(env["target"]) as c:
            c.execute("REVOKE SELECT ON marts.fct_payments FROM v2_payment_app")
            c.execute("DROP VIEW public.consumer_payments")


def test_column_change_is_refused_while_a_view_depends_on_the_mart(env):
    run_results = validated_run(env, "run_m")
    with connect(env["target"]) as c:
        c.execute("ALTER TABLE marts_candidate.fct_payments ADD COLUMN note text")   # a changed model definition
        c.execute("CREATE VIEW public.consumer_payments AS SELECT transaction_id FROM marts.fct_payments")
    try:
        assert reconcile(env["source"], env["target"], pipeline_run_id="run_m", record=True).failed == 0
        refused(env, "run_m", run_results, "changed columns but objects depend on it")
        with connect(env["target"]) as c:
            c.execute("DROP VIEW public.consumer_payments")
        publish(env["target"], "run_m", run_results)
        with connect(env["target"]) as c:
            assert c.execute("SELECT count(*) FROM information_schema.columns WHERE table_schema = 'marts' "
                             "AND table_name = 'fct_payments' AND column_name = 'note'").fetchone()[0] == 1
    finally:
        with connect(env["target"]) as c:
            c.execute("DROP VIEW IF EXISTS public.consumer_payments")
