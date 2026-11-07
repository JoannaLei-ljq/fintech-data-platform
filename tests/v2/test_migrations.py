"""Step 1: migration runner (checksums, idempotency, atomicity) and baseline correctness."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tests.v2.catalog import snapshot
from tests.v2.conftest import (
    ANALYTICS_MIGRATIONS,
    OLTP_MIGRATIONS,
    PROJECT_ROOT,
    connect,
    quiet,
    run_sql_file,
)
from tools.db_guard import UnsafeDatabaseTarget
from tools.migrate import MigrationError, discover, migrate

MVP_OLTP_DDL = PROJECT_ROOT / "docker" / "postgres" / "init" / "schema" / "oltp_schema.sql"
MVP_ANALYTICS_DDL = PROJECT_ROOT / "docker" / "postgres" / "init" / "schema" / "analytics_schema.sql"


def copy_migrations(tmp_path: Path, up_to: int) -> Path:
    target = tmp_path / "migrations"
    target.mkdir()
    for m in discover(OLTP_MIGRATIONS)[:up_to]:
        shutil.copy(m.path, target / m.filename)
    return target


def history(dbname: str) -> list[tuple]:
    with connect(dbname) as conn:
        return conn.execute("SELECT version, name, checksum FROM schema_migrations ORDER BY version").fetchall()


# --- runner behaviour -------------------------------------------------------------

def test_applies_all_and_records_checksums(make_db):
    db = make_db("mig_all")
    applied = migrate(db, OLTP_MIGRATIONS, log=quiet)
    files = discover(OLTP_MIGRATIONS)
    assert applied == [m.version for m in files]
    assert history(db) == [(m.version, m.name, m.checksum) for m in files]


def test_rerun_is_a_noop(make_db):
    db = make_db("mig_rerun")
    migrate(db, OLTP_MIGRATIONS, log=quiet)
    before = history(db)
    assert migrate(db, OLTP_MIGRATIONS, log=quiet) == []
    assert history(db) == before


def test_target_version_stops_early(make_db):
    db = make_db("mig_to")
    assert migrate(db, OLTP_MIGRATIONS, target_version=1, log=quiet) == [1]
    assert [v for v, *_ in history(db)] == [1]


def test_detects_edited_migration(make_db, tmp_path):
    db = make_db("mig_tamper")
    directory = copy_migrations(tmp_path, up_to=1)
    migrate(db, directory, log=quiet)
    (directory / "V001__baseline.sql").write_text(
        (directory / "V001__baseline.sql").read_text() + "\n-- edited later\n")
    with pytest.raises(MigrationError, match="checksum mismatch for V001__baseline.sql"):
        migrate(db, directory, log=quiet)


def test_failed_migration_leaves_no_trace(make_db, tmp_path):
    db = make_db("mig_atomic")
    directory = copy_migrations(tmp_path, up_to=1)
    (directory / "V002__broken.sql").write_text(
        "CREATE TABLE should_not_exist (id int);\nSELECT * FROM no_such_table;\n")
    with pytest.raises(Exception, match="no_such_table"):
        migrate(db, directory, log=quiet)
    assert [v for v, *_ in history(db)] == [1]  # V001 committed, V002 not recorded
    with connect(db) as conn:
        assert conn.execute("SELECT to_regclass('should_not_exist')").fetchone()[0] is None


def test_applied_file_missing_is_an_error(make_db, tmp_path):
    db = make_db("mig_missing")
    directory = copy_migrations(tmp_path, up_to=1)
    (directory / "V002__extra.sql").write_text("CREATE TABLE extra (id int);\n")
    migrate(db, directory, log=quiet)
    (directory / "V002__extra.sql").unlink()
    with pytest.raises(MigrationError, match="V002 is recorded as applied but its file is missing"):
        migrate(db, directory, log=quiet)


@pytest.mark.parametrize("names", [["V001__a.sql", "V003__c.sql"], ["V002__b.sql"], ["V001__Bad-Name.sql"]])
def test_rejects_bad_migration_sets(tmp_path, names):
    for name in names:
        (tmp_path / name).write_text("SELECT 1;\n")
    with pytest.raises(MigrationError):
        discover(tmp_path)


@pytest.mark.parametrize("db", ["fintech_oltp", "fintech_analytics", "demo_oltp", "postgres"])
def test_refuses_non_v2_databases_before_connecting(db, monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "203.0.113.1")  # unroutable: proves no connection is attempted
    with pytest.raises(UnsafeDatabaseTarget):
        migrate(db, OLTP_MIGRATIONS, log=quiet)


# --- baseline correctness ---------------------------------------------------------

@pytest.mark.parametrize("label,ddl,migrations", [
    ("oltp", MVP_OLTP_DDL, OLTP_MIGRATIONS),
    ("analytics", MVP_ANALYTICS_DDL, ANALYTICS_MIGRATIONS),
])
def test_v001_baseline_matches_mvp_schema(make_db, label, ddl, migrations):
    reference = make_db(f"ref_{label}")
    baseline = make_db(f"base_{label}")
    run_sql_file(reference, ddl)
    migrate(baseline, migrations, target_version=1, log=quiet)
    with connect(reference) as ref, connect(baseline) as base:
        expected, actual = snapshot(ref), snapshot(base)
    assert expected["columns"] and expected["constraints"] and expected["indexes"]  # not vacuous
    for section in expected:
        assert actual[section] == expected[section], f"{label} baseline differs in {section}"
