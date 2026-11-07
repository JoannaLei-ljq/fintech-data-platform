"""Step 0: database-name safety guards. No database writes happen in this module."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tools.db_guard import (
    MIGRATION_PREFIXES,
    PROTECTED_OVERRIDE_ENV,
    SYNC_PREFIXES,
    UnsafeDatabaseTarget,
    assert_writable,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("db", ["fintech_oltp", "fintech_analytics"])
def test_migrations_never_touch_protected_databases(db, monkeypatch):
    monkeypatch.setenv(PROTECTED_OVERRIDE_ENV, "1")  # the override must not apply to migrations
    with pytest.raises(UnsafeDatabaseTarget, match="protected"):
        assert_writable(db, allowed_prefixes=MIGRATION_PREFIXES, purpose="migration")


@pytest.mark.parametrize("db", ["demo_oltp", "postgres", "airflow", "v2", "V2_oltp", "xv2_oltp"])
def test_migrations_only_accept_v2_prefix(db):
    with pytest.raises(UnsafeDatabaseTarget):
        assert_writable(db, allowed_prefixes=MIGRATION_PREFIXES, purpose="migration")


@pytest.mark.parametrize("db", ["v2_oltp", "v2_analytics", "v2_test_x"])
def test_migrations_accept_v2_databases(db):
    assert_writable(db, allowed_prefixes=MIGRATION_PREFIXES, purpose="migration")


def test_sync_rejects_protected_target_without_override(monkeypatch):
    monkeypatch.delenv(PROTECTED_OVERRIDE_ENV, raising=False)
    with pytest.raises(UnsafeDatabaseTarget, match=PROTECTED_OVERRIDE_ENV):
        assert_writable("fintech_analytics", allowed_prefixes=SYNC_PREFIXES, purpose="sync",
                        allow_protected_override=True)


def test_sync_override_requires_exact_value(monkeypatch):
    monkeypatch.setenv(PROTECTED_OVERRIDE_ENV, "true")
    with pytest.raises(UnsafeDatabaseTarget):
        assert_writable("fintech_analytics", allowed_prefixes=SYNC_PREFIXES, purpose="sync",
                        allow_protected_override=True)
    monkeypatch.setenv(PROTECTED_OVERRIDE_ENV, "1")
    assert_writable("fintech_analytics", allowed_prefixes=SYNC_PREFIXES, purpose="sync",
                    allow_protected_override=True)


@pytest.mark.parametrize("db", ["demo_analytics", "v2_analytics"])
def test_sync_accepts_demo_and_v2_targets(db):
    assert_writable(db, allowed_prefixes=SYNC_PREFIXES, purpose="sync")


def test_sync_cli_refuses_protected_target_before_connecting():
    """The real CLI exits before opening any connection when pointed at the main database."""
    env = {k: v for k, v in os.environ.items() if k != PROTECTED_OVERRIDE_ENV}
    env.update(OLTP_DB="fintech_oltp", ANALYTICS_DB="fintech_analytics",
               POSTGRES_HOST="203.0.113.1")  # unroutable: a connection attempt would hang or fail differently
    result = subprocess.run([sys.executable, "-m", "sync.incremental_sync"], cwd=PROJECT_ROOT,
                            env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert "UnsafeDatabaseTarget" in result.stderr
    assert "refusing to write to protected database 'fintech_analytics'" in result.stderr
