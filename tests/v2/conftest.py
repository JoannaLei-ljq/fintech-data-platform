"""Shared fixtures: throwaway v2_test_* databases on the local Postgres (127.0.0.1:5434).

Every database created here starts with ``v2_test_`` and is dropped after the test,
so tests never touch fintech_*, demo_* or the v2_oltp / v2_analytics dev databases.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

from sync.incremental_sync import conninfo  # noqa: E402  (needs .env loaded first)

TEST_DB_PREFIX = "v2_test_"
OLTP_MIGRATIONS = PROJECT_ROOT / "migrations" / "oltp"
ANALYTICS_MIGRATIONS = PROJECT_ROOT / "migrations" / "analytics"
MVP_DEMO_SEED = PROJECT_ROOT / "sql" / "demo" / "seed_demo.sql"


def _admin() -> psycopg.Connection:
    return psycopg.connect(conninfo("postgres"), autocommit=True)


@pytest.fixture
def make_db() -> Iterator[Callable[[str], str]]:
    """Factory: make_db("label") creates an empty v2_test_<label>_<rand> database."""
    created: list[str] = []

    def _make(label: str) -> str:
        name = f"{TEST_DB_PREFIX}{label}_{uuid.uuid4().hex[:6]}"
        with _admin() as admin:
            admin.execute(f'CREATE DATABASE "{name}"')
        created.append(name)
        return name

    yield _make
    with _admin() as admin:
        for name in created:
            assert name.startswith(TEST_DB_PREFIX)  # never drop anything else
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def connect(dbname: str) -> psycopg.Connection:
    return psycopg.connect(conninfo(dbname), autocommit=True)


def run_sql_file(dbname: str, path: Path) -> None:
    with connect(dbname) as conn:
        conn.execute(path.read_text())


def quiet(_: str) -> None:
    """Log sink for migrate() in tests."""
