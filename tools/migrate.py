"""Versioned SQL migrations with checksums for the V2 databases.

Files live in migrations/<target>/V<NNN>__<name>.sql, numbered contiguously from 001.
Each pending file runs in its own transaction together with its schema_migrations row,
so a failed migration leaves no trace. Every run re-verifies the SHA-256 checksum of
each applied file, so editing an applied migration is detected instead of ignored.

    python -m tools.migrate --target oltp                 # apply all pending to v2_oltp
    python -m tools.migrate --target oltp --to 1          # stop after V001
    python -m tools.migrate --target analytics --status   # show applied / pending
"""

from __future__ import annotations

import argparse
import hashlib
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from sync.incremental_sync import conninfo
from tools.db_guard import MIGRATION_PREFIXES, assert_connected_to, assert_writable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS_ROOT = PROJECT_ROOT / "migrations"
DEFAULT_DATABASES = {"oltp": "v2_oltp", "analytics": "v2_analytics"}
FILENAME = re.compile(r"^V(\d{3})__([a-z0-9_]+)\.sql$")
ADVISORY_LOCK_KEY = 48210001  # serialises concurrent migration runs on one database

SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version      integer     PRIMARY KEY,
    name         text        NOT NULL,
    checksum     char(64)    NOT NULL,
    applied_at   timestamptz NOT NULL DEFAULT now(),
    execution_ms integer     NOT NULL
)
"""


class MigrationError(RuntimeError):
    """The migration set or the database's migration history is inconsistent."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path
    checksum: str

    @property
    def filename(self) -> str:
        return self.path.name


def discover(directory: Path) -> list[Migration]:
    migrations = []
    for path in sorted(directory.glob("*.sql")):
        match = FILENAME.match(path.name)
        if not match:
            raise MigrationError(f"unexpected migration file name: {path.name}")
        migrations.append(Migration(int(match[1]), match[2], path,
                                    hashlib.sha256(path.read_bytes()).hexdigest()))
    versions = [m.version for m in migrations]
    if versions != list(range(1, len(versions) + 1)):
        raise MigrationError(f"migration versions must be contiguous from 1, found {versions}")
    return migrations


def _applied(conn: psycopg.Connection) -> dict[int, str]:
    return {v: c for v, c in conn.execute("SELECT version, checksum FROM schema_migrations")}


def _verify_history(applied: dict[int, str], migrations: list[Migration]) -> None:
    by_version = {m.version: m for m in migrations}
    for version, checksum in sorted(applied.items()):
        migration = by_version.get(version)
        if migration is None:
            raise MigrationError(f"V{version:03d} is recorded as applied but its file is missing")
        if migration.checksum != checksum:
            raise MigrationError(f"checksum mismatch for {migration.filename}: "
                                 "the file changed after it was applied")
    if applied and sorted(applied) != list(range(1, max(applied) + 1)):
        raise MigrationError(f"applied versions are not contiguous: {sorted(applied)}")


def migrate(dbname: str, directory: Path, target_version: int | None = None,
            log: Callable[[str], None] = print) -> list[int]:
    """Apply pending migrations up to target_version. Returns the versions applied."""
    assert_writable(dbname, allowed_prefixes=MIGRATION_PREFIXES, purpose="migration")
    migrations = discover(directory)
    with psycopg.connect(conninfo(dbname), autocommit=True) as conn:
        assert_connected_to(conn, dbname)
        conn.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        try:
            conn.execute(SCHEMA_MIGRATIONS_DDL)
            applied = _applied(conn)
            _verify_history(applied, migrations)
            pending = [m for m in migrations if m.version not in applied
                       and (target_version is None or m.version <= target_version)]
            for migration in pending:
                started = time.monotonic()
                with conn.transaction():
                    conn.execute(migration.path.read_text())
                    conn.execute(
                        "INSERT INTO schema_migrations (version, name, checksum, execution_ms) "
                        "VALUES (%s, %s, %s, %s)",
                        (migration.version, migration.name, migration.checksum,
                         int((time.monotonic() - started) * 1000)),
                    )
                log(f"{dbname}: applied {migration.filename}")
            if not pending:
                log(f"{dbname}: up to date (V{max(applied, default=0):03d})")
            return [m.version for m in pending]
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))


def status(dbname: str, directory: Path) -> list[tuple[str, str]]:
    """Read-only: (filename, 'applied' | 'pending') for every migration file."""
    migrations = discover(directory)
    with psycopg.connect(conninfo(dbname), autocommit=True) as conn:
        assert_connected_to(conn, dbname)
        exists = conn.execute("SELECT to_regclass('public.schema_migrations') IS NOT NULL").fetchone()[0]
        applied = _applied(conn) if exists else {}
    _verify_history(applied, migrations)
    return [(m.filename, "applied" if m.version in applied else "pending") for m in migrations]


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    parser = argparse.ArgumentParser(description="Apply versioned SQL migrations to a V2 database")
    parser.add_argument("--target", choices=sorted(DEFAULT_DATABASES), required=True)
    parser.add_argument("--db", help="database name (default: v2_<target>)")
    parser.add_argument("--to", type=int, dest="target_version", help="stop after this version")
    parser.add_argument("--status", action="store_true", help="show migration status and exit")
    args = parser.parse_args()

    dbname = args.db or DEFAULT_DATABASES[args.target]
    directory = MIGRATIONS_ROOT / args.target
    if args.status:
        for filename, state in status(dbname, directory):
            print(f"{state:8} {filename}")
        return
    migrate(dbname, directory, args.target_version)


if __name__ == "__main__":
    main()
