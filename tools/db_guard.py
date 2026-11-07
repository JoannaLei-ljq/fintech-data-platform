"""Database-name safety guards.

Every code path that writes to a database calls assert_writable() before connecting
and assert_connected_to() right after, so a wrong environment variable cannot point
a write at the original databases.
"""

from __future__ import annotations

import os

import psycopg

# The original step-1 databases. V2 work and tests must never write to them.
PROTECTED_DATABASES = frozenset({"fintech_oltp", "fintech_analytics"})

# Only the MVP sync may write to a protected database, and only with this set to "1".
PROTECTED_OVERRIDE_ENV = "FINTECH_ALLOW_PROTECTED_DB_WRITES"

SYNC_PREFIXES = ("demo_", "v2_")
MIGRATION_PREFIXES = ("v2_",)


class UnsafeDatabaseTarget(RuntimeError):
    """Raised when a write would target a database outside the allowed set."""


def assert_writable(dbname: str, *, allowed_prefixes: tuple[str, ...], purpose: str,
                    allow_protected_override: bool = False) -> None:
    if dbname in PROTECTED_DATABASES:
        if allow_protected_override and os.getenv(PROTECTED_OVERRIDE_ENV) == "1":
            return
        hint = f" Set {PROTECTED_OVERRIDE_ENV}=1 to allow it explicitly." if allow_protected_override else ""
        raise UnsafeDatabaseTarget(f"{purpose}: refusing to write to protected database {dbname!r}.{hint}")
    if not dbname.startswith(allowed_prefixes):
        raise UnsafeDatabaseTarget(
            f"{purpose}: database {dbname!r} does not start with any of {allowed_prefixes}"
        )


def assert_connected_to(conn: psycopg.Connection, expected: str) -> None:
    """Confirm the server-side database, not just the name we asked for."""
    actual = conn.execute("SELECT current_database()").fetchone()[0]
    if actual != expected:
        raise UnsafeDatabaseTarget(f"connected to {actual!r}, expected {expected!r}")
