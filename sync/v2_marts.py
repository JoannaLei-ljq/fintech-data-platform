"""Shared definitions for V2 mart validation and publication (used by v2_reconcile and v2_publish)."""

from __future__ import annotations

from dataclasses import dataclass

CANDIDATE_SCHEMA = "marts_candidate"
PUBLISHED_SCHEMA = "marts"
MART_TABLES = ("fct_payments", "fct_daily_payments", "fct_merchant_performance")
BUILD_INFO = "mart_build_info"


@dataclass(frozen=True)
class BuildInfo:
    pipeline_run_id: str
    dbt_invocation_id: str
    sync_run_id: int
    sync_watermark: str
    source_generation: str


def read_build_info(conn) -> BuildInfo | None:
    """The manifest dbt's on-run-start hook wrote for the candidate marts (None if absent)."""
    exists = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{CANDIDATE_SCHEMA}.{BUILD_INFO}",)).fetchone()[0]
    if not exists:
        return None
    rows = conn.execute(f"SELECT pipeline_run_id, dbt_invocation_id, sync_run_id, sync_watermark, "
                        f"source_generation::text FROM {CANDIDATE_SCHEMA}.{BUILD_INFO}").fetchall()
    return BuildInfo(*rows[0]) if len(rows) == 1 else None


def candidate_fingerprint(conn) -> str:
    """md5 over every row of the three candidate marts and the build manifest, order-independent.
    The session must use TimeZone UTC so timestamps render identically."""
    parts = []
    for table in (*MART_TABLES, BUILD_INFO):
        digest = conn.execute(
            f"SELECT md5(coalesce(string_agg(t::text, E'\\n' ORDER BY t::text), '')) FROM {CANDIDATE_SCHEMA}.{table} t"
        ).fetchone()[0]
        parts.append(f"{table}:{digest}")
    return conn.execute("SELECT md5(%s)", ("|".join(parts),)).fetchone()[0]
