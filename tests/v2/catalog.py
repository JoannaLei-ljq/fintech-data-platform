"""Catalog snapshot used to compare two database schemas structurally."""

from __future__ import annotations

import psycopg

USER_SCHEMAS = "n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')"

QUERIES = {
    "schemas": f"""
        SELECT n.nspname FROM pg_namespace n
        WHERE {USER_SCHEMAS} AND n.nspname NOT LIKE 'pg_temp%' AND n.nspname NOT LIKE 'pg_toast_temp%'
        ORDER BY 1""",
    "columns": """
        SELECT table_schema, table_name, column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND table_name <> 'schema_migrations'
        ORDER BY 1, 2, 3""",
    "constraints": f"""
        SELECT c.conrelid::regclass::text, c.conname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
        WHERE {USER_SCHEMAS} AND c.conrelid::regclass::text <> 'schema_migrations'
        ORDER BY 1, 2""",
    "indexes": """
        SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND tablename <> 'schema_migrations'
        ORDER BY 1, 2, 3""",
    "triggers": """
        SELECT event_object_schema, event_object_table, trigger_name, action_timing,
               event_manipulation, action_statement
        FROM information_schema.triggers ORDER BY 1, 2, 3, 5""",
    "functions": f"""
        SELECT p.proname, pg_get_functiondef(p.oid) FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE {USER_SCHEMAS} AND p.prokind = 'f' ORDER BY 1""",
}


def snapshot(conn: psycopg.Connection) -> dict[str, list[tuple]]:
    return {name: conn.execute(query).fetchall() for name, query in QUERIES.items()}
