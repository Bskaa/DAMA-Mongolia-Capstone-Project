"""Reconcile the operational source with Bronze (DAMA: completeness across systems).

Every other check looks inside one system. This one compares two. For each
replicated table, with `bound` = the newest `updated_at` Bronze has seen:

    lower = source rows with updated_at <= bound   (must all be in Bronze)
    upper = all source rows                        (Bronze cannot have more)

Rows created after Meltano's last sync (e.g. by the hourly generator) are not
"missing", and rows updated after the sync are still counted once, so a healthy
Bronze always satisfies lower <= bronze_rows <= upper. Event tables are
append-only at the source (an event is never edited), so for them upper = lower:
any extra Bronze row means the source deleted it.

    status match              lower <= bronze_rows <= upper
    status missing_in_bronze  bronze_rows < lower: the incremental sync lost rows
    status extra_in_bronze    bronze_rows > upper: rows were hard-deleted at the source
    status bronze_empty       the source has rows but Bronze has none (sync never ran)

Results go to monitoring.reconciliation; checks.sql turns problems into alerts.

    python -m monitoring.reconcile
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg import sql

from monitoring.ops_store import connect, ensure_ops_tables

TABLES = (
    "players", "platforms", "games", "game_platforms",
    "sessions", "purchases", "achievements", "reviews",
)
APPEND_ONLY = {"sessions", "purchases", "achievements", "reviews"}


def classify(lower: int, upper: int, bronze_rows: int) -> str:
    if upper and not bronze_rows:
        return "bronze_empty"
    if bronze_rows < lower:
        return "missing_in_bronze"
    if bronze_rows > upper:
        return "extra_in_bronze"
    return "match"


def _bronze_updated_at(cursor: Any, table: str) -> sql.Composable:
    """`updated_at` as timestamptz; Meltano may land it without a time zone (UTC)."""
    cursor.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = 'bronze' AND table_name = %s AND column_name = 'updated_at'",
        (table,),
    )
    row = cursor.fetchone()
    if row is None:
        raise LookupError(table)
    if row[0] == "timestamp without time zone":
        return sql.SQL("(updated_at AT TIME ZONE 'UTC')")
    return sql.SQL("updated_at")


def _has_column(cursor: Any, table: str, column: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = 'bronze' AND table_name = %s AND column_name = %s",
        (table, column),
    )
    return cursor.fetchone() is not None


def reconcile() -> list[dict[str, Any]]:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL (source database) is not set.")
    checked_at = datetime.now(timezone.utc)
    results = []
    with psycopg.connect(database_url) as source, connect() as analytics:
        with source.cursor() as source_cursor, analytics.cursor() as bronze_cursor:
            ensure_ops_tables(bronze_cursor)
            for table in TABLES:
                source_table = sql.Identifier("public", table)
                source_cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(source_table))
                upper = int(source_cursor.fetchone()[0])
                bound, lower, bronze_rows = None, upper, 0
                try:
                    updated_at = _bronze_updated_at(bronze_cursor, table)
                except LookupError:  # Bronze table does not exist yet
                    updated_at = None
                if updated_at is not None:
                    identifier = sql.Identifier("bronze", table)
                    live = (sql.SQL("_sdc_deleted_at IS NULL")
                            if _has_column(bronze_cursor, table, "_sdc_deleted_at") else sql.SQL("TRUE"))
                    bronze_cursor.execute(
                        sql.SQL("SELECT count(*), max({}) FROM {} WHERE {}").format(updated_at, identifier, live)
                    )
                    bronze_rows, bound = bronze_cursor.fetchone()
                    bronze_rows = int(bronze_rows)
                    if bound is not None:
                        source_cursor.execute(
                            sql.SQL("SELECT count(*) FROM {} WHERE updated_at <= %s").format(source_table),
                            (bound,),
                        )
                        lower = int(source_cursor.fetchone()[0])
                        if table in APPEND_ONLY:
                            upper = lower
                results.append({
                    "table": table, "compared_up_to": bound, "source_rows": lower,
                    "source_rows_total": upper, "bronze_rows": bronze_rows,
                    "status": classify(lower, upper, bronze_rows),
                })
            bronze_cursor.executemany(
                """
                INSERT INTO monitoring.reconciliation (
                    checked_at, table_name, compared_up_to, source_rows, bronze_rows, difference, status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [(checked_at, r["table"], r["compared_up_to"], r["source_rows"], r["bronze_rows"],
                  r["bronze_rows"] - r["source_rows"], r["status"]) for r in results],
            )
    return results


def main() -> int:
    results = reconcile()
    print(json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
