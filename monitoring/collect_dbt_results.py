"""Store dbt's run_results.json in monitoring.dbt_node_results.

dbt overwrites run_results.json on every invocation, so yesterday's
"WARN 71" is gone by today. Storing every node of every invocation is what makes
"the warning grew from 71 to 140" detectable (see checks.sql).

    python -m monitoring.collect_dbt_results /tmp/dbt-target/run_results.json

Idempotent: re-loading the same invocation replaces its rows.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from monitoring.ops_store import connect, ensure_ops_tables

STORED_TYPES = {"model", "test", "seed", "snapshot", "unit_test"}


def parse_run_results(data: dict[str, Any]) -> tuple[str, str, str, list[tuple[Any, ...]]]:
    metadata = data.get("metadata") or {}
    invocation_id = metadata.get("invocation_id")
    generated_at = metadata.get("generated_at")
    if not invocation_id or not generated_at:
        raise ValueError("run_results.json has no metadata.invocation_id / generated_at")
    command = " ".join((data.get("args") or {}).get("invocation_command", "").split()[:3]) or (
        (data.get("args") or {}).get("which") or ""
    )
    rows = []
    for result in data.get("results") or []:
        unique_id = result.get("unique_id", "")
        resource_type = unique_id.split(".", 1)[0]
        if resource_type not in STORED_TYPES:
            continue
        failures = result.get("failures")
        rows.append(
            (
                invocation_id,
                generated_at,
                command,
                resource_type,
                unique_id.split(".")[-1] if resource_type != "test" else unique_id.split(".")[2],
                unique_id,
                str(result.get("status") or "unknown"),
                round(float(result.get("execution_time") or 0), 3),
                int(failures) if failures is not None else None,
                (result.get("message") or "")[:500],
            )
        )
    return invocation_id, generated_at, command, rows


def store(rows: list[tuple[Any, ...]], invocation_id: str) -> None:
    with connect() as connection, connection.cursor() as cursor:
        ensure_ops_tables(cursor)
        cursor.execute(
            "DELETE FROM monitoring.dbt_node_results WHERE invocation_id = %s", (invocation_id,)
        )
        cursor.executemany(
            """
            INSERT INTO monitoring.dbt_node_results (
                invocation_id, generated_at, command, resource_type, node_name,
                unique_id, status, execution_seconds, failures, message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("run_results", type=Path, help="Path to dbt's run_results.json")
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=6,
        help="Ignore a file older than this: it belongs to an earlier dbt run (default 6).",
    )
    args = parser.parse_args(argv)
    if not args.run_results.exists():
        print(f"{args.run_results} does not exist: dbt did not start, nothing to store.")
        return 1
    invocation_id, generated_at, _, rows = parse_run_results(
        json.loads(args.run_results.read_text(encoding="utf-8"))
    )
    written = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    if written.tzinfo is None:
        written = written.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) - written > timedelta(hours=args.max_age_hours):
        print(f"{args.run_results} was written at {generated_at}, before this run: not stored.")
        return 1
    store(rows, invocation_id)
    by_status: dict[str, int] = {}
    for row in rows:
        by_status[row[6]] = by_status.get(row[6], 0) + 1
    print(json.dumps({"invocation_id": invocation_id, "generated_at": generated_at,
                      "nodes": len(rows), "by_status": by_status}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
