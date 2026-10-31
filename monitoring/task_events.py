"""Record one Airflow task try in monitoring.task_runs (called from DAG callbacks).

    python -m monitoring.task_events --dag-id D --task-id T --run-id R --try-number 1 \
        --state success --started-at 2026-10-31T02:00:00+00:00 [--alert]

With --alert a failed task also becomes an immediate alert (email + alert_log).
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from monitoring.ops_store import connect, ensure_ops_tables


def record(dag_id: str, task_id: str, run_id: str, try_number: int, state: str,
           started_at: datetime | None, ended_at: datetime) -> float | None:
    duration = (ended_at - started_at).total_seconds() if started_at else None
    with connect() as connection, connection.cursor() as cursor:
        ensure_ops_tables(cursor)
        cursor.execute(
            """
            INSERT INTO monitoring.task_runs (
                dag_id, task_id, run_id, try_number, state, started_at, ended_at, duration_seconds
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dag_id, task_id, run_id, try_number) DO UPDATE
            SET state = EXCLUDED.state,
                started_at = EXCLUDED.started_at,
                ended_at = EXCLUDED.ended_at,
                duration_seconds = EXCLUDED.duration_seconds
            """,
            (dag_id, task_id, run_id, try_number, state, started_at, ended_at,
             round(duration, 1) if duration is not None else None),
        )
    return duration


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--try-number", type=int, required=True)
    parser.add_argument("--state", required=True, choices=("success", "failed", "up_for_retry"))
    parser.add_argument("--started-at", type=_timestamp)
    parser.add_argument("--ended-at", type=_timestamp, default=datetime.now(timezone.utc))
    parser.add_argument("--alert", action="store_true", help="also send a task_failed alert")
    args = parser.parse_args(argv)

    record(args.dag_id, args.task_id, args.run_id, args.try_number, args.state,
           args.started_at, args.ended_at)
    if args.alert and args.state == "failed":
        from monitoring.alerts import task_failed

        task_failed(args.dag_id, args.task_id, args.run_id, args.try_number)
    return 0


if __name__ == "__main__":
    sys.exit(main())
