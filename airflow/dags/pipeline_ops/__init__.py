"""Helpers shared by the project's DAGs (ignored by the DAG parser via .airflowignore).

* project_environment(): container-internal connection settings for project code
* run_module():          run `python -m <module>` from the project virtualenv
* run_dbt():             run dbt with a caller-specific target path
* TASK_CALLBACKS:        on_success / on_retry / on_failure callbacks for default_args.
  Every task try is recorded in monitoring.task_runs (durations -> slow_task rule) and
  a failed task is alerted immediately (email + monitoring.alert_log).
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

PROJECT_ROOT = Path("/opt/project")
VENV = Path("/opt/project-venv")
MELTANO_ROOT = Path("/opt/meltano")
SOURCE_HOST = "postgres-source"
ANALYTICS_HOST = "postgres-analytics"


def project_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(PROJECT_ROOT)
    environment["ANALYTICS_HOST"] = ANALYTICS_HOST
    environment["ANALYTICS_PORT"] = "5432"
    environment["DBT_HOST"] = ANALYTICS_HOST
    environment["MELTANO_PROJECT_ROOT"] = str(MELTANO_ROOT)
    environment["POSTGRES_PORT"] = "5432"
    environment["TAP_POSTGRES_HOST"] = SOURCE_HOST
    environment["TAP_POSTGRES_PORT"] = "5432"
    environment["TAP_POSTGRES_DATABASE"] = environment.get("POSTGRES_DB", "gaming_source")
    environment["TARGET_POSTGRES_HOST"] = ANALYTICS_HOST
    environment["TARGET_POSTGRES_PORT"] = "5432"
    environment["TARGET_POSTGRES_USER"] = environment["POSTGRES_USER"]
    environment["TARGET_POSTGRES_PASSWORD"] = environment["POSTGRES_PASSWORD"]
    environment["TARGET_POSTGRES_DATABASE"] = environment.get("ANALYTICS_DB", "gaming_analytics")
    environment["TARGET_POSTGRES_DEFAULT_TARGET_SCHEMA"] = "bronze"
    environment["TARGET_POSTGRES_LOAD_METHOD"] = "upsert"

    user = quote(environment["POSTGRES_USER"], safe="")
    password = quote(environment["POSTGRES_PASSWORD"], safe="")
    source_db = quote(environment["TAP_POSTGRES_DATABASE"], safe="")
    source_url = f"postgresql://{user}:{password}@{SOURCE_HOST}:5432/{source_db}"
    environment["TAP_POSTGRES_SQLALCHEMY_URL"] = source_url
    environment["DATABASE_URL"] = source_url
    return environment


def run_module(module: str, *args: str, timeout: float | None = None,
               check: bool = True, capture: bool = False) -> subprocess.CompletedProcess[str]:
    """Run `python -m module args...` with the project's virtualenv and environment."""
    return subprocess.run(
        [str(VENV / "bin" / "python"), "-m", module, *args],
        cwd=PROJECT_ROOT,
        env=project_environment(),
        check=check,
        timeout=timeout,
        capture_output=capture,
        text=True,
    )


def run_dbt(*args: str, target_path: str) -> None:
    """Run dbt from the project virtualenv; each caller gets its own target path, so
    run_results.json of one DAG never overwrites another's."""
    subprocess.run(
        [
            str(VENV / "bin" / "dbt"),
            *args,
            "--project-dir", str(PROJECT_ROOT / "dbt"),
            "--profiles-dir", str(PROJECT_ROOT / "dbt"),
            "--target-path", target_path,
            "--log-path", f"{target_path}-logs",
        ],
        cwd=PROJECT_ROOT,
        env=project_environment(),
        check=True,
    )


def _record(context: dict[str, Any], state: str, alert: bool = False) -> None:
    """Never raises: a broken monitoring call must not hide the task's real result."""
    try:
        ti = context.get("ti") or context["task_instance"]
        started = getattr(ti, "start_date", None)
        args = [
            "--dag-id", ti.dag_id,
            "--task-id", ti.task_id,
            "--run-id", str(context.get("run_id") or ti.run_id),
            "--try-number", str(ti.try_number),
            "--state", state,
            "--ended-at", datetime.now(timezone.utc).isoformat(),
        ]
        if started is not None:
            args += ["--started-at", started.isoformat()]
        if alert:
            args.append("--alert")
        result = run_module("monitoring.task_events", *args, timeout=90, check=False, capture=True)
        if result.returncode:
            print(f"monitoring.task_events failed: {result.stderr or result.stdout}")
    except Exception as error:  # noqa: BLE001
        print(f"could not record task {state}: {error}")


def on_success(context: dict[str, Any]) -> None:
    _record(context, "success")


def on_retry(context: dict[str, Any]) -> None:
    _record(context, "up_for_retry")


def on_failure(context: dict[str, Any]) -> None:
    _record(context, "failed", alert=True)


TASK_CALLBACKS = {
    "on_success_callback": on_success,
    "on_retry_callback": on_retry,
    "on_failure_callback": on_failure,
}
