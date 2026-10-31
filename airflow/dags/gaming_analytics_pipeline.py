"""Daily gaming analytics pipeline: Bronze ingestion, dbt build, quality checks, alerts.

    check_databases ─┬─> sync_source_to_bronze ─┬─> load_business_workbooks ─┐
                     │                          └─> reconcile_source_bronze ─┼──────────┐
                     └─> ingest_rawg (skipped if unavailable) ───────────────┴─> dbt_build
    dbt_build ──> collect_dbt_results ──> monitor_data_quality ──> check_and_alert <──┘

* Every task records its tries in monitoring.task_runs, and a task that fails
  (after retries) is emailed immediately (pipeline_ops.TASK_CALLBACKS).
* reconcile_source_bronze compares source and Bronze row counts (completeness).
* collect_dbt_results stores every dbt node result, so test results have a history.
* check_and_alert evaluates monitoring/checks.sql: new problems are emailed once
  per day, "all clear" is a short ✅, and a critical problem turns the task red.
"""

from __future__ import annotations

import json
import socket
import subprocess
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.task.trigger_rule import TriggerRule

from pipeline_ops import (
    ANALYTICS_HOST,
    MELTANO_ROOT,
    SOURCE_HOST,
    TASK_CALLBACKS,
    VENV,
    project_environment,
    run_dbt,
    run_module,
)

DBT_TARGET_PATH = "/tmp/dbt-target"


def wait_for_databases() -> None:
    for host, port in ((SOURCE_HOST, 5432), (ANALYTICS_HOST, 5432)):
        with socket.create_connection((host, port), timeout=10):
            pass


def run_meltano() -> None:
    subprocess.run(
        [str(VENV / "bin" / "meltano"), "run", "tap-postgres", "target-postgres"],
        cwd=MELTANO_ROOT,
        env=project_environment(),
        check=True,
    )


def run_rawg() -> None:
    """Refresh the external RAWG catalogue.

    RAWG only feeds the genre benchmark, so an unavailable API (or a missing
    key) must not stop the core pipeline: the task is marked *skipped* and
    dbt rebuilds the benchmark from the previously loaded RAWG data.
    """
    environment = project_environment()
    if not environment.get("RAWG_API_KEY", "").strip():
        raise AirflowSkipException(
            "RAWG_API_KEY is not set; keeping the previously loaded RAWG catalogue."
        )
    try:
        run_module("ingestion.api.rawg", "--max-pages", environment.get("RAWG_MAX_PAGES", "5"))
    except subprocess.CalledProcessError as error:
        raise AirflowSkipException(
            f"RAWG ingestion failed (exit code {error.returncode}); "
            "keeping the previously loaded RAWG catalogue."
        ) from error


def load_business_workbooks() -> None:
    run_module("ingestion.excel.load_excel")


def build_dbt() -> None:
    """Seeds, snapshots, models and tests in one DAG-ordered run.

    `dbt build` tests each model right after building it and skips everything
    downstream of a failing test, so bad data never reaches Gold.
    """
    run_dbt("build", target_path=DBT_TARGET_PATH)


def reconcile_source_bronze() -> None:
    """Source vs Bronze row counts; mismatches become alerts in check_and_alert."""
    result = run_module("monitoring.reconcile", check=False, capture=True)
    print(result.stdout, result.stderr)
    if result.returncode:
        raise AirflowException(f"Reconciliation crashed (exit code {result.returncode}).")


def collect_dbt_results() -> None:
    """Store this run's dbt results (also after a failed build, which is when they matter)."""
    result = run_module(
        "monitoring.collect_dbt_results", f"{DBT_TARGET_PATH}/run_results.json",
        check=False, capture=True,
    )
    print(result.stdout, result.stderr)
    if result.returncode:
        raise AirflowSkipException("No dbt results from this run to store.")


def monitor_data_quality(**context: Any) -> dict[str, Any]:
    result = run_module("monitoring.data_quality", check=False, capture=True)
    if result.stderr:
        print(result.stderr)
    try:
        summary = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(result.stdout)
        raise AirflowException("Data quality monitor did not return a valid JSON summary") from exc
    compact = {key: value for key, value in summary.items() if key != "results"}
    print(json.dumps(compact, indent=2))
    context["ti"].xcom_push(key="data_quality_summary", value=compact)
    if result.returncode or summary["status"] == "fail":
        raise AirflowException(
            f"Data quality checks failed: {summary['failed_count']} relation(s) failed"
        )
    return compact


def check_and_alert() -> None:
    """Run the alert rules; critical problems turn this task red after the email is sent."""
    result = run_module("monitoring.alerts", "--source", "pipeline", check=False, capture=True)
    print(result.stdout, result.stderr)
    if result.returncode == 1:
        raise AirflowException("Critical problems found: see the alert email / monitoring.alert_log.")
    if result.returncode:
        raise AirflowException(f"monitoring.alerts crashed (exit code {result.returncode}).")


with DAG(
    dag_id="gaming_analytics_pipeline",
    description="Daily Bronze ingestion, dbt Silver/Gold builds, quality checks and alerts.",
    start_date=datetime(2025, 1, 1, tzinfo=ZoneInfo("Asia/Ulaanbaatar")),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "gaming-analytics",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
        **TASK_CALLBACKS,
    },
    tags=["gaming-analytics", "capstone"],
    doc_md=__doc__,
) as dag:
    check_databases = PythonOperator(task_id="check_databases", python_callable=wait_for_databases)
    sync_source = PythonOperator(task_id="sync_source_to_bronze", python_callable=run_meltano)
    ingest_rawg = PythonOperator(task_id="ingest_rawg", python_callable=run_rawg)
    load_excel = PythonOperator(task_id="load_business_workbooks", python_callable=load_business_workbooks)
    build_dbt_models = PythonOperator(
        task_id="dbt_build",
        python_callable=build_dbt,
        # Runs when the core loads succeeded; a skipped (unavailable) RAWG
        # refresh is fine because dbt reuses the previous RAWG data.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    reconcile = PythonOperator(task_id="reconcile_source_bronze", python_callable=reconcile_source_bronze)
    store_dbt_results = PythonOperator(
        task_id="collect_dbt_results",
        python_callable=collect_dbt_results,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )
    monitor_quality = PythonOperator(
        task_id="monitor_data_quality",
        python_callable=monitor_data_quality,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    alert = PythonOperator(
        task_id="check_and_alert",
        python_callable=check_and_alert,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    # Source replication and the RAWG refresh are independent and run in
    # parallel; the Excel loader validates game IDs against bronze.games, so it
    # waits for Meltano.
    check_databases >> [sync_source, ingest_rawg]
    sync_source >> [load_excel, reconcile]
    [load_excel, ingest_rawg] >> build_dbt_models
    build_dbt_models >> store_dbt_results >> monitor_quality >> alert  # measure, then judge
    reconcile >> alert
