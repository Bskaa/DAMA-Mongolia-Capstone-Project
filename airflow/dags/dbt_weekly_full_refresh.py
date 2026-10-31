"""Weekly `dbt build --full-refresh`: rebuild incremental facts from Silver.

fact_sessions and fact_purchases are incremental (delete+insert on their key with
a 3-day lookback). That is fast, but a row deleted upstream (Silver filters
Meltano soft-deleted rows) is never removed from an incremental table, and a
late update older than the lookback is never picked up. Once a week, while the
daily pipeline is idle, everything is rebuilt from scratch so Gold converges to
Silver again. Snapshots are not affected by --full-refresh, so SCD2 history is kept.

Created paused (see is_paused_upon_creation); unpause after the first successful
daily run. Sunday 04:00 Asia/Ulaanbaatar (the daily pipeline runs at 02:00 and usually
finishes well before). Results are stored like the daily run's, so the alert
rules see a failing weekly rebuild too.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.task.trigger_rule import TriggerRule

from pipeline_ops import TASK_CALLBACKS, run_dbt, run_module

TARGET_PATH = "/tmp/dbt-target-weekly"


def full_refresh() -> None:
    run_dbt("build", "--full-refresh", target_path=TARGET_PATH)


def collect_results() -> None:
    result = run_module(
        "monitoring.collect_dbt_results", f"{TARGET_PATH}/run_results.json", check=False, capture=True
    )
    print(result.stdout, result.stderr)
    if result.returncode:
        raise AirflowSkipException("No dbt results from this run to store.")


with DAG(
    dag_id="dbt_weekly_full_refresh",
    description="Weekly full rebuild of incremental Gold facts.",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Asia/Ulaanbaatar")),
    schedule="0 4 * * 0",
    catchup=False,
    max_active_runs=1,
    # Created paused: on a fresh Airflow it would otherwise run immediately for the last
    # Sunday, at the same time as the first daily pipeline. Unpause once that succeeded.
    is_paused_upon_creation=True,
    default_args={
        "owner": "gaming-analytics",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=3),
        **TASK_CALLBACKS,
    },
    tags=["gaming-analytics", "dbt"],
    doc_md=__doc__,
):
    rebuild = PythonOperator(task_id="dbt_build_full_refresh", python_callable=full_refresh)
    store = PythonOperator(
        task_id="collect_dbt_results",
        python_callable=collect_results,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )
    rebuild >> store
