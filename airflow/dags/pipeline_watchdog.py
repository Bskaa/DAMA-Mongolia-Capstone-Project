"""Who watches the watcher? Two small DAGs.

1. pipeline_watchdog   10:00 Asia/Ulaanbaatar, on its OWN schedule.
   If gaming_analytics_pipeline dies at 02:00 on its first task, its own
   check_and_alert may never run, so that DAG cannot tell you. The watchdog does
   not depend on it: it runs the same monitoring/checks.sql eight hours later.
   Rule `pipeline_silent` sees "no quality check today". Problems already
   emailed today are not sent again, and "all fine" stays quiet (the pipeline
   already sent its morning ✅).

2. alert_drill         never scheduled. Trigger it by hand: one task that fails
   on purpose. An alert you have never seen arrive is a rumour; prove the chain
   (callback -> email -> monitoring.alert_log) works before a real failure does.

Honest limit: both DAGs run inside the same Airflow. If the scheduler itself is
down, nothing here runs either; the real answer to "who watches the watcher" is
something outside this box (a cron job, another machine, a heartbeat service).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from pipeline_ops import TASK_CALLBACKS, run_module

LOCAL_TZ = ZoneInfo("Asia/Ulaanbaatar")


def watchdog() -> None:
    result = run_module(
        "monitoring.alerts", "--quiet-if-ok", "--source", "watchdog", check=False, capture=True
    )
    print(result.stdout, result.stderr)
    if result.returncode == 1:
        raise AirflowException("Critical problems found by the watchdog (alert sent).")
    if result.returncode:
        raise AirflowException(f"monitoring.alerts crashed (exit code {result.returncode}).")


def fail_on_purpose() -> None:
    raise AirflowException("Drill: if an alert email arrives, the alert chain works.")


with DAG(
    dag_id="pipeline_watchdog",
    description="Runs the alert rules on its own clock, independent of the pipeline.",
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TZ),
    schedule="0 10 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "gaming-analytics", "retries": 1, "retry_delay": timedelta(minutes=5),
                  **TASK_CALLBACKS},
    tags=["gaming-analytics", "monitoring"],
    doc_md=__doc__,
):
    PythonOperator(task_id="watchdog", python_callable=watchdog)


with DAG(
    dag_id="alert_drill",
    description="Fails on purpose: you should receive a task_failed email.",
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TZ),
    schedule=None,
    catchup=False,
    default_args={"owner": "gaming-analytics", "retries": 0, **TASK_CALLBACKS},
    tags=["gaming-analytics", "monitoring"],
    doc_md=__doc__,
):
    PythonOperator(task_id="this_task_fails_on_purpose", python_callable=fail_on_purpose)
