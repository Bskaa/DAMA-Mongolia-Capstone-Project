"""Run the gaming analytics ingestion and dbt pipeline."""

from __future__ import annotations

import json
import os
import socket
import subprocess
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.task.trigger_rule import TriggerRule
from airflow.utils.email import send_email

PROJECT_ROOT = Path("/opt/project")
VENV = Path("/opt/project-venv")
MELTANO_ROOT = Path("/opt/meltano")
SOURCE_HOST = "postgres-source"
ANALYTICS_HOST = "postgres-analytics"


def _project_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(PROJECT_ROOT)
    environment["ANALYTICS_HOST"] = ANALYTICS_HOST
    environment["ANALYTICS_PORT"] = "5432"
    environment["DBT_HOST"] = ANALYTICS_HOST
    environment["MELTANO_PROJECT_ROOT"] = str(MELTANO_ROOT)
    environment["POSTGRES_PORT"] = "5432"
    environment["TAP_POSTGRES_HOST"] = SOURCE_HOST
    environment["TAP_POSTGRES_PORT"] = "5432"
    environment["TAP_POSTGRES_DATABASE"] = environment.get(
        "POSTGRES_DB", "gaming_source"
    )
    environment["TARGET_POSTGRES_HOST"] = ANALYTICS_HOST
    environment["TARGET_POSTGRES_PORT"] = "5432"
    environment["TARGET_POSTGRES_USER"] = environment["POSTGRES_USER"]
    environment["TARGET_POSTGRES_PASSWORD"] = environment["POSTGRES_PASSWORD"]
    environment["TARGET_POSTGRES_DATABASE"] = environment.get(
        "ANALYTICS_DB", "gaming_analytics"
    )
    environment["TARGET_POSTGRES_DEFAULT_TARGET_SCHEMA"] = "bronze"
    environment["TARGET_POSTGRES_LOAD_METHOD"] = "upsert"

    user = quote(environment["POSTGRES_USER"], safe="")
    password = quote(environment["POSTGRES_PASSWORD"], safe="")
    source_db = quote(environment["TAP_POSTGRES_DATABASE"], safe="")
    environment["TAP_POSTGRES_SQLALCHEMY_URL"] = (
        f"postgresql://{user}:{password}@{SOURCE_HOST}:5432/{source_db}"
    )
    environment["DATABASE_URL"] = (
        f"postgresql://{user}:{password}@{SOURCE_HOST}:5432/{source_db}"
    )
    return environment


def _run(command: list[str], cwd: Path, environment: dict[str, str]) -> None:
    subprocess.run(command, cwd=cwd, env=environment, check=True)


def wait_for_databases() -> None:
    for host, port in ((SOURCE_HOST, 5432), (ANALYTICS_HOST, 5432)):
        with socket.create_connection((host, port), timeout=10):
            pass


def run_meltano() -> None:
    _run(
        [str(VENV / "bin" / "meltano"), "run", "tap-postgres", "target-postgres"],
        MELTANO_ROOT,
        _project_environment(),
    )


def run_rawg() -> None:
    environment = _project_environment()
    max_pages = environment.get("RAWG_MAX_PAGES", "5")
    _run(
        [
            str(VENV / "bin" / "python"),
            "-m",
            "ingestion.api.rawg",
            "--max-pages",
            max_pages,
        ],
        PROJECT_ROOT,
        environment,
    )


def load_business_workbooks() -> None:
    _run(
        [str(VENV / "bin" / "python"), "-m", "ingestion.excel.load_excel"],
        PROJECT_ROOT,
        _project_environment(),
    )


def run_dbt() -> None:
    _run(
        [
            str(VENV / "bin" / "dbt"),
            "run",
            "--project-dir",
            str(PROJECT_ROOT / "dbt"),
            "--profiles-dir",
            str(PROJECT_ROOT / "dbt"),
            "--target-path",
            "/tmp/dbt-target",
            "--log-path",
            "/tmp/dbt-logs",
        ],
        PROJECT_ROOT,
        _project_environment(),
    )


def test_dbt() -> None:
    _run(
        [
            str(VENV / "bin" / "dbt"),
            "test",
            "--project-dir",
            str(PROJECT_ROOT / "dbt"),
            "--profiles-dir",
            str(PROJECT_ROOT / "dbt"),
            "--target-path",
            "/tmp/dbt-target",
            "--log-path",
            "/tmp/dbt-logs",
        ],
        PROJECT_ROOT,
        _project_environment(),
    )


def monitor_data_quality(**context: Any) -> dict[str, Any]:
    task_instance = context["ti"]
    result = subprocess.run(
        [str(VENV / "bin" / "python"), "-m", "monitoring.data_quality"],
        cwd=PROJECT_ROOT,
        env=_project_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    try:
        summary = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise AirflowException(
            "Data quality monitor did not return a valid JSON summary"
        ) from exc
    task_instance.xcom_push(key="data_quality_summary", value=summary)
    if result.returncode or summary["status"] == "fail":
        raise AirflowException(
            f"Data quality checks failed: {summary['failed_count']} relation(s) failed"
        )
    return summary


def notify_data_quality(**context: Any) -> None:
    recipient_setting = os.environ.get("QUALITY_ALERT_EMAIL_TO", "").strip()
    smtp_host = os.environ.get("AIRFLOW__SMTP__SMTP_HOST", "").strip()
    sender = os.environ.get("AIRFLOW__SMTP__SMTP_MAIL_FROM", "").strip()
    smtp_port = os.environ.get("AIRFLOW__SMTP__SMTP_PORT", "").strip()
    missing = [
        name
        for name, value in (
            ("QUALITY_ALERT_EMAIL_TO", recipient_setting),
            ("AIRFLOW__SMTP__SMTP_HOST", smtp_host),
            ("AIRFLOW__SMTP__SMTP_PORT", smtp_port),
            ("AIRFLOW__SMTP__SMTP_MAIL_FROM", sender),
        )
        if not value
    ]
    if missing:
        raise AirflowException(
            "Email notification is not configured; set " + ", ".join(missing) + " in .env"
        )
    recipients = [
        address.strip() for address in recipient_setting.split(",") if address.strip()
    ]
    if not recipients:
        raise AirflowException("QUALITY_ALERT_EMAIL_TO must contain at least one recipient")

    task_instance = context["ti"]
    dag_run = context["dag_run"]
    summary = task_instance.xcom_pull(
        task_ids="monitor_data_quality", key="data_quality_summary"
    )
    task_states = {
        item.task_id: item.state for item in dag_run.get_task_instances()
    }
    failed_tasks = sorted(
        task_id for task_id, state in task_states.items() if state == "failed"
    )
    overall_status = (
        "FAIL"
        if failed_tasks or not summary or summary["status"] == "fail"
        else "WARN"
        if summary["status"] == "warn"
        else "PASS"
    )
    run_id = escape(str(summary.get("run_id", "not recorded"))) if summary else "not recorded"
    counts = (
        f"Checked: {summary['relations_checked']}; "
        f"passed: {summary['passed_count']}; "
        f"warnings: {summary['warning_count']}; "
        f"failed: {summary['failed_count']}"
        if summary
        else "Quality check summary was not recorded."
    )
    finding_rows = ""
    if summary:
        findings = [
            item
            for item in summary["results"]
            if item["freshness_status"] in ("warn", "fail")
            or item["volume_status"] in ("warn", "fail")
        ][:30]

        def render_finding(item: dict[str, Any]) -> str:
            age = item["freshness_age_hours"]
            change = item["volume_change_percent"]
            age_text = f"{age:.1f}h" if age is not None else "n/a"
            change_text = f"{change:.1f}%" if change is not None else "n/a"
            return (
                "<tr>"
                f"<td>{escape(item['schema'])}.{escape(item['table'])}</td>"
                f"<td>{escape(item['freshness_status'])}</td>"
                f"<td>{escape(item['volume_status'])}</td>"
                f"<td>{item['row_count']}</td>"
                f"<td>{age_text}</td>"
                f"<td>{change_text}</td>"
                "</tr>"
            )

        finding_rows = "".join(
            render_finding(item) for item in findings
        )
        if not findings:
            finding_rows = "<tr><td colspan='6'>No data quality warnings.</td></tr>"
    failed_task_list = (
        "<p>Failed Airflow tasks: "
        + escape(", ".join(failed_tasks))
        + "</p>"
        if failed_tasks
        else ""
    )
    html_content = (
        f"<h2>Gaming analytics data quality: {overall_status}</h2>"
        f"<p>DAG run: {escape(str(dag_run.run_id))}</p>"
        f"<p>Quality run: {run_id}</p><p>{escape(counts)}</p>"
        f"{failed_task_list}"
        "<table border='1' cellpadding='4'><tr><th>Relation</th>"
        "<th>Freshness</th><th>Volume</th><th>Rows</th>"
        "<th>Age</th><th>Change</th></tr>"
        f"{finding_rows}</table>"
    )
    send_email(
        to=recipients,
        subject=f"[{overall_status}] Gaming analytics data quality",
        html_content=html_content,
    )


with DAG(
    dag_id="gaming_analytics_pipeline",
    description="Daily Bronze ingestion, dbt Silver/Gold builds, and quality checks.",
    start_date=datetime(2025, 1, 1, tzinfo=ZoneInfo("Asia/Ulaanbaatar")),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "gaming-analytics",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["gaming-analytics", "capstone"],
) as dag:
    check_databases = PythonOperator(
        task_id="check_databases",
        python_callable=wait_for_databases,
    )
    sync_source = PythonOperator(
        task_id="sync_source_to_bronze",
        python_callable=run_meltano,
    )
    ingest_rawg = PythonOperator(
        task_id="ingest_rawg",
        python_callable=run_rawg,
    )
    load_excel = PythonOperator(
        task_id="load_business_workbooks",
        python_callable=load_business_workbooks,
    )
    build_dbt = PythonOperator(
        task_id="build_dbt_models",
        python_callable=run_dbt,
    )
    validate_dbt = PythonOperator(
        task_id="test_dbt_models",
        python_callable=test_dbt,
    )
    monitor_quality = PythonOperator(
        task_id="monitor_data_quality",
        python_callable=monitor_data_quality,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    send_quality_report = PythonOperator(
        task_id="email_data_quality_report",
        python_callable=notify_data_quality,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        check_databases
        >> sync_source
        >> ingest_rawg
        >> load_excel
        >> build_dbt
        >> validate_dbt
        >> monitor_quality
        >> send_quality_report
    )
