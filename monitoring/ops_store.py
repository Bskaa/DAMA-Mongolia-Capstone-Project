"""Operational history of the pipeline itself (DAMA: metadata about the data platform).

The warehouse tables describe the business; these describe the pipeline. Airflow,
dbt and PostgreSQL only know the *current* state, so this module stores one row
per event, which is what makes trends (slower tasks, growing test failures,
shrinking tables) visible at all:

    monitoring.data_quality_runs / _snapshots  (written by data_quality.py)
    monitoring.dbt_node_results   one row per dbt node per invocation
    monitoring.task_runs          one row per Airflow task try
    monitoring.alert_log          one row per alert that fired (sent or not)
"""

from __future__ import annotations

import os
from typing import Any

import psycopg

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.dbt_node_results (
    invocation_id TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    command TEXT,
    resource_type TEXT NOT NULL,
    node_name TEXT NOT NULL,
    unique_id TEXT NOT NULL,
    status TEXT NOT NULL,
    execution_seconds NUMERIC(12, 3),
    failures BIGINT,
    message TEXT,
    PRIMARY KEY (invocation_id, unique_id)
);
CREATE INDEX IF NOT EXISTS idx_dbt_node_results_generated_at
    ON monitoring.dbt_node_results (generated_at);

CREATE TABLE IF NOT EXISTS monitoring.task_runs (
    dag_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    try_number INTEGER NOT NULL,
    state TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ NOT NULL,
    duration_seconds NUMERIC(12, 1),
    PRIMARY KEY (dag_id, task_id, run_id, try_number)
);
CREATE INDEX IF NOT EXISTS idx_task_runs_ended_at ON monitoring.task_runs (ended_at);

CREATE TABLE IF NOT EXISTS monitoring.alert_log (
    alert_id BIGSERIAL PRIMARY KEY,
    fired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    alert_date DATE NOT NULL,
    source TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'warn')),
    check_name TEXT NOT NULL,
    object TEXT NOT NULL,
    detail TEXT NOT NULL,
    sent BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alert_log_date_check
    ON monitoring.alert_log (alert_date, check_name, object);
"""

# Day boundaries for "once per day" and "did it run today" (business time zone).
ALERT_TIMEZONE = os.environ.get("ALERT_TIMEZONE", "Asia/Ulaanbaatar")


def connection_settings() -> dict[str, Any]:
    missing = [name for name in ("POSTGRES_USER", "POSTGRES_PASSWORD") if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing database settings: {', '.join(missing)}")
    return {
        "host": os.environ.get("ANALYTICS_HOST", "localhost"),
        "port": int(os.environ.get("ANALYTICS_PORT", "5433")),
        "dbname": os.environ.get("ANALYTICS_DB", "gaming_analytics"),
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "connect_timeout": 10,
    }


def connect() -> psycopg.Connection[Any]:
    connection = psycopg.connect(**connection_settings())
    with connection.cursor() as cursor:
        cursor.execute("SELECT set_config('TimeZone', %s, false)", (ALERT_TIMEZONE,))
    return connection


def ensure_ops_tables(cursor: psycopg.Cursor[Any]) -> None:
    cursor.execute(OPS_DDL)
