from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg import sql


@dataclass(frozen=True)
class MonitoredRelation:
    schema: str
    table: str
    freshness_column: str | None = None
    warn_after_hours: float | None = None
    error_after_hours: float | None = None
    # Optional relations (the RAWG catalogue) may be empty or stale without
    # failing the pipeline: RAWG ingestion is skipped when no API key is set.
    optional: bool = False


def soften_for_optional(status: str, optional: bool) -> str:
    """Report an optional relation's failure as a warning instead."""
    return "warn" if optional and status == "fail" else status


def _relations() -> tuple[MonitoredRelation, ...]:
    meltano_tables = (
        "players",
        "platforms",
        "games",
        "game_platforms",
        "sessions",
        "purchases",
        "achievements",
        "reviews",
    )
    rawg_tables = (
        "rawg_games",
        "rawg_platforms",
        "rawg_genres",
        "rawg_publishers",
        "rawg_developers",
    )
    excel_tables = (
        "raw_marketing_campaigns",
        "raw_game_targets",
        "raw_campaign_budget",
        "business_file_loads",
        "business_load_log",
        "business_record_history",
    )
    silver_tables = (
        "stg_players",
        "stg_platforms",
        "stg_games",
        "stg_game_platforms",
        "stg_sessions",
        "stg_purchases",
        "stg_achievements",
        "stg_reviews",
        "stg_rawg_games",
        "stg_rawg_platforms",
        "stg_rawg_genres",
        "stg_rawg_publishers",
        "stg_rawg_developers",
        "stg_marketing_campaigns",
        "stg_game_targets",
        "stg_campaign_budget",
        "stg_rawg_game_genres",
        "stg_business_record_history",
        "fx_rates_to_usd",
    )
    gold_tables = (
        "bridge_game_platforms",
        "dim_date",
        "dim_games",
        "dim_platforms",
        "dim_players",
        "fact_achievements",
        "fact_campaigns",
        "fact_purchases",
        "fact_reviews",
        "fact_sessions",
        "dim_players_history",
        "dim_games_history",
        "mart_daily_kpis",
        "mart_game_target_vs_actual",
        "mart_campaign_performance",
        "mart_player_retention",
        "mart_genre_benchmark",
    )
    snapshot_tables = ("snap_players", "snap_games")

    freshness_warn = _positive_float("DATA_QUALITY_FRESHNESS_WARN_HOURS", 48)
    freshness_error = _positive_float("DATA_QUALITY_FRESHNESS_ERROR_HOURS", 168)
    excel_warn = _positive_float("DATA_QUALITY_EXCEL_FRESHNESS_WARN_HOURS", 720)
    excel_error = _positive_float("DATA_QUALITY_EXCEL_FRESHNESS_ERROR_HOURS", 1440)
    if freshness_error <= freshness_warn:
        raise ValueError(
            "DATA_QUALITY_FRESHNESS_ERROR_HOURS must be greater than "
            "DATA_QUALITY_FRESHNESS_WARN_HOURS"
        )
    if excel_error <= excel_warn:
        raise ValueError(
            "DATA_QUALITY_EXCEL_FRESHNESS_ERROR_HOURS must be greater than "
            "DATA_QUALITY_EXCEL_FRESHNESS_WARN_HOURS"
        )

    relations = [
        MonitoredRelation("bronze", name, "_sdc_extracted_at", freshness_warn, freshness_error)
        for name in meltano_tables
    ]
    relations.extend(
        MonitoredRelation(
            "bronze", name, "fetched_at", freshness_warn, freshness_error, optional=True
        )
        for name in rawg_tables
    )
    relations.extend(
        MonitoredRelation("bronze", name, "loaded_at", excel_warn, excel_error)
        for name in excel_tables
    )
    relations.extend(
        MonitoredRelation("silver", name, optional=name.startswith("stg_rawg_"))
        for name in silver_tables
    )
    relations.extend(MonitoredRelation("gold", name) for name in gold_tables)
    relations.extend(MonitoredRelation("snapshots", name) for name in snapshot_tables)
    return tuple(relations)


def _positive_float(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, default))
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


def evaluate_freshness(
    last_seen: datetime | None,
    checked_at: datetime,
    warn_after_hours: float,
    error_after_hours: float,
) -> tuple[str, float | None]:
    if last_seen is None:
        return "fail", None
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=timezone.utc)
    age_hours = max((checked_at - last_seen).total_seconds() / 3600, 0)
    if age_hours > error_after_hours:
        return "fail", age_hours
    if age_hours > warn_after_hours:
        return "warn", age_hours
    return "pass", age_hours


def evaluate_volume(
    current_count: int,
    previous_count: int | None,
    warn_percent: float,
) -> tuple[str, float | None]:
    if current_count == 0:
        return "fail", None
    if previous_count is None or previous_count == 0:
        return "baseline", None
    change_percent = (current_count - previous_count) / previous_count * 100
    if abs(change_percent) > warn_percent:
        return "warn", change_percent
    return "pass", change_percent


def _connection_values() -> dict[str, Any]:
    required = ("POSTGRES_USER", "POSTGRES_PASSWORD", "ANALYTICS_DB")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing database settings: {', '.join(missing)}")
    try:
        port = int(os.environ.get("ANALYTICS_PORT", "5432"))
    except ValueError as exc:
        raise ValueError("ANALYTICS_PORT must be an integer") from exc
    return {
        "host": os.environ.get("ANALYTICS_HOST", "localhost"),
        "port": port,
        "dbname": os.environ["ANALYTICS_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "connect_timeout": 10,
    }


def run_quality_checks() -> dict[str, Any]:
    checked_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    volume_warn_percent = _positive_float(
        "DATA_QUALITY_VOLUME_CHANGE_WARN_PERCENT", 50
    )
    relations = _relations()
    results: list[dict[str, Any]] = []

    with psycopg.connect(**_connection_values()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE SCHEMA IF NOT EXISTS monitoring;
                CREATE TABLE IF NOT EXISTS monitoring.data_quality_runs (
                    run_id UUID PRIMARY KEY,
                    checked_at TIMESTAMPTZ NOT NULL,
                    status TEXT NOT NULL,
                    passed_count INTEGER NOT NULL,
                    warning_count INTEGER NOT NULL,
                    failed_count INTEGER NOT NULL,
                    summary JSONB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS monitoring.data_quality_snapshots (
                    run_id UUID NOT NULL REFERENCES monitoring.data_quality_runs(run_id),
                    schema_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    row_count BIGINT NOT NULL,
                    last_ingested_at TIMESTAMPTZ,
                    freshness_status TEXT NOT NULL,
                    volume_change_percent NUMERIC,
                    volume_status TEXT NOT NULL,
                    PRIMARY KEY (run_id, schema_name, table_name)
                );
                """
            )

            for relation in relations:
                previous_query = sql.SQL(
                    """
                    SELECT snapshot.row_count
                    FROM monitoring.data_quality_snapshots AS snapshot
                    JOIN monitoring.data_quality_runs AS run USING (run_id)
                    WHERE snapshot.schema_name = %s AND snapshot.table_name = %s
                    ORDER BY run.checked_at DESC
                    LIMIT 1
                    """
                )
                cursor.execute(previous_query, (relation.schema, relation.table))
                previous_row = cursor.fetchone()
                previous_count = previous_row[0] if previous_row else None

                if relation.freshness_column:
                    count_query = sql.SQL(
                        "SELECT COUNT(*), MAX({}) FROM {}.{}"
                    ).format(
                        sql.Identifier(relation.freshness_column),
                        sql.Identifier(relation.schema),
                        sql.Identifier(relation.table),
                    )
                else:
                    count_query = sql.SQL("SELECT COUNT(*), NULL FROM {}.{}").format(
                        sql.Identifier(relation.schema),
                        sql.Identifier(relation.table),
                    )
                cursor.execute(count_query)
                current_count, last_ingested_at = cursor.fetchone()

                volume_status, change_percent = evaluate_volume(
                    current_count, previous_count, volume_warn_percent
                )
                if relation.freshness_column:
                    freshness_status, age_hours = evaluate_freshness(
                        last_ingested_at,
                        checked_at,
                        relation.warn_after_hours or 0,
                        relation.error_after_hours or 0,
                    )
                else:
                    freshness_status, age_hours = "not_applicable", None
                freshness_status = soften_for_optional(freshness_status, relation.optional)
                volume_status = soften_for_optional(volume_status, relation.optional)

                results.append(
                    {
                        "schema": relation.schema,
                        "table": relation.table,
                        "optional": relation.optional,
                        "row_count": current_count,
                        "last_ingested_at": (
                            last_ingested_at.isoformat()
                            if last_ingested_at is not None
                            else None
                        ),
                        "freshness_status": freshness_status,
                        "freshness_age_hours": age_hours,
                        "volume_status": volume_status,
                        "volume_change_percent": change_percent,
                    }
                )

            relation_statuses = [
                "fail"
                if "fail"
                in (item["freshness_status"], item["volume_status"])
                else "warn"
                if "warn"
                in (item["freshness_status"], item["volume_status"])
                else "pass"
                for item in results
            ]
            failed_count = relation_statuses.count("fail")
            warning_count = relation_statuses.count("warn")
            passed_count = relation_statuses.count("pass")
            status = "fail" if failed_count else "warn" if warning_count else "pass"
            summary: dict[str, Any] = {
                "run_id": run_id,
                "checked_at": checked_at.isoformat(),
                "status": status,
                "relations_checked": len(results),
                "passed_count": passed_count,
                "warning_count": warning_count,
                "failed_count": failed_count,
                "volume_change_warn_percent": volume_warn_percent,
                "results": results,
            }

            cursor.execute(
                """
                INSERT INTO monitoring.data_quality_runs (
                    run_id, checked_at, status, passed_count, warning_count,
                    failed_count, summary
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    checked_at,
                    status,
                    passed_count,
                    warning_count,
                    failed_count,
                    json.dumps(summary),
                ),
            )
            cursor.executemany(
                """
                INSERT INTO monitoring.data_quality_snapshots (
                    run_id, schema_name, table_name, row_count, last_ingested_at,
                    freshness_status, volume_change_percent, volume_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        run_id,
                        item["schema"],
                        item["table"],
                        item["row_count"],
                        item["last_ingested_at"],
                        item["freshness_status"],
                        item["volume_change_percent"],
                        item["volume_status"],
                    )
                    for item in results
                ],
            )

    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    result = run_quality_checks()
    if result["status"] == "fail":
        raise SystemExit(1)
