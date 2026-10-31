"""Simulate live traffic: append one hour of synthetic activity to the source DB every hour.

Each run calls the single generator CLI in append mode:

    python -m ingestion.generate.generate_gaming_data --mode append \
        --window-start <data_interval_start> --window-end <data_interval_end> ...

* The time window is the run's data interval, so every new account, game and
  event is timestamped inside that hour and its updated_at is newer than
  Meltano's incremental bookmark (the daily pipeline picks it up at 02:00).
* The seed is derived from the window, so a retry of the same hour generates the
  same batch; the generator writes in one transaction, so a failed attempt
  leaves nothing behind.
* Volumes follow a simple daily traffic curve in Asia/Ulaanbaatar time (quiet at
  night, peak in the evening; weekends +20 %). A weekday totals 24 x the base
  hourly volume, e.g. 9 600 sessions with the default 400/hour, close to the
  generator's daily append defaults. All base volumes can be overridden per run
  from the Airflow UI ("Trigger DAG w/ config").
"""

from __future__ import annotations

import json
import subprocess
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

from pipeline_ops import PROJECT_ROOT, TASK_CALLBACKS, VENV, project_environment

LOCAL_TZ = ZoneInfo("Asia/Ulaanbaatar")

# Relative traffic by local hour; normalised below so a weekday averages 1.0.
_HOURLY_SHAPE = (
    0.55, 0.35, 0.25, 0.20, 0.20, 0.25,  # 00-05 night
    0.40, 0.60, 0.75, 0.85, 0.90, 0.95,  # 06-11 morning
    1.00, 1.05, 1.00, 1.00, 1.10, 1.25,  # 12-17 afternoon
    1.50, 1.75, 1.90, 1.85, 1.55, 1.05,  # 18-23 evening peak
)
HOURLY_TRAFFIC = tuple(value * 24 / sum(_HOURLY_SHAPE) for value in _HOURLY_SHAPE)
# Weekend days get ~20 % more traffic.
WEEKEND_BOOST = 1.2

# Base volume for one average hour (about 1/24 of the generator's daily append defaults).
DEFAULT_PARAMS = {
    "players": 40,
    "games": 0,
    "sessions": 400,
    "purchases": 120,
    "reviews": 80,
    "achievements": 80,
}
# New games are rare: by default one game is released every `new_game_every_hours` hours.
DEFAULT_NEW_GAME_EVERY_HOURS = 24


def traffic_factor(window_start: datetime, seasonality: bool) -> float:
    if not seasonality:
        return 1.0
    local = window_start.astimezone(LOCAL_TZ)
    factor = HOURLY_TRAFFIC[local.hour]
    if local.weekday() >= 5:
        factor *= WEEKEND_BOOST
    return factor


def batch_counts(
    params: dict[str, Any], window_start: datetime, seasonality: bool
) -> dict[str, int]:
    """Scale the base hourly volumes for this hour; games come on a fixed cadence."""
    factor = traffic_factor(window_start, seasonality)
    counts = {
        name: max(0, round(int(params[name]) * factor))
        for name in ("players", "sessions", "purchases", "reviews", "achievements")
    }
    counts["games"] = int(params["games"])
    every = int(params["new_game_every_hours"])
    if every > 0 and int(window_start.timestamp() // 3600) % every == 0:
        counts["games"] += 1
    return counts


def window_seed(window_start: datetime, window_end: datetime) -> int:
    """Stable seed per window: retries of the same hour reproduce the same batch."""
    key = f"{window_start.isoformat()}|{window_end.isoformat()}".encode()
    return zlib.crc32(key)


def run_window(context: dict[str, Any]) -> tuple[datetime, datetime]:
    """The hour this run generates.

    * A real data interval (start < end) is used as is.
    * Airflow 3 cron schedules (CronTriggerTimetable) give an empty interval
      (start == end == logical date): the run at 13:00 generates 12:00-13:00.
      Anchoring on the logical date, not the clock, keeps a retry or a late start
      on the same hour (and the same seed).
    * Manual runs anchor on their logical date too (= trigger time), so they
      generate the previous full hour; with no date at all, the clock is used.
    """
    start = context.get("data_interval_start")
    end = context.get("data_interval_end")
    if start is not None and end is not None and start < end:
        return start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    anchor = context.get("logical_date") or end or datetime.now(timezone.utc)
    end = anchor.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return end - timedelta(hours=1), end


def generate_hour(**context: Any) -> dict[str, Any]:
    params = context["params"]
    window_start, window_end = run_window(context)
    counts = batch_counts(params, window_start, bool(params["seasonality"]))
    command = [
        str(VENV / "bin" / "python"),
        "-m",
        "ingestion.generate.generate_gaming_data",
        "--mode",
        "append",
        "--window-start",
        window_start.isoformat(),
        "--window-end",
        window_end.isoformat(),
        "--seed",
        str(window_seed(window_start, window_end)),
    ]
    for name in ("players", "games", "sessions", "purchases", "reviews", "achievements"):
        command += [f"--{name}", str(counts[name])]
    print("Running:", " ".join(command))

    result = subprocess.run(
        command, cwd=PROJECT_ROOT, env=project_environment(), capture_output=True, text=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode:
        hint = (
            " Seed the source first with `--mode initial`."
            if "Run --mode initial first" in (result.stdout + result.stderr)
            else ""
        )
        raise AirflowException(f"Generator exited with code {result.returncode}.{hint}")
    report = json.loads(result.stdout)
    if report.get("invalid_sessions") or report.get("invalid_session_dates"):
        raise AirflowException(f"Generated data failed validation: {report}")
    return {key: value for key, value in report.items() if key.startswith(("appended_", "window_"))}


with DAG(
    dag_id="gaming_source_data_generator",
    description="Hourly append of one hour of synthetic activity to the source database.",
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TZ),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "gaming-analytics",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
        **TASK_CALLBACKS,
    },
    params={
        "players": Param(DEFAULT_PARAMS["players"], type="integer", minimum=0,
                         description="New players in an average hour (scaled by traffic)."),
        "games": Param(DEFAULT_PARAMS["games"], type="integer", minimum=0,
                       description="Extra new games per run (not scaled)."),
        "new_game_every_hours": Param(DEFAULT_NEW_GAME_EVERY_HOURS, type="integer", minimum=0,
                                      description="Release one new game every N hours; 0 disables."),
        "sessions": Param(DEFAULT_PARAMS["sessions"], type="integer", minimum=0,
                          description="Sessions in an average hour (scaled by traffic)."),
        "purchases": Param(DEFAULT_PARAMS["purchases"], type="integer", minimum=0,
                           description="Purchases in an average hour (scaled by traffic)."),
        "reviews": Param(DEFAULT_PARAMS["reviews"], type="integer", minimum=0,
                         description="Reviews in an average hour (scaled by traffic)."),
        "achievements": Param(DEFAULT_PARAMS["achievements"], type="integer", minimum=0,
                              description="Achievements in an average hour (scaled by traffic)."),
        "seasonality": Param(True, type="boolean",
                             description="Apply the day/night and weekend traffic curve."),
    },
    tags=["gaming-analytics", "capstone", "data-generator"],
    doc_md=__doc__,
) as dag:
    PythonOperator(task_id="append_hour_of_activity", python_callable=generate_hour)
