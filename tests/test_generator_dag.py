"""Tests for the hourly generator DAG's scheduling logic (Airflow stubbed when absent)."""

from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from tests.airflow_stubs import load_dag_file  # noqa: E402

DAG_MODULE = load_dag_file("gaming_source_data_generator")
UTC = timezone.utc


def default_params(**overrides):
    params = dict(DAG_MODULE.DEFAULT_PARAMS)
    params.update(new_game_every_hours=DAG_MODULE.DEFAULT_NEW_GAME_EVERY_HOURS, seasonality=True)
    params.update(overrides)
    return params


class TrafficTests(unittest.TestCase):
    def test_weekday_curve_averages_to_the_base_volume(self) -> None:
        self.assertAlmostEqual(sum(DAG_MODULE.HOURLY_TRAFFIC) / 24, 1.0)

    def test_evening_is_busier_than_night_in_ulaanbaatar_time(self) -> None:
        # 2026-11-02 is a Monday. 12:00 UTC = 20:00 UB; 19:00 UTC = 03:00 UB.
        evening = DAG_MODULE.traffic_factor(datetime(2026, 11, 2, 12, tzinfo=UTC), True)
        night = DAG_MODULE.traffic_factor(datetime(2026, 11, 2, 19, tzinfo=UTC), True)
        self.assertGreater(evening, 3 * night)

    def test_weekend_boost_and_disabled_seasonality(self) -> None:
        monday = DAG_MODULE.traffic_factor(datetime(2026, 11, 2, 12, tzinfo=UTC), True)
        saturday = DAG_MODULE.traffic_factor(datetime(2026, 10, 31, 12, tzinfo=UTC), True)
        self.assertAlmostEqual(saturday, monday * DAG_MODULE.WEEKEND_BOOST)
        self.assertEqual(DAG_MODULE.traffic_factor(datetime(2026, 11, 2, 19, tzinfo=UTC), False), 1.0)

    def test_weekday_total_is_24_times_the_hourly_base(self) -> None:
        params = default_params()
        day = datetime(2026, 11, 1, 16, tzinfo=UTC)  # 00:00 Monday in Ulaanbaatar
        total = sum(
            DAG_MODULE.batch_counts(params, day + timedelta(hours=h), True)["sessions"]
            for h in range(24)
        )
        self.assertAlmostEqual(total, 24 * params["sessions"], delta=24)

    def test_one_new_game_on_the_configured_cadence(self) -> None:
        params = default_params()
        start = datetime(2026, 11, 2, 0, tzinfo=UTC)
        games = [DAG_MODULE.batch_counts(params, start + timedelta(hours=h), True)["games"] for h in range(48)]
        self.assertEqual(sum(games), 2)
        self.assertEqual(DAG_MODULE.batch_counts(default_params(new_game_every_hours=0), start, True)["games"], 0)


class WindowTests(unittest.TestCase):
    def test_uses_the_data_interval(self) -> None:
        start = datetime(2026, 10, 31, 9, tzinfo=UTC)
        window = DAG_MODULE.run_window({"data_interval_start": start, "data_interval_end": start + timedelta(hours=1)})
        self.assertEqual(window, (start, start + timedelta(hours=1)))

    def test_manual_runs_fall_back_to_the_previous_full_hour(self) -> None:
        start, end = DAG_MODULE.run_window({"data_interval_start": None, "data_interval_end": None})
        self.assertEqual(end - start, timedelta(hours=1))
        self.assertEqual((end.minute, end.second, end.microsecond), (0, 0, 0))

    def test_seed_is_stable_per_window(self) -> None:
        start = datetime(2026, 10, 31, 9, tzinfo=UTC)
        end = start + timedelta(hours=1)
        self.assertEqual(DAG_MODULE.window_seed(start, end), DAG_MODULE.window_seed(start, end))
        self.assertNotEqual(DAG_MODULE.window_seed(start, end), DAG_MODULE.window_seed(end, end + timedelta(hours=1)))


class CommandTests(unittest.TestCase):
    def test_runs_the_generator_in_append_mode_for_the_window(self) -> None:
        start = datetime(2026, 11, 2, 12, tzinfo=UTC)
        report = {"mode": "append", "appended_sessions": 1, "invalid_sessions": 0, "invalid_session_dates": 0}
        completed = mock.Mock(returncode=0, stdout=json.dumps(report), stderr="")
        with mock.patch.object(DAG_MODULE.subprocess, "run", return_value=completed) as run, \
                mock.patch.dict(os.environ, {"POSTGRES_USER": "gaming", "POSTGRES_PASSWORD": "p@ss"}):
            result = DAG_MODULE.generate_hour(
                params=default_params(),
                data_interval_start=start,
                data_interval_end=start + timedelta(hours=1),
            )
        command = run.call_args.args[0]
        self.assertEqual(command[1:5], ["-m", "ingestion.generate.generate_gaming_data", "--mode", "append"])
        self.assertIn("--window-start", command)
        self.assertEqual(command[command.index("--window-start") + 1], start.isoformat())
        self.assertNotIn("--reset", command)
        environment = run.call_args.kwargs["env"]
        self.assertIn("p%40ss@postgres-source:5432", environment["DATABASE_URL"])
        self.assertEqual(result, {"appended_sessions": 1})


if __name__ == "__main__":
    unittest.main()
