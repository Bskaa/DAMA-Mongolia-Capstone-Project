from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from monitoring.alerts import compose_email, new_alerts, smtp_settings, sort_alerts
from monitoring.collect_dbt_results import parse_run_results
from monitoring.reconcile import classify
from tests.airflow_stubs import CREATED_DAGS, load_dag_file

ALERTS = [
    ("warn", "slow_task", "dag . b", "200 s, usually 50 s"),
    ("critical", "dbt_failed", "test . not_null_x", "fail . 3 rows"),
    ("critical", "dbt_failed", "model . fact_y", "error"),
]


class AlertTests(unittest.TestCase):
    def test_critical_alerts_come_first(self) -> None:
        self.assertEqual([a[0] for a in sort_alerts(ALERTS)], ["critical", "critical", "warn"])

    def test_problems_already_sent_today_are_dropped(self) -> None:
        fresh = new_alerts(ALERTS, {("dbt_failed", "model . fact_y")})
        self.assertEqual(len(fresh), 2)
        self.assertNotIn(("critical", "dbt_failed", "model . fact_y", "error"), fresh)

    def test_email_groups_by_rule_and_caps_examples(self) -> None:
        many = [("critical", "rows_disappeared", f"gold.t{i}", "-1 rows") for i in range(8)]
        subject, body = compose_email(sort_alerts(many + ALERTS), repeated=2, who="capstone")
        self.assertTrue(subject.startswith("🔴 capstone: 11 new problem(s)"))
        self.assertIn("rows_disappeared x8", body)
        self.assertIn("... 3 more in monitoring.alert_log", body)
        self.assertIn("(+2 already reported today)", body)

    def test_warning_only_email_is_yellow(self) -> None:
        subject, _ = compose_email([ALERTS[0]], 0, "capstone")
        self.assertTrue(subject.startswith("🟡"))

    def test_email_is_disabled_without_smtp_settings(self) -> None:
        with mock.patch.dict(os.environ, {"AIRFLOW__SMTP__SMTP_HOST": "", "QUALITY_ALERT_EMAIL_TO": ""}):
            self.assertIsNone(smtp_settings())
        with mock.patch.dict(os.environ, {"AIRFLOW__SMTP__SMTP_HOST": "smtp.example.com",
                                          "QUALITY_ALERT_EMAIL_TO": "a@x.mn, b@x.mn",
                                          "QUALITY_EMAIL_ENABLED": "true"}):
            self.assertEqual(smtp_settings()["to"], ["a@x.mn", "b@x.mn"])


class DbtResultsTests(unittest.TestCase):
    def test_parses_models_tests_and_unit_tests(self) -> None:
        data = {
            "metadata": {"invocation_id": "abc", "generated_at": "2026-10-31T02:10:00.123456Z"},
            "args": {"which": "build"},
            "results": [
                {"unique_id": "model.gaming_analytics.fact_sessions", "status": "success",
                 "execution_time": 1.23456, "failures": None, "message": "SELECT 10"},
                {"unique_id": "test.gaming_analytics.not_null_fact_sessions_session_id.5f1a",
                 "status": "warn", "execution_time": 0.2, "failures": 7, "message": "Got 7"},
                {"unique_id": "unit_test.gaming_analytics.fact_campaigns.fact_campaigns_budget_math",
                 "status": "pass", "execution_time": 0.1},
                {"unique_id": "operation.gaming_analytics.on-run-start", "status": "success"},
            ],
        }
        invocation_id, _, command, rows = parse_run_results(data)
        self.assertEqual((invocation_id, command), ("abc", "build"))
        self.assertEqual([row[3] for row in rows], ["model", "test", "unit_test"])
        self.assertEqual(rows[0][4], "fact_sessions")
        self.assertEqual(rows[1][4], "not_null_fact_sessions_session_id")
        self.assertEqual(rows[1][8], 7)
        self.assertEqual(rows[2][4], "fact_campaigns_budget_math")

    def test_rejects_files_without_metadata(self) -> None:
        with self.assertRaises(ValueError):
            parse_run_results({"results": []})


class ReconciliationTests(unittest.TestCase):
    def test_bounds_classify_missing_extra_and_healthy(self) -> None:
        self.assertEqual(classify(lower=100, upper=120, bronze_rows=110), "match")
        self.assertEqual(classify(lower=100, upper=120, bronze_rows=97), "missing_in_bronze")
        self.assertEqual(classify(lower=100, upper=100, bronze_rows=101), "extra_in_bronze")
        self.assertEqual(classify(lower=0, upper=50, bronze_rows=0), "bronze_empty")
        self.assertEqual(classify(lower=0, upper=0, bronze_rows=0), "match")


class DagIntegrityTests(unittest.TestCase):
    """Every DAG file imports, and every task carries the monitoring callbacks."""

    @classmethod
    def setUpClass(cls) -> None:
        CREATED_DAGS.clear()
        cls.modules = {
            name: load_dag_file(name)
            for name in ("gaming_analytics_pipeline", "gaming_source_data_generator", "pipeline_watchdog",
                         "dbt_weekly_full_refresh")
        }
        cls.dags = {dag.dag_id: dag for dag in CREATED_DAGS}

    def test_expected_dags_exist(self) -> None:
        self.assertTrue({"gaming_analytics_pipeline", "gaming_source_data_generator",
                         "pipeline_watchdog", "alert_drill", "dbt_weekly_full_refresh"} <= set(self.dags))

    def test_every_dag_records_and_alerts_task_outcomes(self) -> None:
        for dag in self.dags.values():
            default_args = dag.kwargs["default_args"]
            for callback in ("on_success_callback", "on_failure_callback", "on_retry_callback"):
                self.assertIn(callback, default_args, f"{dag.dag_id} lacks {callback}")

    def test_pipeline_measures_then_judges(self) -> None:
        tasks = {task.task_id: task for task in self.dags["gaming_analytics_pipeline"].tasks}
        self.assertEqual(tasks["collect_dbt_results"].upstream, {"dbt_build"})
        self.assertEqual(tasks["monitor_data_quality"].upstream, {"collect_dbt_results"})
        self.assertEqual(tasks["check_and_alert"].upstream, {"monitor_data_quality", "reconcile_source_bronze"})
        self.assertEqual(tasks["reconcile_source_bronze"].upstream, {"sync_source_to_bronze"})
        for task_id in ("collect_dbt_results", "monitor_data_quality", "check_and_alert"):
            self.assertEqual(tasks[task_id].kwargs["trigger_rule"], "all_done")

    def test_failure_callback_never_raises_and_requests_an_alert(self) -> None:
        import pipeline_ops

        ti = mock.Mock(dag_id="d", task_id="t", run_id="r", try_number=2,
                       start_date=datetime.now(timezone.utc) - timedelta(seconds=5))
        completed = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch.object(pipeline_ops, "run_module", return_value=completed) as run:
            pipeline_ops.on_failure({"ti": ti, "run_id": "r"})
        args = run.call_args.args
        self.assertEqual(args[0], "monitoring.task_events")
        self.assertIn("--alert", args)
        self.assertEqual(args[args.index("--state") + 1], "failed")
        with mock.patch.object(pipeline_ops, "run_module", side_effect=OSError("boom")):
            pipeline_ops.on_success({"ti": ti})  # must not raise


if __name__ == "__main__":
    unittest.main()
