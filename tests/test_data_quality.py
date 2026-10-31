from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from monitoring.data_quality import (
    _relations,
    evaluate_freshness,
    evaluate_volume,
    soften_for_optional,
)


class DataQualityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.checked_at = datetime(2026, 10, 27, 0, 0, tzinfo=timezone.utc)

    def test_missing_freshness_timestamp_fails(self) -> None:
        status, age_hours = evaluate_freshness(
            None, self.checked_at, warn_after_hours=48, error_after_hours=168
        )
        self.assertEqual(status, "fail")
        self.assertIsNone(age_hours)

    def test_freshness_warns_and_fails_at_configured_thresholds(self) -> None:
        warning_status, warning_age = evaluate_freshness(
            self.checked_at - timedelta(hours=72),
            self.checked_at,
            warn_after_hours=48,
            error_after_hours=168,
        )
        failed_status, failed_age = evaluate_freshness(
            self.checked_at - timedelta(hours=200),
            self.checked_at,
            warn_after_hours=48,
            error_after_hours=168,
        )
        self.assertEqual(warning_status, "warn")
        self.assertEqual(warning_age, 72)
        self.assertEqual(failed_status, "fail")
        self.assertEqual(failed_age, 200)

    def test_naive_database_timestamps_are_treated_as_utc(self) -> None:
        last_seen = (self.checked_at - timedelta(hours=1)).replace(tzinfo=None)
        status, age_hours = evaluate_freshness(
            last_seen, self.checked_at, warn_after_hours=48, error_after_hours=168
        )
        self.assertEqual(status, "pass")
        self.assertEqual(age_hours, 1)

    def test_volume_first_run_is_baseline(self) -> None:
        status, change = evaluate_volume(100, None, warn_percent=50)
        self.assertEqual(status, "baseline")
        self.assertIsNone(change)

    def test_volume_change_warns_only_above_threshold(self) -> None:
        within_limit, within_change = evaluate_volume(150, 100, warn_percent=50)
        above_limit, above_change = evaluate_volume(151, 100, warn_percent=50)
        self.assertEqual(within_limit, "pass")
        self.assertEqual(within_change, 50)
        self.assertEqual(above_limit, "warn")
        self.assertEqual(above_change, 51)

    def test_empty_relation_fails_volume_check(self) -> None:
        status, change = evaluate_volume(0, 100, warn_percent=50)
        self.assertEqual(status, "fail")
        self.assertIsNone(change)

    def test_optional_relation_failures_become_warnings(self) -> None:
        self.assertEqual(soften_for_optional("fail", optional=True), "warn")
        self.assertEqual(soften_for_optional("fail", optional=False), "fail")
        self.assertEqual(soften_for_optional("pass", optional=True), "pass")

    def test_only_rawg_relations_are_optional(self) -> None:
        optional = {(r.schema, r.table) for r in _relations() if r.optional}
        self.assertIn(("bronze", "rawg_games"), optional)
        self.assertIn(("silver", "stg_rawg_game_genres"), optional)
        self.assertTrue(all("rawg" in table for _, table in optional))


if __name__ == "__main__":
    unittest.main()
