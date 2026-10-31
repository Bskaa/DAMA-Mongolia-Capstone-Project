from __future__ import annotations

import json
import random
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from ingestion.excel.load_excel import history_rows
from ingestion.generate.common import random_datetime


class BusinessHistoryTests(unittest.TestCase):
    def test_history_rows_keep_every_record_with_its_business_key(self) -> None:
        load_id = uuid.uuid4()
        loaded_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        datasets = {
            "marketing_campaigns.xlsx": [
                {
                    "campaign_id": 7,
                    "game_id": 3,
                    "campaign": "Spring",
                    "budget": Decimal("1000.00"),
                    "start_date": date(2026, 3, 1),
                    "end_date": date(2026, 3, 31),
                }
            ],
            "game_targets.xlsx": [
                {"game_id": 3, "target_revenue": Decimal("5000.00"), "target_players": 10,
                 "target_sessions": 20, "target_rating": Decimal("4.20")}
            ],
            "budget.xlsx": [
                {"campaign_id": 7, "budget": Decimal("1000.00"), "actual_spend": Decimal("250.00")}
            ],
        }

        rows = history_rows(load_id, datasets, loaded_at)

        self.assertEqual(len(rows), 3)
        keys = {(row[1], row[2]) for row in rows}
        self.assertEqual(
            keys,
            {("marketing_campaigns.xlsx", 7), ("game_targets.xlsx", 3), ("budget.xlsx", 7)},
        )
        campaign = next(row for row in rows if row[1] == "marketing_campaigns.xlsx")
        self.assertEqual(campaign[0], load_id)
        self.assertEqual(campaign[4], loaded_at)
        self.assertEqual(json.loads(campaign[3])["start_date"], "2026-03-01")
        self.assertEqual(json.loads(campaign[3])["budget"], "1000.00")


class GeneratorHelperTests(unittest.TestCase):
    def test_random_datetime_stays_within_bounds(self) -> None:
        rng = random.Random(1)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=2)
        for _ in range(200):
            value = random_datetime(rng, start, end)
            self.assertGreaterEqual(value, start)
            self.assertLessEqual(value, end)

    def test_random_datetime_returns_start_for_empty_range(self) -> None:
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(random_datetime(random.Random(1), start, start), start)


if __name__ == "__main__":
    unittest.main()
