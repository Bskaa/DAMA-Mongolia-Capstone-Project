from __future__ import annotations

import unittest
from collections import Counter, defaultdict
from datetime import timedelta

from ingestion.generate import behavior
from ingestion.generate.generate_gaming_data import build_initial_dataset, parse_args

ARGS = ["--players", "3000", "--games", "1000", "--sessions", "40000", "--purchases", "6000",
        "--reviews", "4000"]


class BehaviorModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.data = build_initial_dataset(parse_args(ARGS))

    def test_traits_are_stable_per_id(self) -> None:
        self.assertEqual(behavior.player_traits(42), behavior.player_traits(42))
        self.assertEqual(behavior.game_traits(7), behavior.game_traits(7))
        self.assertNotEqual(behavior.player_traits(42), behavior.player_traits(43))

    def test_cohort_retention_decays(self) -> None:
        months = defaultdict(set)
        for session in self.data["sessions"]:
            months[session[1]].add(session[4].year * 12 + session[4].month)
        first = {player: min(m) for player, m in months.items()}
        retained = Counter()
        for player, active in months.items():
            for month in active:
                retained[month - first[player]] += 1
        self.assertGreater(retained[0], retained[1])
        self.assertGreater(retained[1], retained[6])

    def test_a_few_games_are_hits(self) -> None:
        counts = Counter(session[2] for session in self.data["sessions"])
        top_share = sum(count for _, count in counts.most_common(10)) / len(self.data["sessions"])
        self.assertGreater(top_share, 0.05)  # 10 of 1000 games = 1 % if uniform

    def test_purchases_happen_inside_sessions_in_local_currency(self) -> None:
        country = {row[0]: row[2] for row in self.data["players"]}
        sessions = defaultdict(list)
        for row in self.data["sessions"]:
            sessions[(row[1], row[2])].append((row[4], row[5]))
        for _, player_id, game_id, purchased_at, amount, currency, _, _ in self.data["purchases"]:
            self.assertEqual(currency, behavior.COUNTRY_CURRENCY[country[player_id]])
            self.assertGreater(amount, 0)
            self.assertTrue(any(start <= purchased_at <= end for start, end in sessions[(player_id, game_id)]))

    def test_reviews_only_from_pairs_that_played_and_follow_quality(self) -> None:
        played = {(row[1], row[2]) for row in self.data["sessions"]}
        ratings = defaultdict(list)
        for _, player_id, game_id, rating, _, _ in self.data["reviews"]:
            self.assertIn((player_id, game_id), played)
            ratings[behavior.game_traits(game_id).quality > 3.3].append(rating)
        mean = {key: sum(values) / len(values) for key, values in ratings.items()}
        self.assertGreater(mean[True], mean[False] + 0.5)

    def test_campaigns_lift_the_promoted_game(self) -> None:
        campaigns = behavior.load_campaigns()
        if not campaigns:
            self.skipTest("marketing_campaigns.xlsx not available")
        during = before = 0
        for campaign in campaigns:
            days = (campaign.end - campaign.start).days + 1
            for row in self.data["sessions"]:
                if row[2] != campaign.game_id:
                    continue
                day = row[4].date()
                if campaign.start <= day <= campaign.end:
                    during += 1
                elif campaign.start - timedelta(days=days) <= day < campaign.start:
                    before += 1
        self.assertGreater(during, 1.5 * before)

    def test_evenings_are_busier_than_nights_in_ulaanbaatar(self) -> None:
        hours = Counter((row[4].hour + behavior.LOCAL_UTC_OFFSET_HOURS) % 24 for row in self.data["sessions"])
        self.assertGreater(hours[20], 4 * hours[3])


if __name__ == "__main__":
    unittest.main()
