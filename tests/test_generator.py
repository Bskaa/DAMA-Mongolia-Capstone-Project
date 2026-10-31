from __future__ import annotations

import contextlib
import io
import random
import unittest
from datetime import date

from ingestion.generate.common import COLUMNS, TABLES
from ingestion.generate.generate_gaming_data import (
    build_initial_dataset,
    parse_args,
)

SMALL = ["--players", "30", "--games", "6", "--sessions", "120", "--purchases", "40", "--reviews", "25"]


def parse_error(argv: list[str]) -> str:
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr), self_raises(SystemExit):
        parse_args(argv)
    return stderr.getvalue()


@contextlib.contextmanager
def self_raises(exception: type[BaseException]):
    try:
        yield
    except exception:
        return
    raise AssertionError(f"{exception.__name__} was not raised")


class ParseArgsTests(unittest.TestCase):
    def test_initial_is_the_default_mode_with_reproducible_defaults(self) -> None:
        args = parse_args([])
        self.assertEqual(args.mode, "initial")
        self.assertEqual(args.seed, 42)
        self.assertEqual(args.as_of, date(2025, 12, 31))
        self.assertEqual(args.players, 10_000)
        self.assertEqual(args.achievements, 100_000 // 5)

    def test_append_mode_uses_batch_sized_defaults(self) -> None:
        args = parse_args(["--mode", "append"])
        self.assertEqual(args.seed, 123)
        self.assertEqual(args.as_of, date.today())
        self.assertEqual((args.players, args.games, args.sessions), (1_000, 100, 10_000))
        self.assertEqual(args.achievements, 2_000)

    def test_explicit_options_override_mode_defaults(self) -> None:
        args = parse_args(["--mode", "append", "--sessions", "5", "--as-of", "2026-01-31"])
        self.assertEqual(args.sessions, 5)
        self.assertEqual(args.as_of, date(2026, 1, 31))

    def test_append_allows_zero_counts_but_initial_does_not(self) -> None:
        self.assertEqual(parse_args(["--mode", "append", "--players", "0"]).players, 0)
        self.assertIn("at least 1", parse_error(["--players", "0"]))

    def test_reset_is_rejected_in_append_mode(self) -> None:
        self.assertIn("--reset is only valid", parse_error(["--mode", "append", "--reset"]))

    def test_window_sets_as_of_and_is_append_only(self) -> None:
        args = parse_args([
            "--mode", "append",
            "--window-start", "2026-10-31T08:00:00+08:00",
            "--window-end", "2026-10-31T09:00:00+08:00",
        ])
        self.assertEqual(args.window_start.isoformat(), "2026-10-31T00:00:00+00:00")
        self.assertEqual(args.as_of, date(2026, 10, 31))
        self.assertIn("only valid with --mode append",
                      parse_error(["--window-start", "2026-10-31T00:00", "--window-end", "2026-10-31T01:00"]))
        self.assertIn("must be given together", parse_error(["--mode", "append", "--window-start", "2026-10-31T00:00"]))
        self.assertIn("earlier than", parse_error(
            ["--mode", "append", "--window-start", "2026-10-31T02:00", "--window-end", "2026-10-31T01:00"]))

    def test_initial_rejects_more_reviews_than_player_game_pairs(self) -> None:
        self.assertIn("--reviews", parse_error(["--players", "2", "--games", "2", "--reviews", "5"]))


class InitialDatasetTests(unittest.TestCase):
    def test_same_seed_builds_the_same_dataset(self) -> None:
        first = build_initial_dataset(parse_args(SMALL))
        second = build_initial_dataset(parse_args(SMALL))
        self.assertEqual(first, second)

    def test_rows_respect_counts_shapes_and_business_rules(self) -> None:
        args = parse_args(SMALL)
        data = build_initial_dataset(args)
        self.assertEqual(set(data), set(TABLES))
        for table, rows in data.items():
            for row in rows:
                self.assertEqual(len(row), len(COLUMNS[table]), table)
        self.assertEqual(len(data["players"]), 30)
        self.assertEqual(len(data["sessions"]), 120)
        self.assertEqual(len(data["achievements"]), min(120 // 5, 30 * 6))

        supported = {(row[0], row[1]) for row in data["game_platforms"]}
        players = {row[0]: row[4] for row in data["players"]}
        releases = {row[0]: row[4] for row in data["games"]}
        for session_id, player_id, game_id, platform_id, started, ended, minutes, _ in data["sessions"]:
            self.assertIn((game_id, platform_id), supported)
            self.assertGreater(ended, started)
            self.assertEqual((ended - started).total_seconds(), minutes * 60)
            self.assertGreaterEqual(started, players[player_id])
            self.assertGreaterEqual(started.date(), releases[game_id])
        review_pairs = [(row[1], row[2]) for row in data["reviews"]]
        self.assertEqual(len(review_pairs), len(set(review_pairs)))


if __name__ == "__main__":
    unittest.main()
