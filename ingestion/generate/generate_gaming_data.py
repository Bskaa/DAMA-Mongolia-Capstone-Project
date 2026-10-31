"""Generate synthetic data for the gaming source database.

Two modes share one CLI:

  --mode initial (default)  Build a complete, reproducible dataset. Refuses to
                            write into populated tables unless --reset is given.
  --mode append             Add new players, games and activity on top of the
                            existing data. Never deletes or updates rows; new IDs
                            continue from MAX(id) + 1.

With --window-start/--window-end (append only) every generated timestamp falls
inside that window, so each scheduled batch looks like the events of that hour
and its updated_at values are newer than Meltano's incremental bookmark.

Examples:
  python -m ingestion.generate.generate_gaming_data
  python -m ingestion.generate.generate_gaming_data --reset --players 100 --games 20
  python -m ingestion.generate.generate_gaming_data --mode append --sessions 2000
  python -m ingestion.generate.generate_gaming_data --mode append \
      --window-start 2026-10-31T09:00:00+00:00 --window-end 2026-10-31T10:00:00+00:00 \
      --players 40 --games 0 --sessions 400 --purchases 120 --reviews 80 --achievements 80
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Sequence, TypeVar

import psycopg
from dotenv import load_dotenv
from faker import Faker

from ingestion.generate.common import (
    ACCOUNT_START,
    ACHIEVEMENT_NAMES,
    BASE_DATE,
    PLATFORM_SEED,
    ROOT,
    TABLES,
    Row,
    activity_bounds,
    insert_table,
    make_achievement,
    make_game,
    make_player,
    make_purchase,
    make_review,
    make_session,
    require_source_schema,
    table_counts,
    utc_end_of_day,
    utc_start_of_day,
)

# Defaults differ per mode: a full dataset for `initial`, a daily-sized batch for `append`.
MODE_DEFAULTS: dict[str, dict[str, Any]] = {
    "initial": {
        "seed": 42,
        "as_of": date(2025, 12, 31),
        "players": 10_000,
        "games": 1_000,
        "sessions": 100_000,
        "purchases": 30_000,
        "reviews": 20_000,
        "achievements": None,  # derived: min(sessions // 5, players * games)
    },
    "append": {
        "seed": 123,
        "as_of": None,  # today
        "players": 1_000,
        "games": 100,
        "sessions": 10_000,
        "purchases": 3_000,
        "reviews": 2_000,
        "achievements": 2_000,
    },
}
COUNT_OPTIONS = ("players", "games", "sessions", "purchases", "reviews", "achievements")
ID_COLUMNS = {
    "players": "player_id",
    "platforms": "platform_id",
    "games": "game_id",
    "sessions": "session_id",
    "purchases": "purchase_id",
    "achievements": "achievement_id",
    "reviews": "review_id",
}

Key = TypeVar("Key")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--mode",
        choices=tuple(MODE_DEFAULTS),
        default="initial",
        help="initial: build a full dataset (default); append: add rows to existing data.",
    )
    parser.add_argument("--seed", type=int, help="Random seed (initial: 42, append: 123).")
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        help="Latest activity date, YYYY-MM-DD (initial: 2025-12-31, append: today).",
    )
    parser.add_argument("--players", type=int, help="New players (initial: 10000, append: 1000).")
    parser.add_argument("--games", type=int, help="New games (initial: 1000, append: 100).")
    parser.add_argument("--sessions", type=int, help="Sessions (initial: 100000, append: 10000).")
    parser.add_argument("--purchases", type=int, help="Purchases (initial: 30000, append: 3000).")
    parser.add_argument("--reviews", type=int, help="Reviews (initial: 20000, append: 2000).")
    parser.add_argument(
        "--achievements",
        type=int,
        help="Achievements (initial: min(sessions // 5, players * games), append: 2000).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="initial mode only: delete existing source rows before loading.",
    )
    parser.add_argument(
        "--window-start",
        type=_aware_datetime,
        help="append mode only: earliest event timestamp (ISO 8601, UTC if no offset).",
    )
    parser.add_argument(
        "--window-end",
        type=_aware_datetime,
        help="append mode only: latest event timestamp (ISO 8601, UTC if no offset).",
    )
    args = parser.parse_args(argv)

    if (args.window_start is None) != (args.window_end is None):
        parser.error("--window-start and --window-end must be given together")
    if args.window_start is not None:
        if args.mode != "append":
            parser.error("--window-start/--window-end are only valid with --mode append")
        if args.window_start >= args.window_end:
            parser.error("--window-start must be earlier than --window-end")
        if args.as_of is not None and args.as_of != args.window_end.date():
            parser.error("--as-of must equal the date of --window-end when a window is given")
        args.as_of = args.window_end.astimezone(timezone.utc).date()

    defaults = MODE_DEFAULTS[args.mode]
    for name, default in defaults.items():
        if getattr(args, name) is None:
            setattr(args, name, default)
    if args.as_of is None:
        args.as_of = date.today()
    if args.mode == "initial" and args.achievements is None:
        args.achievements = min(args.sessions // 5, args.players * args.games)

    if args.reset and args.mode != "initial":
        parser.error("--reset is only valid with --mode initial; append never deletes data")
    if args.as_of < BASE_DATE:
        parser.error(f"--as-of must be on or after {BASE_DATE.isoformat()}")
    minimum = 1 if args.mode == "initial" else 0
    for name in COUNT_OPTIONS:
        if name == "achievements" and args.mode == "initial":
            continue
        if getattr(args, name) < minimum:
            parser.error(f"--{name} must be at least {minimum} in {args.mode} mode")
    if args.mode == "initial":
        if args.reviews > args.players * args.games:
            parser.error("--reviews cannot exceed the number of unique player/game pairs")
        if args.achievements > args.players * args.games * len(ACHIEVEMENT_NAMES):
            parser.error("--achievements cannot exceed the number of unique combinations")
    return args


def _aware_datetime(value: str) -> datetime:
    """Parse an ISO 8601 timestamp; naive values are interpreted as UTC."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid ISO 8601 timestamp: {value!r}") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def pick_unique(
    rng: random.Random,
    wanted: int,
    draw: Callable[[], Key],
    taken: set[Key],
    enumerate_all: Callable[[], Any],
) -> set[Key]:
    """Pick `wanted` new keys not in `taken`: random draws first, then a scan.

    The scan guarantees termination when the key space is almost full.
    """
    picked: set[Key] = set()
    attempts, max_attempts = 0, max(1_000, wanted * 20)
    while len(picked) < wanted and attempts < max_attempts:
        key = draw()
        if key not in taken:
            picked.add(key)
        attempts += 1
    if len(picked) < wanted:
        for key in enumerate_all():
            if key not in taken and key not in picked:
                picked.add(key)
                if len(picked) >= wanted:
                    break
    return picked


# --- initial mode -----------------------------------------------------------


def build_initial_dataset(args: argparse.Namespace) -> dict[str, list[Row]]:
    """Build a complete dataset in memory; deterministic for a given seed."""
    rng = random.Random(args.seed)
    fake = Faker("en_US")
    fake.seed_instance(args.seed)
    activity_start = utc_start_of_day(BASE_DATE)
    end_of_day = utc_end_of_day(args.as_of)
    account_start = utc_start_of_day(ACCOUNT_START)
    account_end = utc_start_of_day(args.as_of)
    data: dict[str, list[Row]] = {table: [] for table in TABLES}

    players: dict[int, datetime] = {}
    for player_id in range(1, args.players + 1):
        row = make_player(rng, fake, player_id, account_start, account_end)
        players[player_id] = row[4]
        data["players"].append(row)

    platform_ids = list(range(1, len(PLATFORM_SEED) + 1))
    for platform_id, (name, manufacturer) in zip(platform_ids, PLATFORM_SEED):
        data["platforms"].append((platform_id, name, manufacturer, activity_start, activity_start))

    games: dict[int, Row] = {}
    game_platforms: dict[int, list[int]] = {}
    for game_id in range(1, args.games + 1):
        game, pairs = make_game(rng, fake, game_id, args.as_of, platform_ids)
        games[game_id] = game
        game_platforms[game_id] = [pair[1] for pair in pairs]
        data["games"].append(game)
        data["game_platforms"].extend(pairs)

    def bounds(player_id: int, game_id: int):
        return activity_bounds(activity_start, end_of_day, players[player_id], games[game_id][4])

    for session_id in range(1, args.sessions + 1):
        player_id = rng.randint(1, args.players)
        game_id = rng.randint(1, args.games)
        platform_id = rng.choice(game_platforms[game_id])
        data["sessions"].append(
            make_session(rng, session_id, player_id, game_id, platform_id, bounds(player_id, game_id))
        )

    for purchase_id in range(1, args.purchases + 1):
        player_id = rng.randint(1, args.players)
        game_id = rng.randint(1, args.games)
        data["purchases"].append(
            make_purchase(rng, purchase_id, player_id, game_id, games[game_id][6], bounds(player_id, game_id))
        )

    review_pairs: set[tuple[int, int]] = set()
    while len(review_pairs) < args.reviews:
        review_pairs.add((rng.randint(1, args.players), rng.randint(1, args.games)))
    for review_id, (player_id, game_id) in enumerate(sorted(review_pairs), start=1):
        data["reviews"].append(make_review(rng, review_id, player_id, game_id, bounds(player_id, game_id)))

    achievement_keys: set[tuple[int, int, str]] = set()
    while len(achievement_keys) < args.achievements:
        achievement_keys.add(
            (rng.randint(1, args.players), rng.randint(1, args.games), rng.choice(ACHIEVEMENT_NAMES))
        )
    for achievement_id, (player_id, game_id, name) in enumerate(sorted(achievement_keys), start=1):
        data["achievements"].append(
            make_achievement(rng, achievement_id, player_id, game_id, name, bounds(player_id, game_id))
        )
    return data


def validate_source(cursor: psycopg.Cursor[Any]) -> dict[str, int]:
    """Cross-table business-rule checks run after every load."""
    cursor.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM sessions s
             JOIN game_platforms gp USING (game_id, platform_id)
             WHERE s.ended_at <= s.started_at
                OR s.duration_minutes != EXTRACT(EPOCH FROM (s.ended_at - s.started_at)) / 60
            ) AS invalid_sessions,
            (SELECT COUNT(*) FROM sessions s
             JOIN players p USING (player_id)
             JOIN games g USING (game_id)
             WHERE s.started_at < p.created_at
                OR s.started_at::date < g.release_date
            ) AS invalid_session_dates
        """
    )
    invalid_sessions, invalid_dates = cursor.fetchone()
    return {"invalid_sessions": int(invalid_sessions), "invalid_session_dates": int(invalid_dates)}


def run_initial(cursor: psycopg.Cursor[Any], args: argparse.Namespace) -> dict[str, Any]:
    existing = table_counts(cursor)
    if any(existing[table] for table in ("players", "games", "sessions")) and not args.reset:
        raise SystemExit(
            "Source tables already contain data. Use --reset to replace the dataset, "
            "or --mode append to add to it."
        )
    if args.reset:
        cursor.execute("TRUNCATE TABLE " + ", ".join(TABLES) + " RESTART IDENTITY CASCADE")
    dataset = build_initial_dataset(args)
    for table in TABLES:
        insert_table(cursor, table, dataset[table])

    report: dict[str, Any] = {"mode": "initial", **table_counts(cursor), **validate_source(cursor)}
    mismatched = {
        table: (report[table], len(rows))
        for table, rows in dataset.items()
        if report[table] != len(rows)
    }
    if mismatched or report["invalid_sessions"] or report["invalid_session_dates"]:
        raise RuntimeError(
            "Source data validation failed: "
            + json.dumps({"mismatched_counts": mismatched, **validate_source(cursor)})
        )
    return report


# --- append mode ------------------------------------------------------------


def _max_id(cursor: psycopg.Cursor[Any], table: str) -> int:
    cursor.execute(f"SELECT COALESCE(MAX({ID_COLUMNS[table]}), 0) FROM {table}")
    return int(cursor.fetchone()[0])


def _sync_sequences(cursor: psycopg.Cursor[Any]) -> None:
    """Move serial/identity sequences past the inserted IDs, if the columns have any."""
    for table, column in ID_COLUMNS.items():
        cursor.execute("SELECT pg_get_serial_sequence(%s, %s)", (f"public.{table}", column))
        sequence_name = cursor.fetchone()[0]
        max_id = _max_id(cursor, table)
        if sequence_name and max_id > 0:
            cursor.execute("SELECT setval(%s, %s, true)", (sequence_name, max_id))


def run_append(cursor: psycopg.Cursor[Any], args: argparse.Namespace) -> dict[str, Any]:
    rng = random.Random(args.seed)
    fake = Faker("en_US")
    fake.seed_instance(args.seed)
    if args.window_start is not None:
        # Scheduled batch: new accounts, games and events all happen inside the window.
        activity_start = account_start = args.window_start
        end_of_day = account_end = args.window_end
        new_game_release: date | None = args.window_start.date()
    else:
        activity_start = utc_start_of_day(BASE_DATE)
        end_of_day = utc_end_of_day(args.as_of)
        account_start = utc_start_of_day(ACCOUNT_START)
        account_end = utc_start_of_day(args.as_of)
        new_game_release = None

    # 1. Current state (read only).
    cursor.execute("SELECT player_id, created_at FROM players ORDER BY player_id")
    players = {int(player_id): created_at for player_id, created_at in cursor.fetchall()}
    cursor.execute("SELECT game_id, release_date, price FROM games ORDER BY game_id")
    games = {
        int(game_id): (release_date, Decimal(str(price)))
        for game_id, release_date, price in cursor.fetchall()
    }
    cursor.execute("SELECT game_id, platform_id FROM game_platforms ORDER BY game_id, platform_id")
    game_platforms: dict[int, list[int]] = {}
    for game_id, platform_id in cursor.fetchall():
        game_platforms.setdefault(int(game_id), []).append(int(platform_id))

    # 2. Platforms: add any seed platform that is missing.
    cursor.execute("SELECT platform_id, platform_name FROM platforms")
    platform_ids_by_name = {name: int(platform_id) for platform_id, name in cursor.fetchall()}
    next_platform_id = _max_id(cursor, "platforms") + 1
    new: dict[str, list[Row]] = {table: [] for table in TABLES}
    for name, manufacturer in PLATFORM_SEED:
        if name not in platform_ids_by_name:
            platform_ids_by_name[name] = next_platform_id
            new["platforms"].append(
                (next_platform_id, name, manufacturer, activity_start, activity_start)
            )
            next_platform_id += 1
    insert_table(cursor, "platforms", new["platforms"])
    platform_ids = sorted(platform_ids_by_name.values())

    # 3. New players and games continue from MAX(id) + 1.
    first_player_id = _max_id(cursor, "players") + 1
    for player_id in range(first_player_id, first_player_id + args.players):
        row = make_player(rng, fake, player_id, account_start, account_end)
        players[player_id] = row[4]
        new["players"].append(row)
    insert_table(cursor, "players", new["players"])

    first_game_id = _max_id(cursor, "games") + 1
    for game_id in range(first_game_id, first_game_id + args.games):
        game, pairs = make_game(
            rng, fake, game_id, args.as_of, platform_ids, release_date=new_game_release
        )
        games[game_id] = (game[4], game[6])
        game_platforms[game_id] = [pair[1] for pair in pairs]
        new["games"].append(game)
        new["game_platforms"].extend(pairs)
    insert_table(cursor, "games", new["games"])
    insert_table(cursor, "game_platforms", new["game_platforms"])

    # 4. Activity across old and new players/games.
    player_ids = list(players)
    game_ids = list(games)
    wants_activity = args.sessions or args.purchases or args.reviews or args.achievements
    if wants_activity and (not player_ids or not game_ids):
        raise SystemExit(
            "No players or games exist yet. Run --mode initial first, or append players and games."
        )
    playable_game_ids = [game_id for game_id in game_ids if game_platforms.get(game_id)]
    if args.sessions and not playable_game_ids:
        raise SystemExit("No game has a supported platform, so sessions cannot be generated.")

    def bounds(player_id: int, game_id: int):
        return activity_bounds(activity_start, end_of_day, players[player_id], games[game_id][0])

    first_session_id = _max_id(cursor, "sessions") + 1
    for session_id in range(first_session_id, first_session_id + args.sessions):
        player_id = rng.choice(player_ids)
        game_id = rng.choice(playable_game_ids)
        platform_id = rng.choice(game_platforms[game_id])
        new["sessions"].append(
            make_session(rng, session_id, player_id, game_id, platform_id, bounds(player_id, game_id))
        )
    insert_table(cursor, "sessions", new["sessions"])

    first_purchase_id = _max_id(cursor, "purchases") + 1
    for purchase_id in range(first_purchase_id, first_purchase_id + args.purchases):
        player_id = rng.choice(player_ids)
        game_id = rng.choice(game_ids)
        new["purchases"].append(
            make_purchase(rng, purchase_id, player_id, game_id, games[game_id][1], bounds(player_id, game_id))
        )
    insert_table(cursor, "purchases", new["purchases"])

    # Reviews: one per player/game pair.
    cursor.execute("SELECT player_id, game_id FROM reviews")
    taken_pairs = {(int(p), int(g)) for p, g in cursor.fetchall()}
    review_count = min(args.reviews, max(0, len(player_ids) * len(game_ids) - len(taken_pairs)))
    review_pairs = pick_unique(
        rng,
        review_count,
        lambda: (rng.choice(player_ids), rng.choice(game_ids)),
        taken_pairs,
        lambda: ((p, g) for p in player_ids for g in game_ids),
    )
    first_review_id = _max_id(cursor, "reviews") + 1
    for review_id, (player_id, game_id) in enumerate(sorted(review_pairs), start=first_review_id):
        new["reviews"].append(make_review(rng, review_id, player_id, game_id, bounds(player_id, game_id)))
    insert_table(cursor, "reviews", new["reviews"])

    # Achievements: one per player/game/achievement name.
    cursor.execute("SELECT player_id, game_id, achievement_name FROM achievements")
    taken_keys = {(int(p), int(g), name) for p, g, name in cursor.fetchall()}
    capacity = len(player_ids) * len(game_ids) * len(ACHIEVEMENT_NAMES)
    achievement_count = min(args.achievements, max(0, capacity - len(taken_keys)))
    achievement_keys = pick_unique(
        rng,
        achievement_count,
        lambda: (rng.choice(player_ids), rng.choice(game_ids), rng.choice(ACHIEVEMENT_NAMES)),
        taken_keys,
        lambda: ((p, g, n) for p in player_ids for g in game_ids for n in ACHIEVEMENT_NAMES),
    )
    first_achievement_id = _max_id(cursor, "achievements") + 1
    for achievement_id, (player_id, game_id, name) in enumerate(
        sorted(achievement_keys), start=first_achievement_id
    ):
        new["achievements"].append(
            make_achievement(rng, achievement_id, player_id, game_id, name, bounds(player_id, game_id))
        )
    insert_table(cursor, "achievements", new["achievements"])

    _sync_sequences(cursor)
    report: dict[str, Any] = {"mode": "append", **table_counts(cursor)}
    if args.window_start is not None:
        report["window_start"] = args.window_start.isoformat()
        report["window_end"] = args.window_end.isoformat()
    report.update({f"appended_{table}": len(rows) for table, rows in new.items()})
    report.update(validate_source(cursor))
    if report["invalid_sessions"] or report["invalid_session_dates"]:
        raise RuntimeError("Source data validation failed after append: " + json.dumps(report))
    return report


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    load_dotenv(ROOT / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is missing. Set it in the project .env file.")

    # One transaction: committed when the block exits normally, rolled back on error.
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            require_source_schema(cursor)
            report = run_initial(cursor, args) if args.mode == "initial" else run_append(cursor, args)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main(sys.argv[1:])
