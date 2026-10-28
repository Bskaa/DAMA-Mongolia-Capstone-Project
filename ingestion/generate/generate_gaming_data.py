"""Generate reproducible synthetic data for the gaming source database."""

from __future__ import annotations

import argparse
import json
import os
import random
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

import psycopg
from dotenv import load_dotenv
from faker import Faker

ROOT = Path(__file__).resolve().parents[2]
TABLES = (
    "game_platforms",
    "sessions",
    "purchases",
    "achievements",
    "reviews",
    "games",
    "platforms",
    "players",
)
BASE_DATE = date(2023, 1, 1)
PLATFORM_SEED = (
    ("PC", "Various"),
    ("PlayStation 5", "Sony"),
    ("PlayStation 4", "Sony"),
    ("Xbox Series X|S", "Microsoft"),
    ("Xbox One", "Microsoft"),
    ("Nintendo Switch", "Nintendo"),
    ("iOS", "Apple"),
    ("Android", "Google"),
)
GENRES = (
    "Action",
    "Adventure",
    "Casual",
    "Indie",
    "RPG",
    "Simulation",
    "Sports",
    "Strategy",
)
ACHIEVEMENT_NAMES = (
    "First Steps",
    "Explorer",
    "Collector",
    "Sharpshooter",
    "Speed Runner",
    "Team Player",
    "Survivor",
    "Completionist",
)
COUNTRIES = ("US", "CA", "GB", "DE", "FR", "JP", "KR", "AU", "BR", "MN")
PAYMENT_METHODS = ("card", "wallet", "gift_card", "mobile")
CURRENCIES = ("USD", "EUR", "GBP", "JPY", "CAD")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date(2025, 12, 31))
    parser.add_argument("--players", type=int, default=10_000)
    parser.add_argument("--games", type=int, default=1_000)
    parser.add_argument("--sessions", type=int, default=100_000)
    parser.add_argument("--purchases", type=int, default=30_000)
    parser.add_argument("--reviews", type=int, default=20_000)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete existing source rows before loading the generated dataset.",
    )
    args = parser.parse_args()
    if args.as_of < BASE_DATE:
        parser.error(f"--as-of must be on or after {BASE_DATE.isoformat()}")
    for name in ("players", "games", "sessions", "purchases", "reviews"):
        if getattr(args, name) < 1:
            parser.error(f"--{name} must be at least 1")
    if args.reviews > args.players * args.games:
        parser.error("--reviews cannot exceed the number of unique player/game pairs")
    return args


def random_datetime(
    rng: random.Random, start: datetime, end: datetime
) -> datetime:
    if start >= end:
        return start
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, seconds))


def chunks(rows: Sequence[tuple[Any, ...]], size: int = 2_000) -> Iterable[Sequence[tuple[Any, ...]]]:
    for offset in range(0, len(rows), size):
        yield rows[offset : offset + size]


def insert_rows(
    cursor: psycopg.Cursor[Any],
    table: str,
    columns: Sequence[str],
    rows: Sequence[tuple[Any, ...]],
) -> None:
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(columns)
    query = f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"
    for batch in chunks(rows):
        cursor.executemany(query, batch)


def build_dataset(args: argparse.Namespace) -> dict[str, list[tuple[Any, ...]]]:
    rng = random.Random(args.seed)
    fake = Faker("en_US")
    fake.seed_instance(args.seed)
    end_of_day = datetime.combine(args.as_of, time.max, tzinfo=timezone.utc)
    activity_start = datetime.combine(BASE_DATE, time.min, tzinfo=timezone.utc)
    account_start = datetime.combine(date(2022, 1, 1), time.min, tzinfo=timezone.utc)
    account_end = datetime.combine(args.as_of, time.min, tzinfo=timezone.utc)
    data: dict[str, list[tuple[Any, ...]]] = {table: [] for table in TABLES}

    players: dict[int, datetime] = {}
    for player_id in range(1, args.players + 1):
        created_at = random_datetime(rng, account_start, account_end)
        players[player_id] = created_at
        data["players"].append(
            (
                player_id,
                f"{fake.user_name()}_{player_id}",
                rng.choice(COUNTRIES),
                rng.randint(1960, 2008),
                created_at,
                created_at,
            )
        )

    for platform_id, (name, manufacturer) in enumerate(PLATFORM_SEED, start=1):
        data["platforms"].append(
            (platform_id, name, manufacturer, activity_start, activity_start)
        )

    game_release_dates: dict[int, date] = {}
    game_prices: dict[int, Decimal] = {}
    game_platforms: dict[int, list[int]] = {}
    for game_id in range(1, args.games + 1):
        release_date = date(
            rng.randint(2010, args.as_of.year),
            rng.randint(1, 12),
            1,
        )
        if release_date > args.as_of:
            release_date = args.as_of
        game_release_dates[game_id] = release_date
        created_at = datetime.combine(release_date, time.min, tzinfo=timezone.utc)
        price = Decimal(str(rng.choice((0, 4.99, 9.99, 14.99, 19.99, 29.99, 59.99))))
        game_prices[game_id] = price
        data["games"].append(
            (
                game_id,
                f"{fake.word().title()} {fake.word().title()} {game_id:04d}",
                fake.company(),
                fake.company(),
                release_date,
                rng.choice(GENRES),
                price,
                created_at,
                created_at,
            )
        )
        supported_platforms = rng.sample(
            range(1, len(PLATFORM_SEED) + 1), k=rng.randint(1, 4)
        )
        game_platforms[game_id] = supported_platforms
        data["game_platforms"].extend(
            (game_id, platform_id, created_at, created_at)
            for platform_id in supported_platforms
        )

    def activity_bounds(player_id: int, game_id: int) -> tuple[datetime, datetime]:
        released_at = datetime.combine(
            game_release_dates[game_id], time.min, tzinfo=timezone.utc
        )
        start = max(activity_start, players[player_id], released_at)
        if start >= end_of_day:
            start = end_of_day - timedelta(seconds=1)
        return start, end_of_day

    for session_id in range(1, args.sessions + 1):
        player_id = rng.randint(1, args.players)
        game_id = rng.randint(1, args.games)
        platform_id = rng.choice(game_platforms[game_id])
        start, end = activity_bounds(player_id, game_id)
        started_at = random_datetime(rng, start, end)
        duration = rng.randint(5, 360)
        data["sessions"].append(
            (
                session_id,
                player_id,
                game_id,
                platform_id,
                started_at,
                started_at + timedelta(minutes=duration),
                duration,
                started_at,
            )
        )

    for purchase_id in range(1, args.purchases + 1):
        player_id = rng.randint(1, args.players)
        game_id = rng.randint(1, args.games)
        start, end = activity_bounds(player_id, game_id)
        price = game_prices[game_id]
        amount = max(Decimal("0.99"), price * Decimal(str(rng.uniform(0.5, 1.0))))
        purchased_at = random_datetime(rng, start, end)
        data["purchases"].append(
            (
                purchase_id,
                player_id,
                game_id,
                purchased_at,
                amount.quantize(Decimal("0.01")),
                rng.choice(CURRENCIES),
                rng.choice(PAYMENT_METHODS),
                purchased_at,
            )
        )

    review_pairs: set[tuple[int, int]] = set()
    while len(review_pairs) < args.reviews:
        review_pairs.add((rng.randint(1, args.players), rng.randint(1, args.games)))
    for review_id, (player_id, game_id) in enumerate(sorted(review_pairs), start=1):
        start, end = activity_bounds(player_id, game_id)
        review_date = random_datetime(rng, start, end)
        data["reviews"].append(
            (
                review_id,
                player_id,
                game_id,
                rng.randint(1, 5),
                review_date,
                review_date,
            )
        )

    achievement_count = min(args.sessions // 5, args.players * args.games)
    achievement_pairs: set[tuple[int, int, str]] = set()
    while len(achievement_pairs) < achievement_count:
        achievement_pairs.add(
            (
                rng.randint(1, args.players),
                rng.randint(1, args.games),
                rng.choice(ACHIEVEMENT_NAMES),
            )
        )
    for achievement_id, (player_id, game_id, achievement_name) in enumerate(
        sorted(achievement_pairs), start=1
    ):
        start, end = activity_bounds(player_id, game_id)
        achieved_at = random_datetime(rng, start, end)
        data["achievements"].append(
            (
                achievement_id,
                player_id,
                game_id,
                achievement_name,
                achieved_at,
                achieved_at,
            )
        )
    return data


def main() -> None:
    args = parse_args()
    load_dotenv(ROOT / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is missing. Set it in the project .env file.")

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('public.players')")
            if cursor.fetchone()[0] is None:
                raise SystemExit(
                    "Source schema is missing. Start postgres-source so its init script runs."
                )
            cursor.execute(
                "SELECT COUNT(*) FROM players UNION ALL "
                "SELECT COUNT(*) FROM games UNION ALL "
                "SELECT COUNT(*) FROM sessions"
            )
            existing = [count for (count,) in cursor.fetchall()]
            if any(existing) and not args.reset:
                raise SystemExit(
                    "Source tables already contain data. Use --reset to replace the dataset."
                )
            if args.reset:
                cursor.execute(
                    "TRUNCATE TABLE " + ", ".join(TABLES) + " RESTART IDENTITY CASCADE"
                )
            dataset = build_dataset(args)

            column_map = {
                "players": (
                    "player_id, username, country, birth_year, created_at, updated_at"
                ),
                "platforms": (
                    "platform_id, platform_name, manufacturer, created_at, updated_at"
                ),
                "games": (
                    "game_id, game_name, developer, publisher, release_date, genre, "
                    "price, created_at, updated_at"
                ),
                "game_platforms": "game_id, platform_id, created_at, updated_at",
                "sessions": (
                    "session_id, player_id, game_id, platform_id, started_at, "
                    "ended_at, duration_minutes, updated_at"
                ),
                "purchases": (
                    "purchase_id, player_id, game_id, purchase_date, amount, "
                    "currency, payment_method, updated_at"
                ),
                "achievements": (
                    "achievement_id, player_id, game_id, achievement_name, achieved_at, "
                    "updated_at"
                ),
                "reviews": (
                    "review_id, player_id, game_id, rating, review_date, updated_at"
                ),
            }
            for table in (
                "players",
                "platforms",
                "games",
                "game_platforms",
                "sessions",
                "purchases",
                "achievements",
                "reviews",
            ):
                insert_rows(
                    cursor,
                    table,
                    column_map[table].split(", "),
                    dataset[table],
                )

            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM players) AS players,
                    (SELECT COUNT(*) FROM games) AS games,
                    (SELECT COUNT(*) FROM platforms) AS platforms,
                    (SELECT COUNT(*) FROM game_platforms) AS game_platforms,
                    (SELECT COUNT(*) FROM sessions) AS sessions,
                    (SELECT COUNT(*) FROM purchases) AS purchases,
                    (SELECT COUNT(*) FROM achievements) AS achievements,
                    (SELECT COUNT(*) FROM reviews) AS reviews,
                    (SELECT COUNT(*) FROM sessions s
                     JOIN game_platforms gp USING (game_id, platform_id)
                     WHERE s.ended_at <= s.started_at
                        OR s.duration_minutes !=
                           EXTRACT(EPOCH FROM (s.ended_at - s.started_at)) / 60
                    ) AS invalid_sessions,
                    (SELECT COUNT(*) FROM sessions s
                     JOIN players p USING (player_id)
                     JOIN games g USING (game_id)
                     WHERE s.started_at < p.created_at
                        OR s.started_at::date < g.release_date
                    ) AS invalid_session_dates
                """
            )
            report = dict(zip((column.name for column in cursor.description), cursor.fetchone()))
            expected_counts = {
                "players": args.players,
                "games": args.games,
                "platforms": len(PLATFORM_SEED),
                "game_platforms": len(dataset["game_platforms"]),
                "sessions": args.sessions,
                "purchases": args.purchases,
                "achievements": min(args.sessions // 5, args.players * args.games),
                "reviews": args.reviews,
            }
            mismatched = {
                table: (report[table], expected)
                for table, expected in expected_counts.items()
                if report[table] != expected
            }
            if mismatched or report["invalid_sessions"] or report["invalid_session_dates"]:
                raise RuntimeError(
                    "Source data validation failed: "
                    + json.dumps(
                        {
                            "mismatched_counts": mismatched,
                            "invalid_sessions": report["invalid_sessions"],
                            "invalid_session_dates": report["invalid_session_dates"],
                        }
                    )
                )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
