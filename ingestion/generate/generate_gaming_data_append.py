"""
Append synthetic gaming data to the existing PostgreSQL source database.

This script NEVER truncates or deletes existing data.
It generates new players/games and appends new activity records.
"""

from __future__ import annotations

import argparse
import json
import os
import random
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import psycopg
from dotenv import load_dotenv
from faker import Faker


# Төслийн үндсэн .env файлыг анхны скрипттэй адил байрлалаас уншина.
ROOT = Path(__file__).resolve().parents[2]

BASE_DATE = date(2023, 1, 1)
ACCOUNT_START = date(2022, 1, 1)

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

TABLES = (
    "players",
    "platforms",
    "games",
    "game_platforms",
    "sessions",
    "purchases",
    "achievements",
    "reviews",
)

ID_COLUMNS = {
    "players": "player_id",
    "games": "game_id",
    "sessions": "session_id",
    "purchases": "purchase_id",
    "achievements": "achievement_id",
    "reviews": "review_id",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append synthetic data without deleting existing records."
    )

    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())

    parser.add_argument("--players", type=int, default=1000)
    parser.add_argument("--games", type=int, default=100)
    parser.add_argument("--sessions", type=int, default=10000)
    parser.add_argument("--purchases", type=int, default=3000)
    parser.add_argument("--reviews", type=int, default=2000)
    parser.add_argument("--achievements", type=int, default=2000)

    args = parser.parse_args()

    if args.as_of < BASE_DATE:
        parser.error(f"--as-of must be on or after {BASE_DATE.isoformat()}")

    for name in (
        "players",
        "games",
        "sessions",
        "purchases",
        "reviews",
        "achievements",
    ):
        if getattr(args, name) < 0:
            parser.error(f"--{name} must be zero or greater")

    return args


def random_datetime(
    rng: random.Random,
    start: datetime,
    end: datetime,
) -> datetime:
    """Return a random timezone-aware datetime between start and end."""
    if start >= end:
        return start

    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, seconds))


def insert_rows(
    cursor: psycopg.Cursor[Any],
    table: str,
    columns: Sequence[str],
    rows: Sequence[tuple[Any, ...]],
    batch_size: int = 2000,
) -> None:
    """Insert rows in batches."""
    if not rows:
        return

    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(columns)
    query = f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"

    for offset in range(0, len(rows), batch_size):
        cursor.executemany(query, rows[offset : offset + batch_size])


def get_max_id(cursor: psycopg.Cursor[Any], table: str, id_column: str) -> int:
    cursor.execute(f"SELECT COALESCE(MAX({id_column}), 0) FROM {table}")
    return int(cursor.fetchone()[0])


def get_table_count(cursor: psycopg.Cursor[Any], table: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    return int(cursor.fetchone()[0])


def sync_sequence(
    cursor: psycopg.Cursor[Any],
    table: str,
    id_column: str,
) -> None:
    """
    Synchronize a serial/identity sequence when the column has one.
    Does nothing if PostgreSQL has no sequence associated with the column.
    """
    cursor.execute(
        "SELECT pg_get_serial_sequence(%s, %s)",
        (f"public.{table}", id_column),
    )
    sequence_name = cursor.fetchone()[0]

    if sequence_name:
        cursor.execute(f"SELECT COALESCE(MAX({id_column}), 0) FROM {table}")
        max_id = int(cursor.fetchone()[0])

        if max_id > 0:
            cursor.execute(
                "SELECT setval(%s, %s, true)",
                (sequence_name, max_id),
            )


def main() -> None:
    args = parse_args()

    rng = random.Random(args.seed)
    fake = Faker("en_US")
    fake.seed_instance(args.seed)

    load_dotenv(ROOT / ".env")
    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        raise SystemExit(
            "DATABASE_URL is missing. Check the project's .env file."
        )

    activity_start = datetime.combine(
        BASE_DATE, time.min, tzinfo=timezone.utc
    )
    end_of_day = datetime.combine(
        args.as_of, time.max, tzinfo=timezone.utc
    )
    account_start = datetime.combine(
        ACCOUNT_START, time.min, tzinfo=timezone.utc
    )
    account_end = datetime.combine(
        args.as_of, time.min, tzinfo=timezone.utc
    )

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:

            # ---------------------------------------------------------
            # 1. Шаардлагатай schema / хүснэгтүүд байгаа эсэхийг шалгах
            # ---------------------------------------------------------
            for table in TABLES:
                cursor.execute("SELECT to_regclass(%s)", (f"public.{table}",))
                if cursor.fetchone()[0] is None:
                    raise SystemExit(
                        f"Missing table public.{table}. "
                        "Start postgres-source and run its schema initialization first."
                    )

            # ---------------------------------------------------------
            # 2. Одоогийн өгөгдлийг унших; юу ч устгахгүй
            # ---------------------------------------------------------
            cursor.execute(
                """
                SELECT player_id, created_at
                FROM players
                ORDER BY player_id
                """
            )
            existing_players = {
                int(player_id): created_at
                for player_id, created_at in cursor.fetchall()
            }

            cursor.execute(
                """
                SELECT game_id, release_date, price
                FROM games
                ORDER BY game_id
                """
            )
            existing_games = {
                int(game_id): {
                    "release_date": release_date,
                    "price": Decimal(str(price)),
                }
                for game_id, release_date, price in cursor.fetchall()
            }

            cursor.execute(
                "SELECT game_id, platform_id FROM game_platforms"
            )
            existing_game_platforms: dict[int, list[int]] = {}
            for game_id, platform_id in cursor.fetchall():
                existing_game_platforms.setdefault(int(game_id), []).append(
                    int(platform_id)
                )

            # ---------------------------------------------------------
            # 3. Platform-уудыг шалгах / дутуу seed platform-уудыг нэмэх
            # ---------------------------------------------------------
            cursor.execute("SELECT platform_id, platform_name FROM platforms")
            platform_ids_by_name = {
                platform_name: int(platform_id)
                for platform_id, platform_name in cursor.fetchall()
            }

            next_platform_id = get_max_id(cursor, "platforms", "platform_id") + 1
            platform_rows = []

            for platform_name, manufacturer in PLATFORM_SEED:
                if platform_name not in platform_ids_by_name:
                    platform_id = next_platform_id
                    next_platform_id += 1
                    created_at = activity_start

                    platform_rows.append(
                        (
                            platform_id,
                            platform_name,
                            manufacturer,
                            created_at,
                            created_at,
                        )
                    )
                    platform_ids_by_name[platform_name] = platform_id

            insert_rows(
                cursor,
                "platforms",
                (
                    "platform_id",
                    "platform_name",
                    "manufacturer",
                    "created_at",
                    "updated_at",
                ),
                platform_rows,
            )

            available_platform_ids = list(platform_ids_by_name.values())

            # ---------------------------------------------------------
            # 4. Шинэ players нэмэх; ID-г MAX(player_id) + 1-ээс эхлүүлэх
            # ---------------------------------------------------------
            next_player_id = get_max_id(cursor, "players", "player_id") + 1
            new_player_rows = []

            for player_id in range(
                next_player_id, next_player_id + args.players
            ):
                created_at = random_datetime(
                    rng, account_start, account_end
                )
                existing_players[player_id] = created_at

                new_player_rows.append(
                    (
                        player_id,
                        f"{fake.user_name()}_{player_id}",
                        rng.choice(COUNTRIES),
                        rng.randint(1960, 2008),
                        created_at,
                        created_at,
                    )
                )

            insert_rows(
                cursor,
                "players",
                (
                    "player_id",
                    "username",
                    "country",
                    "birth_year",
                    "created_at",
                    "updated_at",
                ),
                new_player_rows,
            )

            # ---------------------------------------------------------
            # 5. Шинэ games болон тэдгээрийн platform холбоосыг нэмэх
            # ---------------------------------------------------------
            next_game_id = get_max_id(cursor, "games", "game_id") + 1
            new_game_rows = []
            new_game_platform_rows = []

            platform_ids = list(available_platform_ids)

            for game_id in range(next_game_id, next_game_id + args.games):
                release_date = date(
                    rng.randint(2010, args.as_of.year),
                    rng.randint(1, 12),
                    1,
                )
                if release_date > args.as_of:
                    release_date = args.as_of

                created_at = datetime.combine(
                    release_date, time.min, tzinfo=timezone.utc
                )
                price = Decimal(
                    str(rng.choice((0, 4.99, 9.99, 14.99, 19.99, 29.99, 59.99)))
                )

                existing_games[game_id] = {
                    "release_date": release_date,
                    "price": price,
                }

                new_game_rows.append(
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
                    platform_ids,
                    k=rng.randint(1, min(4, len(platform_ids))),
                )
                existing_game_platforms[game_id] = supported_platforms

                new_game_platform_rows.extend(
                    (game_id, platform_id, created_at, created_at)
                    for platform_id in supported_platforms
                )

            insert_rows(
                cursor,
                "games",
                (
                    "game_id",
                    "game_name",
                    "developer",
                    "publisher",
                    "release_date",
                    "genre",
                    "price",
                    "created_at",
                    "updated_at",
                ),
                new_game_rows,
            )

            insert_rows(
                cursor,
                "game_platforms",
                ("game_id", "platform_id", "created_at", "updated_at"),
                new_game_platform_rows,
            )

            # ---------------------------------------------------------
            # 6. Бүх хуучин + шинэ player/game жагсаалтыг бэлтгэх
            # ---------------------------------------------------------
            all_player_ids = list(existing_players.keys())
            all_game_ids = list(existing_games.keys())

            if (
                (args.sessions or args.purchases or args.reviews or args.achievements)
                and (not all_player_ids or not all_game_ids)
            ):
                raise RuntimeError(
                    "Cannot create activity data: no players or games exist. "
                    "Add players/games or run the initial data generator first."
                )

            eligible_game_ids = [
                game_id
                for game_id in all_game_ids
                if existing_game_platforms.get(game_id)
            ]

            if args.sessions and not eligible_game_ids:
                raise RuntimeError(
                    "No games have platform mappings, so sessions cannot be generated."
                )

            def activity_bounds(
                player_id: int, game_id: int
            ) -> tuple[datetime, datetime]:
                player_created_at = existing_players[player_id]
                release_date = existing_games[game_id]["release_date"]
                release_at = datetime.combine(
                    release_date, time.min, tzinfo=timezone.utc
                )

                start = max(activity_start, player_created_at, release_at)
                if start >= end_of_day:
                    start = end_of_day - timedelta(seconds=1)

                return start, end_of_day

            # ---------------------------------------------------------
            # 7. Sessions: бүх хуучин болон шинэ тоглоомоос сонгоно
            # ---------------------------------------------------------
            next_session_id = get_max_id(cursor, "sessions", "session_id") + 1
            session_rows = []

            for session_id in range(
                next_session_id, next_session_id + args.sessions
            ):
                player_id = rng.choice(all_player_ids)
                game_id = rng.choice(eligible_game_ids)
                platform_id = rng.choice(existing_game_platforms[game_id])

                start, end = activity_bounds(player_id, game_id)
                started_at = random_datetime(rng, start, end)
                duration = rng.randint(5, 360)
                ended_at = started_at + timedelta(minutes=duration)

                session_rows.append(
                    (
                        session_id,
                        player_id,
                        game_id,
                        platform_id,
                        started_at,
                        ended_at,
                        duration,
                        started_at,
                    )
                )

            insert_rows(
                cursor,
                "sessions",
                (
                    "session_id",
                    "player_id",
                    "game_id",
                    "platform_id",
                    "started_at",
                    "ended_at",
                    "duration_minutes",
                    "updated_at",
                ),
                session_rows,
            )

            # ---------------------------------------------------------
            # 8. Purchases: бүх хуучин болон шинэ тоглоомоос сонгоно
            # ---------------------------------------------------------
            next_purchase_id = get_max_id(
                cursor, "purchases", "purchase_id"
            ) + 1
            purchase_rows = []

            for purchase_id in range(
                next_purchase_id, next_purchase_id + args.purchases
            ):
                player_id = rng.choice(all_player_ids)
                game_id = rng.choice(all_game_ids)

                start, end = activity_bounds(player_id, game_id)
                purchased_at = random_datetime(rng, start, end)
                price = existing_games[game_id]["price"]

                amount = max(
                    Decimal("0.99"),
                    price * Decimal(str(rng.uniform(0.5, 1.0))),
                ).quantize(Decimal("0.01"))

                purchase_rows.append(
                    (
                        purchase_id,
                        player_id,
                        game_id,
                        purchased_at,
                        amount,
                        rng.choice(CURRENCIES),
                        rng.choice(PAYMENT_METHODS),
                        purchased_at,
                    )
                )

            insert_rows(
                cursor,
                "purchases",
                (
                    "purchase_id",
                    "player_id",
                    "game_id",
                    "purchase_date",
                    "amount",
                    "currency",
                    "payment_method",
                    "updated_at",
                ),
                purchase_rows,
            )

            # ---------------------------------------------------------
            # 9. Reviews: нэг player/game хос дээр давхардсан review үүсгэхгүй
            # ---------------------------------------------------------
            cursor.execute("SELECT player_id, game_id FROM reviews")
            existing_review_pairs = {
                (int(player_id), int(game_id))
                for player_id, game_id in cursor.fetchall()
            }

            total_possible_pairs = len(all_player_ids) * len(all_game_ids)
            available_review_slots = total_possible_pairs - len(
                existing_review_pairs
            )
            review_count = min(args.reviews, max(0, available_review_slots))

            review_pairs: set[tuple[int, int]] = set()
            attempts = 0
            max_attempts = max(1000, review_count * 20)

            while (
                len(review_pairs) < review_count
                and attempts < max_attempts
            ):
                pair = (rng.choice(all_player_ids), rng.choice(all_game_ids))
                if (
                    pair not in existing_review_pairs
                    and pair not in review_pairs
                ):
                    review_pairs.add(pair)
                attempts += 1

            # Санамсаргүй оролдлогоор хангалттай хос олдохгүй бол
            # боломжит хосуудыг дарааллаар нь хайна.
            if len(review_pairs) < review_count:
                for player_id in all_player_ids:
                    for game_id in all_game_ids:
                        pair = (player_id, game_id)
                        if (
                            pair not in existing_review_pairs
                            and pair not in review_pairs
                        ):
                            review_pairs.add(pair)
                            if len(review_pairs) >= review_count:
                                break
                    if len(review_pairs) >= review_count:
                        break

            next_review_id = get_max_id(cursor, "reviews", "review_id") + 1
            review_rows = []

            for review_id, (player_id, game_id) in enumerate(
                sorted(review_pairs),
                start=next_review_id,
            ):
                start, end = activity_bounds(player_id, game_id)
                review_date = random_datetime(rng, start, end)

                review_rows.append(
                    (
                        review_id,
                        player_id,
                        game_id,
                        rng.randint(1, 5),
                        review_date,
                        review_date,
                    )
                )

            insert_rows(
                cursor,
                "reviews",
                (
                    "review_id",
                    "player_id",
                    "game_id",
                    "rating",
                    "review_date",
                    "updated_at",
                ),
                review_rows,
            )

            # ---------------------------------------------------------
            # 10. Achievements: player/game/name давхардлыг хориглох
            # ---------------------------------------------------------
            cursor.execute(
                """
                SELECT player_id, game_id, achievement_name
                FROM achievements
                """
            )
            existing_achievement_keys = {
                (int(player_id), int(game_id), achievement_name)
                for player_id, game_id, achievement_name in cursor.fetchall()
            }

            possible_achievement_keys = (
                len(all_player_ids)
                * len(all_game_ids)
                * len(ACHIEVEMENT_NAMES)
            )
            available_achievement_slots = (
                possible_achievement_keys - len(existing_achievement_keys)
            )
            achievement_count = min(
                args.achievements,
                max(0, available_achievement_slots),
            )

            achievement_keys: set[tuple[int, int, str]] = set()
            attempts = 0
            max_attempts = max(1000, achievement_count * 20)

            while (
                len(achievement_keys) < achievement_count
                and attempts < max_attempts
            ):
                key = (
                    rng.choice(all_player_ids),
                    rng.choice(all_game_ids),
                    rng.choice(ACHIEVEMENT_NAMES),
                )
                if (
                    key not in existing_achievement_keys
                    and key not in achievement_keys
                ):
                    achievement_keys.add(key)
                attempts += 1

            if len(achievement_keys) < achievement_count:
                for player_id in all_player_ids:
                    for game_id in all_game_ids:
                        for achievement_name in ACHIEVEMENT_NAMES:
                            key = (player_id, game_id, achievement_name)
                            if (
                                key not in existing_achievement_keys
                                and key not in achievement_keys
                            ):
                                achievement_keys.add(key)
                                if len(achievement_keys) >= achievement_count:
                                    break
                        if len(achievement_keys) >= achievement_count:
                            break
                    if len(achievement_keys) >= achievement_count:
                        break

            next_achievement_id = get_max_id(
                cursor, "achievements", "achievement_id"
            ) + 1
            achievement_rows = []

            for achievement_id, (player_id, game_id, achievement_name) in enumerate(
                sorted(achievement_keys),
                start=next_achievement_id,
            ):
                start, end = activity_bounds(player_id, game_id)
                achieved_at = random_datetime(rng, start, end)

                achievement_rows.append(
                    (
                        achievement_id,
                        player_id,
                        game_id,
                        achievement_name,
                        achieved_at,
                        achieved_at,
                    )
                )

            insert_rows(
                cursor,
                "achievements",
                (
                    "achievement_id",
                    "player_id",
                    "game_id",
                    "achievement_name",
                    "achieved_at",
                    "updated_at",
                ),
                achievement_rows,
            )

            # ---------------------------------------------------------
            # 11. Sequence ашигладаг хүснэгтүүдийн sequence-ийг тохируулах
            # ---------------------------------------------------------
            for table, id_column in ID_COLUMNS.items():
                sync_sequence(cursor, table, id_column)

            # ---------------------------------------------------------
            # 12. Эцсийн хүснэгтийн нийт мөрийн тоог харуулах
            # ---------------------------------------------------------
            report = {
                table: get_table_count(cursor, table)
                for table in TABLES
            }

            report["appended_players"] = len(new_player_rows)
            report["appended_games"] = len(new_game_rows)
            report["appended_game_platforms"] = len(new_game_platform_rows)
            report["appended_sessions"] = len(session_rows)
            report["appended_purchases"] = len(purchase_rows)
            report["appended_reviews"] = len(review_rows)
            report["appended_achievements"] = len(achievement_rows)

    # psycopg connection context амжилттай дуусахад transaction commit хийгдэнэ.
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()