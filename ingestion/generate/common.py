"""Reference data, row builders and database helpers for the synthetic generator.

`generate_gaming_data` has two modes that share everything in this module:

* `--mode initial` builds a complete, reproducible dataset in empty tables;
* `--mode append` adds new players, games and activity on top of existing data.

Keeping the row builders here guarantees both modes produce rows with exactly the
same shape and business rules (activity never before account creation or game
release, sessions only on supported platforms, ...).
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

import psycopg
from faker import Faker

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
PRICE_POINTS = (0, 4.99, 9.99, 14.99, 19.99, 29.99, 59.99)

# Tables in foreign-key load order, with their insert columns.
COLUMNS: dict[str, tuple[str, ...]] = {
    "players": ("player_id", "username", "country", "birth_year", "created_at", "updated_at"),
    "platforms": ("platform_id", "platform_name", "manufacturer", "created_at", "updated_at"),
    "games": (
        "game_id", "game_name", "developer", "publisher", "release_date", "genre",
        "price", "created_at", "updated_at",
    ),
    "game_platforms": ("game_id", "platform_id", "created_at", "updated_at"),
    "sessions": (
        "session_id", "player_id", "game_id", "platform_id", "started_at", "ended_at",
        "duration_minutes", "updated_at",
    ),
    "purchases": (
        "purchase_id", "player_id", "game_id", "purchase_date", "amount", "currency",
        "payment_method", "updated_at",
    ),
    "achievements": (
        "achievement_id", "player_id", "game_id", "achievement_name", "achieved_at",
        "updated_at",
    ),
    "reviews": ("review_id", "player_id", "game_id", "rating", "review_date", "updated_at"),
}
TABLES = tuple(COLUMNS)

# Row tuples are plain tuples in COLUMNS order.
Row = tuple[Any, ...]


def utc_start_of_day(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


def utc_end_of_day(day: date) -> datetime:
    return datetime.combine(day, time.max, tzinfo=timezone.utc)


def random_datetime(rng: random.Random, start: datetime, end: datetime) -> datetime:
    """Return a random datetime between start and end (inclusive)."""
    if start >= end:
        return start
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, seconds))


# --- Row builders -----------------------------------------------------------
# Each builder consumes random numbers in a fixed order, so a given seed always
# yields the same dataset.


def make_player(
    rng: random.Random,
    fake: Faker,
    player_id: int,
    account_start: datetime,
    account_end: datetime,
) -> Row:
    created_at = random_datetime(rng, account_start, account_end)
    return (
        player_id,
        f"{fake.user_name()}_{player_id}",
        rng.choice(COUNTRIES),
        rng.randint(1960, 2008),
        created_at,
        created_at,
    )


def make_game(
    rng: random.Random,
    fake: Faker,
    game_id: int,
    as_of: date,
    platform_ids: Sequence[int],
    release_date: date | None = None,
) -> tuple[Row, list[Row]]:
    """Return the game row and its game_platforms rows (1-4 supported platforms).

    Without `release_date` the game gets a random release month between 2010
    and `as_of`; hourly append runs pass the run's date so new games are new.
    """
    if release_date is None:
        release_date = date(rng.randint(2010, as_of.year), rng.randint(1, 12), 1)
        if release_date > as_of:
            release_date = as_of
    created_at = utc_start_of_day(release_date)
    price = Decimal(str(rng.choice(PRICE_POINTS)))
    game = (
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
    supported = rng.sample(list(platform_ids), k=rng.randint(1, min(4, len(platform_ids))))
    return game, [(game_id, platform_id, created_at, created_at) for platform_id in supported]


# --- Database helpers -------------------------------------------------------


def chunks(rows: Sequence[Row], size: int = 2_000) -> Iterable[Sequence[Row]]:
    for offset in range(0, len(rows), size):
        yield rows[offset : offset + size]


def insert_rows(
    cursor: psycopg.Cursor[Any],
    table: str,
    columns: Sequence[str],
    rows: Sequence[Row],
    batch_size: int = 2_000,
) -> None:
    """Insert rows in batches with a parameterised INSERT."""
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(columns)
    query = f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"
    for batch in chunks(rows, batch_size):
        cursor.executemany(query, batch)


def insert_table(cursor: psycopg.Cursor[Any], table: str, rows: Sequence[Row]) -> None:
    insert_rows(cursor, table, COLUMNS[table], rows)


def table_counts(cursor: psycopg.Cursor[Any]) -> dict[str, int]:
    counts = {}
    for table in TABLES:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        counts[table] = int(cursor.fetchone()[0])
    return counts


def require_source_schema(cursor: psycopg.Cursor[Any]) -> None:
    for table in TABLES:
        cursor.execute("SELECT to_regclass(%s)", (f"public.{table}",))
        if cursor.fetchone()[0] is None:
            raise SystemExit(
                f"Source table public.{table} is missing. Start postgres-source so its "
                "schema initialization scripts run."
            )
