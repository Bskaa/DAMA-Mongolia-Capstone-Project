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

Behaviour (ingestion/generate/behavior.py): players churn, a few games are hits,
sessions follow the Ulaanbaatar day, campaigns in data/business/marketing_campaigns.xlsx
lift the promoted game, purchases happen inside sessions in the player's local
currency, and reviews/achievements come from player/game pairs that really played.

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
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import psycopg
from dotenv import load_dotenv
from faker import Faker

from ingestion.generate import behavior
from ingestion.generate.common import (
    ACCOUNT_START,
    ACHIEVEMENT_NAMES,
    BASE_DATE,
    PAYMENT_METHODS,
    PLATFORM_SEED,
    ROOT,
    TABLES,
    Row,
    insert_table,
    make_game,
    make_player,
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
        "--campaigns",
        type=Path,
        default=behavior.CAMPAIGNS_WORKBOOK,
        help="Marketing workbook whose campaigns lift the promoted games (default: "
        "data/business/marketing_campaigns.xlsx; ignored if missing).",
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


# --- activity simulation (shared by both modes) ------------------------------------


def simulate_activity(
    rng: random.Random,
    *,
    players: dict[int, tuple[datetime, str]],
    games: dict[int, tuple[date, Decimal]],
    game_platforms: dict[int, list[int]],
    campaigns: Sequence[behavior.Campaign],
    period_start: datetime,
    period_end: datetime,
    counts: dict[str, int],
    first_ids: dict[str, int],
    taken_review_pairs: set[tuple[int, int]],
    taken_achievements: set[tuple[int, int, str]],
) -> dict[str, list[Row]]:
    """Sessions, purchases, reviews and achievements for one period.

    `players` maps id -> (created_at, country); `games` maps id -> (release_date, price_usd).
    """
    out: dict[str, list[Row]] = {"sessions": [], "purchases": [], "reviews": [], "achievements": []}
    rates = behavior.usd_rates()
    shaped = (period_end - period_start) > timedelta(days=2)

    # Who can play in this period: account exists and has not churned yet.
    windows: dict[int, tuple[datetime, datetime]] = {}
    for player_id, (created_at, _) in players.items():
        start = max(period_start, created_at)
        end = min(period_end, behavior.churn_at(created_at, behavior.player_traits(player_id)))
        if end > start:
            windows[player_id] = (start, end)
    if not windows:  # everybody churned: returning players keep a small trickle alive
        windows = {
            player_id: (max(period_start, created_at), period_end)
            for player_id, (created_at, _) in players.items()
            if created_at < period_end
        }
    if not windows or counts["sessions"] == 0:
        return out
    player_ids = sorted(windows)
    player_picker = behavior.WeightedPicker(
        player_ids,
        (behavior.player_traits(p).activity * (windows[p][1] - windows[p][0]).total_seconds()
         for p in player_ids),
    )
    playable = sorted(game_id for game_id in games if game_platforms.get(game_id))
    if not playable:
        return out
    popularity = {g: behavior.game_traits(g).popularity for g in playable}
    game_picker = behavior.WeightedPicker(playable, (popularity[g] for g in playable))
    playable_set = set(playable)
    period_campaigns = [
        campaign for campaign in campaigns
        if campaign.game_id in playable_set
        and campaign.start <= period_end.date() and campaign.end >= period_start.date()
    ]

    # Sessions
    pair_sessions: dict[tuple[int, int], list[tuple[datetime, datetime]]] = {}
    for session_id in range(first_ids["sessions"], first_ids["sessions"] + counts["sessions"]):
        for _ in range(20):
            player_id = player_picker.pick(rng)
            started_at = behavior.activity_time(rng, *windows[player_id], shaped=shaped)
            day = started_at.date()
            # A campaign multiplies its game's normal share by CAMPAIGN_BOOST: route
            # the extra (BOOST - 1) x share of sessions to the promoted games.
            active = [c.game_id for c in period_campaigns if c.start <= day <= c.end]
            extra = [(behavior.CAMPAIGN_BOOST - 1) * popularity[g] / game_picker.total for g in active]
            if active and rng.random() < sum(extra):
                game_id = rng.choices(active, weights=extra)[0]
            else:
                game_id = game_picker.pick(rng)
            if games[game_id][0] <= day:
                break
        else:
            continue  # no released game fits this moment; skip rather than break rules
        minutes = behavior.session_minutes(rng)
        ended_at = started_at + timedelta(minutes=minutes)
        out["sessions"].append(
            (session_id, player_id, game_id, rng.choice(game_platforms[game_id]),
             started_at, ended_at, minutes, started_at)
        )
        pair_sessions.setdefault((player_id, game_id), []).append((started_at, ended_at))

    sessions = out["sessions"]
    if not sessions:
        return out

    # Purchases happen inside sessions; spenders (and whales) buy far more often.
    session_picker = behavior.WeightedPicker(
        range(len(sessions)), (behavior.player_traits(row[1]).spend for row in sessions)
    )
    for purchase_id in range(first_ids["purchases"], first_ids["purchases"] + counts["purchases"]):
        _, player_id, game_id, _, started_at, ended_at, _, _ = sessions[session_picker.pick(rng)]
        purchased_at = min(behavior.activity_time(rng, started_at, ended_at, shaped=False), period_end)
        currency = behavior.COUNTRY_CURRENCY.get(players[player_id][1], "USD")
        amount = behavior.purchase_amount(rng, games[game_id][1], currency, rates)
        out["purchases"].append(
            (purchase_id, player_id, game_id, purchased_at, amount, currency,
             rng.choice(PAYMENT_METHODS), purchased_at)
        )

    # Reviews: one per player/game pair that played; rating follows the game's quality.
    candidates = sorted(pair for pair in pair_sessions if pair not in taken_review_pairs)
    chosen = rng.sample(candidates, k=min(counts["reviews"], len(candidates)))
    for review_id, (player_id, game_id) in enumerate(sorted(chosen), start=first_ids["reviews"]):
        first_start = min(start for start, _ in pair_sessions[(player_id, game_id)])
        latest = min(period_end, windows[player_id][1] + timedelta(days=7))
        review_date = behavior.activity_time(rng, first_start, max(first_start, latest), shaped=False)
        rating = behavior.review_rating(rng, behavior.game_traits(game_id).quality)
        out["reviews"].append((review_id, player_id, game_id, rating, review_date, review_date))

    # Achievements: earned during a session of a pair that played.
    pairs = sorted(pair_sessions)
    keys: set[tuple[int, int, str]] = set()
    already = sum(1 for p, g, _ in taken_achievements if (p, g) in pair_sessions)
    wanted = min(counts["achievements"], max(0, len(pairs) * len(ACHIEVEMENT_NAMES) - already))
    attempts = 0
    while len(keys) < wanted and attempts < wanted * 30:
        attempts += 1
        player_id, game_id = rng.choice(pairs)
        key = (player_id, game_id, rng.choice(ACHIEVEMENT_NAMES))
        if key not in taken_achievements:
            keys.add(key)
    for achievement_id, (player_id, game_id, name) in enumerate(sorted(keys), start=first_ids["achievements"]):
        started_at, ended_at = rng.choice(pair_sessions[(player_id, game_id)])
        achieved_at = min(behavior.activity_time(rng, started_at, ended_at, shaped=False), period_end)
        out["achievements"].append((achievement_id, player_id, game_id, name, achieved_at, achieved_at))
    return out


# --- initial mode -----------------------------------------------------------


def build_initial_dataset(args: argparse.Namespace) -> dict[str, list[Row]]:
    """Build a complete dataset in memory; deterministic for a given seed and campaigns file."""
    rng = random.Random(args.seed)
    fake = Faker("en_US")
    fake.seed_instance(args.seed)
    activity_start = utc_start_of_day(BASE_DATE)
    end_of_day = utc_end_of_day(args.as_of)
    account_start = utc_start_of_day(ACCOUNT_START)
    account_end = utc_start_of_day(args.as_of)
    data: dict[str, list[Row]] = {table: [] for table in TABLES}

    players: dict[int, tuple[datetime, str]] = {}
    for player_id in range(1, args.players + 1):
        row = make_player(rng, fake, player_id, account_start, account_end)
        players[player_id] = (row[4], row[2])
        data["players"].append(row)

    platform_ids = list(range(1, len(PLATFORM_SEED) + 1))
    for platform_id, (name, manufacturer) in zip(platform_ids, PLATFORM_SEED):
        data["platforms"].append((platform_id, name, manufacturer, activity_start, activity_start))

    games: dict[int, tuple[date, Decimal]] = {}
    game_platforms: dict[int, list[int]] = {}
    for game_id in range(1, args.games + 1):
        game, pairs = make_game(rng, fake, game_id, args.as_of, platform_ids)
        games[game_id] = (game[4], game[6])
        game_platforms[game_id] = [pair[1] for pair in pairs]
        data["games"].append(game)
        data["game_platforms"].extend(pairs)

    data.update(
        simulate_activity(
            rng,
            players=players,
            games=games,
            game_platforms=game_platforms,
            campaigns=behavior.load_campaigns(args.campaigns),
            period_start=activity_start,
            period_end=end_of_day,
            counts={name: getattr(args, name) for name in ("sessions", "purchases", "reviews", "achievements")},
            first_ids={"sessions": 1, "purchases": 1, "reviews": 1, "achievements": 1},
            taken_review_pairs=set(),
            taken_achievements=set(),
        )
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
    cursor.execute("SELECT player_id, created_at, country FROM players ORDER BY player_id")
    players = {
        int(player_id): (created_at, str(country).strip())
        for player_id, created_at, country in cursor.fetchall()
    }
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
        players[player_id] = (row[4], row[2])
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
    if not players or not games:
        if args.sessions or args.purchases or args.reviews or args.achievements:
            raise SystemExit(
                "No players or games exist yet. Run --mode initial first, or append players and games."
            )
    cursor.execute("SELECT player_id, game_id FROM reviews")
    taken_pairs = {(int(p), int(g)) for p, g in cursor.fetchall()}
    cursor.execute("SELECT player_id, game_id, achievement_name FROM achievements")
    taken_keys = {(int(p), int(g), name) for p, g, name in cursor.fetchall()}
    activity = simulate_activity(
        rng,
        players=players,
        games=games,
        game_platforms=game_platforms,
        campaigns=behavior.load_campaigns(args.campaigns),
        period_start=activity_start,
        period_end=end_of_day,
        counts={name: getattr(args, name) for name in ("sessions", "purchases", "reviews", "achievements")},
        first_ids={table: _max_id(cursor, table) + 1
                   for table in ("sessions", "purchases", "reviews", "achievements")},
        taken_review_pairs=taken_pairs,
        taken_achievements=taken_keys,
    )
    for table in ("sessions", "purchases", "reviews", "achievements"):
        new[table] = activity[table]
        insert_table(cursor, table, new[table])

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
