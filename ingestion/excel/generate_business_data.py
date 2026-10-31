"""Create deterministic synthetic Excel business inputs for the capstone.

    python -m ingestion.excel.generate_business_data                 # all three workbooks
    python -m ingestion.excel.generate_business_data --only targets  # keep campaigns, refresh targets

Targets are set from each game's *actual* activity in the source database
(revenue in USD, players, sessions, rating) times a random ambition factor
(0.7-1.4), so roughly half the games meet their target and the
"targets vs actual" mart has something to say. Re-seeding the source? Keep the
campaigns (the generator lifts activity during them) and refresh only targets.
"""

from __future__ import annotations

import argparse
import os
import random
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import psycopg
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

from ingestion.generate import behavior

ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT / "data" / "business"
CAMPAIGN_HEADERS = (
    "campaign_id",
    "game_id",
    "campaign",
    "budget",
    "start_date",
    "end_date",
)
TARGET_HEADERS = (
    "game_id",
    "target_revenue",
    "target_players",
    "target_sessions",
    "target_rating",
)
BUDGET_HEADERS = ("campaign_id", "budget", "actual_spend")


def _write_workbook(path: Path, sheet_name: str, headers: Sequence[str], rows: Sequence[Sequence[object]]) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = sheet_name
    sheet.append(list(headers))
    for cell in sheet[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="1F4E78")
    for row in rows:
        sheet.append(list(row))
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    for column_index, header in enumerate(headers, start=1):
        width = max(
            len(header) + 2,
            *(len(str(sheet.cell(row=row_index, column=column_index).value or "")) + 2
              for row_index in range(2, sheet.max_row + 1)),
        )
        sheet.column_dimensions[get_column_letter(column_index)].width = min(width, 32)
    workbook.save(path)


def source_actuals(cursor: Any) -> dict[int, dict[str, Decimal]]:
    """Per game: revenue (USD), distinct players, sessions and average rating."""
    rates = behavior.usd_rates()
    actuals: dict[int, dict[str, Decimal]] = {}
    cursor.execute("SELECT game_id FROM public.games ORDER BY game_id")
    for (game_id,) in cursor.fetchall():
        actuals[int(game_id)] = {"revenue": Decimal(0), "players": Decimal(0),
                                 "sessions": Decimal(0), "rating": Decimal(0)}
    cursor.execute("SELECT game_id, currency, SUM(amount) FROM public.purchases GROUP BY game_id, currency")
    for game_id, currency, amount in cursor.fetchall():
        actuals[int(game_id)]["revenue"] += Decimal(str(amount)) * rates.get(str(currency).strip(), Decimal(1))
    cursor.execute("SELECT game_id, COUNT(DISTINCT player_id), COUNT(*) FROM public.sessions GROUP BY game_id")
    for game_id, players, sessions in cursor.fetchall():
        actuals[int(game_id)].update(players=Decimal(players), sessions=Decimal(sessions))
    cursor.execute("SELECT game_id, AVG(rating) FROM public.reviews GROUP BY game_id")
    for game_id, rating in cursor.fetchall():
        actuals[int(game_id)]["rating"] = Decimal(str(rating))
    return actuals


def target_rows(rng: random.Random, actuals: dict[int, dict[str, Decimal]]) -> list[tuple[object, ...]]:
    """Targets = actual x ambition (0.7-1.4); games without activity get small floors."""
    rows = []
    for game_id in sorted(actuals):
        actual = actuals[game_id]

        def ambition() -> Decimal:
            return Decimal(str(round(rng.uniform(0.7, 1.4), 3)))

        revenue = max(Decimal("50"), actual["revenue"] * ambition()).quantize(Decimal("0.01"))
        reach = ambition()  # one factor, so a session target is never below the player target
        players = max(5, int(actual["players"] * reach))
        sessions = max(players, 10, int(actual["sessions"] * reach))
        base_rating = actual["rating"] if actual["rating"] > 0 else Decimal("3.5")
        rating = min(Decimal("5.00"), max(Decimal("1.00"),
                                          base_rating + Decimal(str(round(rng.uniform(-0.5, 0.6), 2)))))
        rows.append((game_id, revenue, players, sessions, rating.quantize(Decimal("0.01"))))
    return rows


def campaign_and_budget_rows(
    rng: random.Random, game_ids: Sequence[int], campaign_count: int
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]]]:
    if len(game_ids) < campaign_count:
        raise RuntimeError(
            f"Need {campaign_count} source games to generate distinct campaign "
            f"game IDs; found {len(game_ids)}."
        )
    campaign_rows: list[tuple[object, ...]] = []
    budget_rows: list[tuple[object, ...]] = []
    for campaign_id, game_id in enumerate(rng.sample(list(game_ids), k=campaign_count), start=1):
        budget = Decimal(rng.randrange(5_000, 50_001, 500)).quantize(Decimal("0.01"))
        start_date = date(2025, 1, 1) + timedelta(days=rng.randrange(0, 365))
        end_date = start_date + timedelta(days=rng.randrange(14, 91))
        campaign_rows.append(
            (campaign_id, game_id, f"Campaign {campaign_id:03d}", budget, start_date, end_date)
        )
        actual_spend = (budget * Decimal(rng.randrange(55, 111)) / 100).quantize(Decimal("0.01"))
        budget_rows.append((campaign_id, budget, actual_spend))
    return campaign_rows, budget_rows


def generate(seed: int = 42, campaign_count: int = 100, only: str = "all") -> dict[str, int]:
    if campaign_count < 1:
        raise ValueError("campaign_count must be at least 1")
    load_dotenv(ROOT / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is missing from the project .env file.")

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            actuals = source_actuals(cursor)

    rng = random.Random(seed)
    report: dict[str, int] = {}
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if only == "all":
        campaign_rows, budget_rows = campaign_and_budget_rows(rng, sorted(actuals), campaign_count)
        _write_workbook(OUTPUT_DIR / "marketing_campaigns.xlsx", "marketing_campaigns",
                        CAMPAIGN_HEADERS, campaign_rows)
        _write_workbook(OUTPUT_DIR / "budget.xlsx", "budget", BUDGET_HEADERS, budget_rows)
        report.update(marketing_campaigns=len(campaign_rows), budget=len(budget_rows))
    targets = target_rows(random.Random(seed + 1), actuals)
    _write_workbook(OUTPUT_DIR / "game_targets.xlsx", "game_targets", TARGET_HEADERS, targets)
    report["game_targets"] = len(targets)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--campaign-count", type=int, default=100)
    parser.add_argument(
        "--only",
        choices=("all", "targets"),
        default="all",
        help="targets: keep the existing campaign and budget workbooks, refresh targets only.",
    )
    args = parser.parse_args()
    report = generate(args.seed, args.campaign_count, args.only)
    print(report)
    print(f"Excel workbooks written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
