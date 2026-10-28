"""Create deterministic synthetic Excel business inputs for the capstone."""

from __future__ import annotations

import argparse
import os
import random
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Sequence

import psycopg
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

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


def generate(seed: int = 42, campaign_count: int = 100) -> dict[str, int]:
    if campaign_count < 1:
        raise ValueError("campaign_count must be at least 1")
    load_dotenv(ROOT / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is missing from the project .env file.")

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT game_id FROM public.games ORDER BY game_id")
            game_ids = [row[0] for row in cursor.fetchall()]
    if len(game_ids) < campaign_count:
        raise RuntimeError(
            f"Need {campaign_count} source games to generate distinct campaign "
            f"game IDs; found {len(game_ids)}."
        )

    rng = random.Random(seed)
    selected_games = rng.sample(game_ids, k=campaign_count)
    campaign_rows: list[tuple[object, ...]] = []
    budget_rows: list[tuple[object, ...]] = []
    for campaign_id, game_id in enumerate(selected_games, start=1):
        budget = Decimal(rng.randrange(5_000, 50_001, 500)).quantize(Decimal("0.01"))
        start_date = date(2025, 1, 1) + timedelta(days=rng.randrange(0, 365))
        end_date = start_date + timedelta(days=rng.randrange(14, 91))
        campaign_rows.append(
            (
                campaign_id,
                game_id,
                f"Campaign {campaign_id:03d}",
                budget,
                start_date,
                end_date,
            )
        )
        actual_spend = (budget * Decimal(rng.randrange(55, 111)) / 100).quantize(
            Decimal("0.01")
        )
        budget_rows.append((campaign_id, budget, actual_spend))

    target_rows = [
        (
            game_id,
            Decimal(rng.randrange(1_000, 100_001, 250)).quantize(Decimal("0.01")),
            rng.randrange(100, 50_001, 100),
            rng.randrange(1_000, 500_001, 500),
            Decimal(rng.randrange(25, 501)) / 100,
        )
        for game_id in game_ids
    ]

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _write_workbook(
        OUTPUT_DIR / "marketing_campaigns.xlsx",
        "marketing_campaigns",
        CAMPAIGN_HEADERS,
        campaign_rows,
    )
    _write_workbook(
        OUTPUT_DIR / "game_targets.xlsx",
        "game_targets",
        TARGET_HEADERS,
        target_rows,
    )
    _write_workbook(
        OUTPUT_DIR / "budget.xlsx",
        "budget",
        BUDGET_HEADERS,
        budget_rows,
    )
    return {
        "marketing_campaigns": len(campaign_rows),
        "game_targets": len(target_rows),
        "budget": len(budget_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--campaign-count", type=int, default=100)
    args = parser.parse_args()
    report = generate(args.seed, args.campaign_count)
    print(report)
    print(f"Excel workbooks written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
