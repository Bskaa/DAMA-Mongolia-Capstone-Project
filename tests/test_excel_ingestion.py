from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

from openpyxl import Workbook

from ingestion.excel.load_excel import (
    SPECS,
    WorkbookValidationError,
    deduplicate_rows,
    read_workbook,
)


class ExcelIngestionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.folder = Path(self.temp_dir.name)

    def write_workbook(
        self, spec_index: int, rows: list[tuple[object, ...]], headers: tuple[str, ...] | None = None
    ) -> Path:
        spec = SPECS[spec_index]
        path = self.folder / spec.file_name
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = spec.sheet_name
        sheet.append(list(headers or spec.headers))
        for row in rows:
            sheet.append(list(row))
        workbook.save(path)
        return path

    def test_reads_and_normalizes_campaign_rows_and_skips_blank_rows(self) -> None:
        spec = SPECS[0]
        path = self.write_workbook(
            0,
            [
                (1.0, 11.0, "  Summer Sale  ", 1200, datetime(2025, 1, 2), date(2025, 2, 2)),
                (None, None, None, None, None, None),
            ],
        )
        rows, blank_rows = read_workbook(path, spec)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["campaign"], "Summer Sale")
        self.assertEqual(rows[0]["campaign_id"], 1)
        self.assertEqual(rows[0]["game_id"], 11)
        self.assertEqual(rows[0]["start_date"], date(2025, 1, 2))
        self.assertEqual(blank_rows, 1)

    def test_deduplicates_identical_primary_key_rows(self) -> None:
        rows = [
            {"campaign_id": 7, "campaign": "Launch"},
            {"campaign_id": 7, "campaign": "Launch"},
        ]
        unique, duplicates = deduplicate_rows(rows, SPECS[0])
        self.assertEqual(unique, [rows[0]])
        self.assertEqual(duplicates, 1)

    def test_rejects_conflicting_duplicate_primary_keys(self) -> None:
        rows = [
            {"campaign_id": 7, "campaign": "Launch"},
            {"campaign_id": 7, "campaign": "Summer"},
        ]
        with self.assertRaisesRegex(WorkbookValidationError, "conflicting rows"):
            deduplicate_rows(rows, SPECS[0])

    def test_rejects_unexpected_header_contract(self) -> None:
        path = self.write_workbook(
            0,
            [(1, 11, "Launch", 100, date(2025, 1, 1), date(2025, 1, 2))],
            headers=("id", "game_id", "campaign", "budget", "start_date", "end_date"),
        )
        with self.assertRaisesRegex(WorkbookValidationError, "headers must be exactly"):
            read_workbook(path, SPECS[0])

    def test_rejects_invalid_target_rating(self) -> None:
        path = self.write_workbook(1, [(11, 1000, 100, 1000, 5.1)])
        with self.assertRaisesRegex(WorkbookValidationError, "target_rating"):
            read_workbook(path, SPECS[1])

    def test_rejects_campaign_end_before_start(self) -> None:
        path = self.write_workbook(
            0,
            [(1, 11, "Launch", 100, date(2025, 2, 1), date(2025, 1, 1))],
        )
        with self.assertRaisesRegex(WorkbookValidationError, "end_date precedes"):
            read_workbook(path, SPECS[0])


if __name__ == "__main__":
    unittest.main()
