from __future__ import annotations

import io
import json
import os
import unittest
from datetime import date, datetime, timedelta, timezone
from email.message import Message
from email.utils import format_datetime
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse
from unittest.mock import Mock, patch

from ingestion.api.rawg import (
    RESOURCES,
    RawgApiError,
    analytics_connection_values,
    parse_args,
    parse_retry_after,
    record_values,
    request_page,
    validate_page,
)


class RawgIngestionTests(unittest.TestCase):
    def test_analytics_connection_supports_container_host(self) -> None:
        with patch.dict(
            os.environ,
            {
                "POSTGRES_USER": "test-user",
                "POSTGRES_PASSWORD": "test-password",
                "ANALYTICS_HOST": "postgres-analytics",
                "ANALYTICS_PORT": "5432",
            },
        ):
            connection = analytics_connection_values()
        self.assertEqual(connection["host"], "postgres-analytics")
        self.assertEqual(connection["port"], 5432)

    def test_default_cli_limits_and_resources(self) -> None:
        args = parse_args([])
        self.assertEqual(args.resources, list(RESOURCES))
        self.assertEqual(args.page_size, 40)
        self.assertEqual(args.max_pages, 5)

    def test_rejects_page_size_above_api_limit(self) -> None:
        with self.assertRaises(SystemExit) as error:
            parse_args(["--page-size", "41"])
        self.assertEqual(error.exception.code, 2)

    def test_parses_retry_after_seconds_and_http_date(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        retry_at = format_datetime(now + timedelta(seconds=12), usegmt=True)
        self.assertEqual(parse_retry_after("3"), 3.0)
        self.assertEqual(parse_retry_after(retry_at, now=now), 12.0)

    def test_validates_records_and_reads_next_page(self) -> None:
        records, next_page = validate_page(
            {
                "count": 2,
                "next": "https://api.rawg.io/api/games/?page=2&page_size=40",
                "results": [{"id": 10, "name": "Example"}],
            },
            RESOURCES["games"],
            current_page=1,
        )
        self.assertEqual(records[0]["id"], 10)
        self.assertEqual(next_page, 2)

    def test_rejects_external_next_url(self) -> None:
        with self.assertRaisesRegex(RawgApiError, "unsafe pagination URL"):
            validate_page(
                {
                    "count": 1,
                    "next": "https://example.com/api/games/?page=2",
                    "results": [{"id": 10, "name": "Example"}],
                },
                RESOURCES["games"],
                current_page=1,
            )

    def test_rejects_invalid_game_identity(self) -> None:
        with self.assertRaisesRegex(RawgApiError, "invalid id"):
            validate_page(
                {"count": 1, "next": None, "results": [{"id": -1, "name": "Example"}]},
                RESOURCES["games"],
                current_page=1,
            )

    def test_rejects_invalid_numeric_fields(self) -> None:
        with self.assertRaisesRegex(RawgApiError, "invalid numeric field rating"):
            record_values(
                RESOURCES["games"],
                {"id": 10, "name": "Example", "rating": "high"},
            )

    def test_converts_game_date_and_keeps_original_payload(self) -> None:
        values = record_values(
            RESOURCES["games"],
            {
                "id": 5,
                "name": " Example ",
                "released": "2024-05-06",
                "updated": "2024-05-06T10:00:00Z",
                "platforms": [{"platform": {"id": 1, "name": "PC"}}],
            },
        )
        self.assertEqual(values[1], "Example")
        self.assertEqual(values[3], date(2024, 5, 6))
        self.assertEqual(values[4].tzinfo, timezone.utc)
        self.assertEqual(values[11].obj["platforms"][0]["platform"]["name"], "PC")

    def test_uses_api_key_and_retries_rate_limit(self) -> None:
        error_headers = Message()
        error_headers["Retry-After"] = "0"
        retry_error = HTTPError(
            "https://api.rawg.io/api/games/",
            429,
            "rate limited",
            error_headers,
            None,
        )
        response = io.BytesIO(
            json.dumps({"count": 0, "next": None, "results": []}).encode()
        )
        response.status = 200
        opener = Mock(side_effect=[retry_error, response])
        sleeper = Mock()

        payload = request_page(
            "games",
            "test-key",
            page=1,
            page_size=40,
            opener=opener,
            sleeper=sleeper,
        )

        request = opener.call_args_list[0].args[0]
        parsed_url = urlparse(request.full_url)
        query = parse_qs(parsed_url.query)
        self.assertEqual(parsed_url.path, "/api/games")
        self.assertEqual(query["key"], ["test-key"])
        self.assertEqual(payload["count"], 0)
        sleeper.assert_called_once_with(0.0)


if __name__ == "__main__":
    unittest.main()
