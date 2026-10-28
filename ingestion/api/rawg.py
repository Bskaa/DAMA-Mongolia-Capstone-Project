"""Load paginated RAWG game metadata into the analytics PostgreSQL raw schema."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[2]
API_ROOT = "https://api.rawg.io/api"
API_HOST = "api.rawg.io"
DEFAULT_PAGE_SIZE = 40
MAX_PAGE_SIZE = 40
MAX_RETRIES = 5
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRY_AFTER_SECONDS = 300
LOGGER = logging.getLogger("rawg")


@dataclass(frozen=True)
class Resource:
    endpoint: str
    table: str
    columns: tuple[str, ...]


RESOURCES: dict[str, Resource] = {
    "games": Resource(
        "games",
        "rawg_games",
        (
            "rawg_id",
            "name",
            "slug",
            "released",
            "updated",
            "rating",
            "rating_top",
            "ratings_count",
            "metacritic",
            "playtime",
            "background_image",
            "raw_payload",
            "fetched_at",
        ),
    ),
    "platforms": Resource(
        "platforms",
        "rawg_platforms",
        (
            "rawg_id",
            "name",
            "slug",
            "games_count",
            "image_background",
            "image",
            "year_start",
            "year_end",
            "raw_payload",
            "fetched_at",
        ),
    ),
    "genres": Resource(
        "genres",
        "rawg_genres",
        (
            "rawg_id",
            "name",
            "slug",
            "games_count",
            "image_background",
            "raw_payload",
            "fetched_at",
        ),
    ),
    "publishers": Resource(
        "publishers",
        "rawg_publishers",
        (
            "rawg_id",
            "name",
            "slug",
            "games_count",
            "image_background",
            "raw_payload",
            "fetched_at",
        ),
    ),
    "developers": Resource(
        "developers",
        "rawg_developers",
        (
            "rawg_id",
            "name",
            "slug",
            "games_count",
            "image_background",
            "raw_payload",
            "fetched_at",
        ),
    ),
}
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class RawgApiError(RuntimeError):
    """Raised when RAWG responds with an invalid or unsuccessful response."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--resources",
        nargs="+",
        choices=tuple(RESOURCES),
        default=list(RESOURCES),
        help="RAWG collections to ingest (default: all five).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Records per page (1-{MAX_PAGE_SIZE}; default: {DEFAULT_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum pages per collection (default: 5; limits API usage).",
    )
    args = parser.parse_args(argv)
    if not 1 <= args.page_size <= MAX_PAGE_SIZE:
        parser.error(f"--page-size must be between 1 and {MAX_PAGE_SIZE}")
    if args.max_pages < 1:
        parser.error("--max-pages must be at least 1")
    return args


def analytics_connection_values() -> dict[str, Any]:
    required = ("POSTGRES_USER", "POSTGRES_PASSWORD")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            "Missing analytics database settings in .env: " + ", ".join(missing)
        )
    return {
        "host": os.environ.get("ANALYTICS_HOST", "localhost"),
        "port": int(os.environ.get("ANALYTICS_PORT", "5433")),
        "dbname": os.environ.get("ANALYTICS_DB", "gaming_analytics"),
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "connect_timeout": 10,
    }


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        return max(0.0, (retry_at - current).total_seconds())


def request_page(
    endpoint: str,
    api_key: str,
    page: int,
    page_size: int,
    *,
    retries: int = MAX_RETRIES,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    opener: Callable[..., Any] = urlopen,
    sleeper: Callable[[float], None] = time.sleep,
) -> Mapping[str, Any]:
    query = urlencode(
        {"key": api_key, "page": page, "page_size": page_size}
    )
    url = f"{API_ROOT}/{endpoint}?{query}"
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "GamingAnalyticsDataPlatform/1.0",
        },
    )
    for attempt in range(retries + 1):
        try:
            with opener(request, timeout=timeout) as response:
                status = getattr(response, "status", 200)
                if status < 200 or status >= 300:
                    raise RawgApiError(f"RAWG returned HTTP status {status}.")
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            if error.code not in RETRYABLE_STATUSES:
                raise RawgApiError(
                    f"RAWG request for {endpoint} failed with HTTP {error.code}."
                ) from None
            if attempt >= retries:
                raise RawgApiError(
                    f"RAWG request for {endpoint} exhausted retries "
                    f"after HTTP {error.code}."
                ) from None
            wait_seconds = parse_retry_after(error.headers.get("Retry-After"))
            if wait_seconds is None:
                wait_seconds = min(2**attempt + random.random(), 60.0)
            if wait_seconds > MAX_RETRY_AFTER_SECONDS:
                raise RawgApiError(
                    f"RAWG requested a {wait_seconds:.0f}s retry delay; "
                    "stopping rather than exceeding the 5-minute safety limit."
                ) from None
            LOGGER.warning(
                "RAWG endpoint %s returned HTTP %s; retrying in %.1f seconds "
                "(attempt %s/%s).",
                endpoint,
                error.code,
                wait_seconds,
                attempt + 1,
                retries,
            )
            sleeper(wait_seconds)
            continue
        except (TimeoutError, URLError, OSError) as error:
            if attempt >= retries:
                raise RawgApiError(
                    f"RAWG request for {endpoint} failed after {retries + 1} attempts "
                    f"({type(error).__name__})."
                ) from None
            wait_seconds = min(2**attempt + random.random(), 60.0)
            LOGGER.warning(
                "RAWG endpoint %s had a transient network error; retrying in "
                "%.1f seconds (attempt %s/%s).",
                endpoint,
                wait_seconds,
                attempt + 1,
                retries,
            )
            sleeper(wait_seconds)
            continue
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RawgApiError(
                f"RAWG returned invalid JSON for {endpoint} "
                f"({type(error).__name__})."
            ) from None

        if not isinstance(payload, dict):
            raise RawgApiError(f"RAWG returned a non-object response for {endpoint}.")
        return payload
    raise AssertionError("Retry loop exited unexpectedly.")


def validate_page(
    payload: Mapping[str, Any],
    resource: Resource,
    current_page: int,
) -> tuple[list[Mapping[str, Any]], int | None]:
    count = payload.get("count")
    results = payload.get("results")
    next_url = payload.get("next")
    if type(count) is not int or count < 0:
        raise RawgApiError(f"RAWG {resource.endpoint} response has an invalid count.")
    if not isinstance(results, list):
        raise RawgApiError(
            f"RAWG {resource.endpoint} response is missing a results array."
        )
    if next_url is not None and not isinstance(next_url, str):
        raise RawgApiError(f"RAWG {resource.endpoint} response has an invalid next URL.")

    records: list[Mapping[str, Any]] = []
    for index, record in enumerate(results):
        if not isinstance(record, dict):
            raise RawgApiError(
                f"RAWG {resource.endpoint} record {index} is not an object."
            )
        rawg_id = record.get("id")
        name = record.get("name")
        if type(rawg_id) is not int or rawg_id <= 0:
            raise RawgApiError(
                f"RAWG {resource.endpoint} record {index} has an invalid id."
            )
        if not isinstance(name, str) or not name.strip():
            raise RawgApiError(
                f"RAWG {resource.endpoint} record {rawg_id} has no valid name."
            )
        records.append(record)

    next_page: int | None = None
    if next_url:
        parsed = urlparse(next_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != API_HOST
            or parsed.path.rstrip("/") != f"/api/{resource.endpoint}"
        ):
            raise RawgApiError(
                f"RAWG {resource.endpoint} response returned an unsafe pagination URL."
            )
        pages = parse_qs(parsed.query).get("page")
        if not pages or not pages[0].isdigit():
            raise RawgApiError(
                f"RAWG {resource.endpoint} next URL has no valid page number."
            )
        next_page = int(pages[0])
        if next_page <= current_page:
            raise RawgApiError(
                f"RAWG {resource.endpoint} pagination did not advance beyond "
                f"page {current_page}."
            )
    return records, next_page


def _parse_date(value: Any, field: str, rawg_id: int) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RawgApiError(f"RAWG game {rawg_id} has invalid {field}.")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise RawgApiError(f"RAWG game {rawg_id} has invalid {field}.") from None


def _parse_datetime(value: Any, field: str, rawg_id: int) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RawgApiError(f"RAWG game {rawg_id} has invalid {field}.")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise RawgApiError(f"RAWG game {rawg_id} has invalid {field}.") from None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result


def _optional_int(value: Any, field: str, resource: str, rawg_id: int) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise RawgApiError(
            f"RAWG {resource} {rawg_id} has invalid integer field {field}."
        )
    return value


def _optional_number(
    value: Any, field: str, resource: str, rawg_id: int
) -> int | float | None:
    if value is None:
        return None
    if type(value) not in (int, float) or not math.isfinite(value):
        raise RawgApiError(
            f"RAWG {resource} {rawg_id} has invalid numeric field {field}."
        )
    return value


def _optional_text(value: Any, field: str, resource: str, rawg_id: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RawgApiError(
            f"RAWG {resource} {rawg_id} has invalid text field {field}."
        )
    return value


def record_values(resource: Resource, record: Mapping[str, Any]) -> tuple[Any, ...]:
    rawg_id = record["id"]
    name = record["name"].strip()
    slug = _optional_text(record.get("slug"), "slug", resource.endpoint, rawg_id)

    if resource.endpoint == "games":
        values: dict[str, Any] = {
            "rawg_id": rawg_id,
            "name": name,
            "slug": slug,
            "released": _parse_date(record.get("released"), "released date", rawg_id),
            "updated": _parse_datetime(record.get("updated"), "updated", rawg_id),
            "rating": _optional_number(
                record.get("rating"), "rating", resource.endpoint, rawg_id
            ),
            "rating_top": _optional_int(
                record.get("rating_top"), "rating_top", resource.endpoint, rawg_id
            ),
            "ratings_count": _optional_int(
                record.get("ratings_count"),
                "ratings_count",
                resource.endpoint,
                rawg_id,
            ),
            "metacritic": _optional_int(
                record.get("metacritic"), "metacritic", resource.endpoint, rawg_id
            ),
            "playtime": _optional_int(
                record.get("playtime"), "playtime", resource.endpoint, rawg_id
            ),
            "background_image": _optional_text(
                record.get("background_image"),
                "background_image",
                resource.endpoint,
                rawg_id,
            ),
        }
    else:
        values = {
            "rawg_id": rawg_id,
            "name": name,
            "slug": slug,
            "games_count": _optional_int(
                record.get("games_count"), "games_count", resource.endpoint, rawg_id
            ),
            "image_background": _optional_text(
                record.get("image_background"),
                "image_background",
                resource.endpoint,
                rawg_id,
            ),
        }
        if resource.endpoint == "platforms":
            values["image"] = _optional_text(
                record.get("image"), "image", resource.endpoint, rawg_id
            )
            values["year_start"] = _optional_int(
                record.get("year_start"), "year_start", resource.endpoint, rawg_id
            )
            values["year_end"] = _optional_int(
                record.get("year_end"), "year_end", resource.endpoint, rawg_id
            )
    values["raw_payload"] = Jsonb(dict(record))
    values["fetched_at"] = datetime.now(timezone.utc)
    return tuple(values[column] for column in resource.columns)


def upsert_records(
    connection: psycopg.Connection[Any],
    resource: Resource,
    records: list[Mapping[str, Any]],
) -> int:
    if not records:
        return 0
    columns = resource.columns
    column_sql = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(
        f'"{column}" = EXCLUDED."{column}"'
        for column in columns
        if column != "rawg_id"
    )
    query = (
        f'INSERT INTO raw."{resource.table}" ({column_sql}) '
        f"VALUES ({placeholders}) ON CONFLICT (rawg_id) DO UPDATE SET {updates}"
    )
    rows = [record_values(resource, record) for record in records]
    with connection.cursor() as cursor:
        cursor.executemany(query, rows)
    connection.commit()
    return len(rows)


def apply_schema(connection: psycopg.Connection[Any]) -> None:
    schema_path = ROOT / "sql" / "003_rawg_schema.sql"
    sql = schema_path.read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        for statement in sql.split(";"):
            if statement.strip():
                cursor.execute(statement)
    connection.commit()


def ingest_resource(
    connection: psycopg.Connection[Any],
    resource_name: str,
    api_key: str,
    page_size: int,
    max_pages: int,
) -> dict[str, int]:
    resource = RESOURCES[resource_name]
    page = 1
    page_count = 0
    record_count = 0
    while page_count < max_pages:
        payload = request_page(resource.endpoint, api_key, page, page_size)
        records, next_page = validate_page(payload, resource, page)
        record_count += upsert_records(connection, resource, records)
        page_count += 1
        LOGGER.info(
            "Ingested %s page %s: %s records (reported total: %s).",
            resource_name,
            page,
            len(records),
            payload["count"],
        )
        if next_page is None:
            break
        page = next_page
    return {"pages": page_count, "records_upserted": record_count}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    load_dotenv(ROOT / ".env")
    args = parse_args(argv)
    api_key = os.environ.get("RAWG_API_KEY", "").strip()
    if not api_key:
        LOGGER.error(
            "RAWG_API_KEY is missing. Add your key to the project .env file; "
            "the key is required for every RAWG API request."
        )
        return 2

    try:
        connection_settings = analytics_connection_values()
        with psycopg.connect(**connection_settings) as connection:
            apply_schema(connection)
            report = {
                name: ingest_resource(
                    connection,
                    name,
                    api_key,
                    args.page_size,
                    args.max_pages,
                )
                for name in args.resources
            }
    except (OSError, psycopg.Error, RawgApiError, RuntimeError) as error:
        LOGGER.error("%s", error)
        return 1

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
