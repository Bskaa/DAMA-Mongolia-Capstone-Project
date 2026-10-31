"""Audit the local .env before starting the stack (DAMA: data security).

    python -m scripts.check_env            # checks ./.env
    python -m scripts.check_env path/.env

Prints one line per finding and exits 1 when anything is an ERROR:
placeholders still in place, passwords that do not match each other, weak or
reused secrets, characters that break connection URLs. Values are never printed.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Mapping
from urllib.parse import unquote, urlparse

from dotenv import dotenv_values

PLACEHOLDER = re.compile(r"change-this|ci-only-placeholder|^password$|^changeme$", re.IGNORECASE)
URL_SAFE = re.compile(r"^[A-Za-z0-9._~-]+$")
REQUIRED = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "DATABASE_URL", "ANALYTICS_DB")
SECRETS = (
    "POSTGRES_PASSWORD", "DASHBOARD_DB_PASSWORD", "AIRFLOW_DB_PASSWORD",
    "AIRFLOW__API_AUTH__JWT_SECRET", "OPENMETADATA_MYSQL_ROOT_PASSWORD",
)

Finding = tuple[str, str]  # (level, message)


def audit_env(env: Mapping[str, str | None]) -> list[Finding]:
    value = {key: (val or "").strip() for key, val in env.items()}
    findings: list[Finding] = []

    def add(level: str, message: str) -> None:
        findings.append((level, message))

    for key in REQUIRED:
        if not value.get(key):
            add("ERROR", f"{key} is missing or empty")

    for key in SECRETS:
        secret = value.get(key, "")
        if not secret:
            continue
        if PLACEHOLDER.search(secret):
            add("ERROR", f"{key} still has the placeholder value from .env.example")
        elif len(secret) < 12:
            add("WARN", f"{key} is shorter than 12 characters")

    url = value.get("DATABASE_URL", "")
    if url and value.get("POSTGRES_PASSWORD"):
        parsed = urlparse(url)
        if unquote(parsed.password or "") != value["POSTGRES_PASSWORD"]:
            add("ERROR", "DATABASE_URL password does not match POSTGRES_PASSWORD")
        if parsed.username and unquote(parsed.username) != value.get("POSTGRES_USER"):
            add("ERROR", "DATABASE_URL user does not match POSTGRES_USER")
        if parsed.path.lstrip("/") and parsed.path.lstrip("/") != value.get("POSTGRES_DB", "gaming_source"):
            add("WARN", "DATABASE_URL database differs from POSTGRES_DB")
        expected_port = value.get("POSTGRES_PORT") or "5432"
        if parsed.port and str(parsed.port) != expected_port:
            add("WARN", f"DATABASE_URL port {parsed.port} differs from POSTGRES_PORT {expected_port}")

    jwt = value.get("AIRFLOW__API_AUTH__JWT_SECRET", "")
    if not jwt:
        add("WARN", "AIRFLOW__API_AUTH__JWT_SECRET is empty: the orchestration profile will not start")
    elif len(jwt) < 32:
        add("ERROR", "AIRFLOW__API_AUTH__JWT_SECRET should be at least 32 characters")

    airflow_db = value.get("AIRFLOW_DB_PASSWORD", "")
    if not airflow_db:
        add("WARN", "AIRFLOW_DB_PASSWORD is empty: postgres-airflow (orchestration profile) will not start")
    elif not URL_SAFE.match(airflow_db):
        add("ERROR", "AIRFLOW_DB_PASSWORD must be URL-safe (letters, digits, . _ ~ -): it is part of a connection URL")

    dashboard = value.get("DASHBOARD_DB_PASSWORD", "")
    if not dashboard:
        add("WARN", "DASHBOARD_DB_PASSWORD is empty: the dashboard cannot log in")
    elif dashboard == value.get("POSTGRES_PASSWORD"):
        add("ERROR", "DASHBOARD_DB_PASSWORD reuses POSTGRES_PASSWORD: the read-only login must have its own secret")

    distinct = [value[key] for key in SECRETS if value.get(key)]
    if len(distinct) != len(set(distinct)):
        add("WARN", "the same secret is used for more than one purpose")

    if value.get("AIRFLOW__SMTP__SMTP_HOST"):
        if not value.get("QUALITY_ALERT_EMAIL_TO"):
            add("WARN", "SMTP is configured but QUALITY_ALERT_EMAIL_TO is empty: alerts are only logged")
        password = value.get("AIRFLOW__SMTP__SMTP_PASSWORD", "").replace(" ", "")
        if "gmail" in value["AIRFLOW__SMTP__SMTP_HOST"].lower() and len(password) != 16:
            add("WARN", "Gmail needs a 16-character App Password, not the account password")
    else:
        add("INFO", "SMTP is not configured: alerts are logged in monitoring.alert_log but not emailed")

    if value.get("OPENMETADATA_DB_PASSWORD", "openmetadata_password") == "openmetadata_password":
        add("INFO", "OpenMetadata uses the image's default MySQL user password (local learning only)")
    if not value.get("RAWG_API_KEY"):
        add("INFO", "RAWG_API_KEY is empty: the RAWG task is skipped and the genre benchmark has no RAWG data")
    return findings


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    path = Path(args[0]) if args else Path(".env")
    if not path.exists():
        print(f"ERROR {path} does not exist. Copy .env.example to .env first.")
        return 1
    findings = audit_env(dotenv_values(path))
    for level, message in findings:
        print(f"{level:<5} {message}")
    errors = sum(level == "ERROR" for level, _ in findings)
    print(f"\n{errors} error(s), {sum(level == 'WARN' for level, _ in findings)} warning(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
