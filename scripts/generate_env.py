"""Create .env from .env.example with fresh random secrets (no third-party packages).

    python3 scripts/generate_env.py            # writes .env, refuses to overwrite
    python3 scripts/generate_env.py --force    # replace an existing .env

Every placeholder password, the Airflow JWT secret and the matching password inside
DATABASE_URL get new random URL-safe values. Optional settings (RAWG_API_KEY, SMTP,
OPENMETADATA_JWT_TOKEN) are left for you to fill in. Secrets are never printed.
"""

from __future__ import annotations

import argparse
import re
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECRET_KEYS = {
    "POSTGRES_PASSWORD": 24,
    "DASHBOARD_DB_PASSWORD": 24,
    "AIRFLOW_DB_PASSWORD": 24,
    "AIRFLOW__API_AUTH__JWT_SECRET": 48,
    "OPENMETADATA_MYSQL_ROOT_PASSWORD": 24,
}


def token(length: int) -> str:
    # token_urlsafe -> letters, digits, '-' and '_': safe inside connection URLs
    return secrets.token_urlsafe(length)[:length]


def render(example: str) -> str:
    values = {key: token(length) for key, length in SECRET_KEYS.items()}
    lines = []
    for line in example.splitlines():
        match = re.match(r"^([A-Z0-9_]+)=(.*)$", line)
        if match and match.group(1) in values:
            line = f"{match.group(1)}={values[match.group(1)]}"
        lines.append(line)
    text = "\n".join(lines) + "\n"
    # Keep DATABASE_URL consistent with the new POSTGRES_PASSWORD.
    return re.sub(
        r"^(DATABASE_URL=postgresql://[^:]+:)[^@]*(@.*)$",
        lambda m: m.group(1) + values["POSTGRES_PASSWORD"] + m.group(2),
        text,
        flags=re.MULTILINE,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--force", action="store_true", help="overwrite an existing .env")
    parser.add_argument("--example", type=Path, default=ROOT / ".env.example")
    parser.add_argument("--output", type=Path, default=ROOT / ".env")
    args = parser.parse_args(argv)
    if args.output.exists() and not args.force:
        print(f"{args.output} already exists; use --force to replace it.")
        return 1
    args.output.write_text(render(args.example.read_text(encoding="utf-8")), encoding="utf-8")
    try:
        args.output.chmod(0o600)  # owner-only on Linux
    except OSError:
        pass
    print(f"Wrote {args.output} with new random secrets (not shown).")
    print("Optional: set RAWG_API_KEY, AIRFLOW__SMTP__* + QUALITY_ALERT_EMAIL_TO, OPENMETADATA_JWT_TOKEN.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
