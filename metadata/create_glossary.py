"""Create (or update) the business glossary in OpenMetadata.

The definitions behind the dashboard's numbers (DAU, ARPPU, retention, ...)
live in one place that every consumer can find, and dbt models point at them
(meta.openmetadata.glossary in the dbt YAML), so the catalogue links each
term to the tables that implement it.

Run it once before the dbt ingestion, inside the OpenMetadata ingestion image:

    docker compose --profile openmetadata run --rm --entrypoint python \
        openmetadata-ingestion /opt/metadata/create_glossary.py

Idempotent: uses OpenMetadata's create-or-update (PUT) endpoints.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

GLOSSARY = "GamingAnalytics"
GLOSSARY_DESCRIPTION = (
    "Business terms of the Gaming Analytics capstone: engagement, monetisation, "
    "campaign and data-governance definitions used by the Gold marts and the dashboard."
)
# name (FQN part), display name, description, synonyms
TERMS = (
    ("DAU", "Daily active players (DAU)",
     "Distinct players with at least one session on the day. Source: gold.mart_daily_kpis.daily_active_players.",
     ["Daily active users", "Active players"]),
    ("NewPlayer", "New player",
     "A player whose first ever session falls on the day. Source: gold.mart_daily_kpis.new_players.", []),
    ("ARPDAU", "ARPDAU",
     "Average revenue per daily active player: revenue (USD) of the day divided by DAU.",
     ["Average revenue per daily active user"]),
    ("ARPPU", "ARPPU",
     "Average revenue per paying player: revenue (USD) of the day divided by paying players of the day.",
     ["Average revenue per paying user"]),
    ("RetentionRate", "Retention rate",
     "Share of a monthly cohort (month of first session) that is active N months later. "
     "Source: gold.mart_player_retention.", ["Cohort retention"]),
    ("RevenueAttainment", "Revenue attainment",
     "Actual revenue in USD divided by the target revenue from game_targets.xlsx; 1.0 means the target is met.",
     []),
    ("RevenueUplift", "Revenue uplift",
     "Revenue of the promoted game during a campaign minus revenue in the equally long window just before it. "
     "Correlational, not causal.", []),
    ("USDConversion", "USD conversion",
     "Purchase amounts are stored in the original currency; USD values use the illustrative "
     "fx_rates_to_usd reference data and are only for cross-currency comparison.", ["FX rate"]),
    ("PersonalData", "Personal data (PII)",
     "Data that identifies a player directly (username) or indirectly (birth year, country). "
     "Kept in Bronze/Silver only; Gold exposes an age band. Tagged DataGovernance.pii.", ["PII"]),
    ("MedallionLayer", "Medallion layer",
     "Bronze = data as it arrived, Silver = typed and cleaned, Gold = modelled for analytics. "
     "Only Gold is readable by BI roles.", ["Bronze", "Silver", "Gold"]),
)


def request(method: str, url: str, token: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode() or "{}")
    except urllib.error.HTTPError as error:
        raise SystemExit(f"{method} {url} -> HTTP {error.code}: {error.read().decode()[:500]}") from error


def main() -> int:
    base = os.environ.get("OPENMETADATA_HOST_PORT", "http://openmetadata-server:8585/api").rstrip("/")
    token = os.environ.get("OPENMETADATA_JWT_TOKEN", "")
    if not token:
        print("OPENMETADATA_JWT_TOKEN is not set (Settings -> Bots -> ingestion-bot in the UI).")
        return 1
    request("PUT", f"{base}/v1/glossaries", token,
            {"name": GLOSSARY, "displayName": "Gaming Analytics", "description": GLOSSARY_DESCRIPTION})
    for name, display_name, description, synonyms in TERMS:
        request("PUT", f"{base}/v1/glossaryTerms", token, {
            "glossary": GLOSSARY,
            "name": name,
            "displayName": display_name,
            "description": description,
            "synonyms": synonyms,
        })
        print(f"  {GLOSSARY}.{name}")
    print(f"Glossary {GLOSSARY}: {len(TERMS)} terms created or updated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
