# Gaming Analytics Data Platform

An independent learning project for building an end-to-end gaming analytics
platform. Phase 1 establishes a reproducible PostgreSQL source database with
referentially valid synthetic operational data.

## Phase 1 — Docker, PostgreSQL, and synthetic data

### Goal and architecture

Docker Compose runs one PostgreSQL source database. The database initializes
its schema from `sql/source_schema.sql` and persists data in a named Docker
volume. A local Python generator uses Faker and a fixed random seed to create
the source dataset. 

```text
Faker generator ──> PostgreSQL source
                          ├── players
                          ├── games ── game_platforms ── platforms
                          ├── sessions
                          ├── purchases
                          ├── achievements
                          └── reviews
```

`sessions` are one row per play session. `purchases` are one row per purchase,
`reviews` are one row per player/game review, and `achievements` are one row
per player/game/achievement earned. Timestamps use `TIMESTAMPTZ` and are
generated in UTC. `game_platforms` represents a game's supported platforms;
the session foreign key guarantees each session uses a platform supported by
that game.

### Prerequisites

- Docker Desktop with Docker Compose v2
- Python 3.10 or newer
- PowerShell on Windows

Docker Compose uses the current Compose Specification (no legacy `version`
field). The PostgreSQL container is based on the official `postgres:17-alpine`
image. Python dependencies are listed in `requirements.txt`.

### Files

```text
.
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── sql/
│   └── source_schema.sql
└── ingestion/
    └── generate/
        └── generate_gaming_data.py
```

### Setup and run

Run these commands from the project root in PowerShell:

```powershell
Copy-Item .env.example .env
```

Edit `.env` and replace `POSTGRES_PASSWORD` and the password in
`DATABASE_URL` with the same local development password. Do not commit `.env`.
The supplied value is only a local example, not a production credential.

```powershell
docker compose config
docker compose up -d postgres-source
docker compose ps
```

Wait until `postgres-source` reports `healthy`, then install the generator
dependencies in a virtual environment:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m ingestion.generate.generate_gaming_data
```

The default dataset targets 10,000 players, 1,000 games, 100,000 sessions,
30,000 purchases, and 20,000 reviews. The generator also creates the platform,
game-platform, and achievement records. It uses seed `42` and as-of date
`2025-12-31` by default, so re-generating with the same options gives the same
synthetic dataset.

Smaller loads are useful for quick checks:

```powershell
python -m ingestion.generate.generate_gaming_data --players 100 --games 20 --sessions 500 --purchases 100 --reviews 100
```

To replace existing generated data, explicitly pass `--reset`. This truncates
the source tables and reloads them; without the flag, the generator refuses
to load into a populated source database.

```powershell
python -m ingestion.generate.generate_gaming_data --reset
```

The schema initialization script is applied only when PostgreSQL creates an
empty data directory. Changing the SQL file does not update an already
initialized database; apply schema changes deliberately with `psql` or recreate
the volume only when its stored data can be discarded.

### Expected output and validation

The generator prints a JSON object containing row counts and
`"invalid_sessions": 0`. Verify the database directly:

The commands below use the default `POSTGRES_USER` and `POSTGRES_DB` values
from `.env.example`. If you changed either value in `.env`, substitute your
configured values in the `psql` commands.

```powershell
docker compose exec postgres-source psql -U gaming -d gaming_source -c "SELECT 'players' AS table_name, COUNT(*) FROM players UNION ALL SELECT 'games', COUNT(*) FROM games UNION ALL SELECT 'platforms', COUNT(*) FROM platforms UNION ALL SELECT 'game_platforms', COUNT(*) FROM game_platforms UNION ALL SELECT 'sessions', COUNT(*) FROM sessions UNION ALL SELECT 'purchases', COUNT(*) FROM purchases UNION ALL SELECT 'achievements', COUNT(*) FROM achievements UNION ALL SELECT 'reviews', COUNT(*) FROM reviews;"
docker compose exec postgres-source psql -U gaming -d gaming_source -c "SELECT COUNT(*) AS invalid_sessions FROM sessions s LEFT JOIN game_platforms gp USING (game_id, platform_id) WHERE gp.game_id IS NULL OR s.ended_at <= s.started_at OR s.duration_minutes <> EXTRACT(EPOCH FROM (s.ended_at - s.started_at)) / 60;"
```

The first query should show the requested counts; the second should return
`0`. PostgreSQL primary keys, unique constraints, checks, and foreign keys
reject malformed or orphaned rows during generation.

### Persistence check

Restarting the container preserves data because it uses
`postgres_source_data`:

```powershell
docker compose restart postgres-source
docker compose exec postgres-source psql -U gaming -d gaming_source -c "SELECT COUNT(*) FROM sessions;"
```

`docker compose down` also preserves the named volume. Avoid
`docker compose down --volumes` unless you intentionally want to delete the
database and all generated rows.

### Troubleshooting

- **`POSTGRES_PASSWORD` is required:** copy `.env.example` to `.env` and set a
  non-empty password.
- **Port 5432 is in use:** set `POSTGRES_PORT` in `.env` to an unused host port
  and update the port in `DATABASE_URL`.
- **Generator cannot connect:** confirm `docker compose ps` reports healthy
  and that the host, port, user, password, and database in `DATABASE_URL`
  match `.env`.
- **Source schema is missing:** check container logs with
  `docker compose logs postgres-source`. The init script runs only on a new
  empty database volume.
- **Tables already contain data:** use `--reset` only when replacing that
  dataset is intended.
- **PowerShell blocks activation:** use
  `.\.venv\Scripts\python.exe -m pip install -r requirements.txt` and then
  `.\.venv\Scripts\python.exe -m ingestion.generate.generate_gaming_data`
  without activating the environment.

### Completion checklist

- [x] Compose configuration validates.
- [x] PostgreSQL becomes healthy and initializes the source schema.
- [x] Generator loads all tables and reports zero invalid sessions and invalid dates.
- [x] Primary/foreign-key and check constraints are active.
- [x] Database rows survive a container restart.

Verified with the default dataset: 10,000 players, 1,000 games, 8 platforms,
2,539 game-platform mappings, 100,000 sessions, 30,000 purchases, 20,000
achievements, and 20,000 reviews. Before Phase 2, the source schema contained
8 tables, 8 primary keys, 11 foreign keys, 10 check constraints, 4 unique
constraints, and 20 indexes.

**Next phase:** Phase 2 — Meltano ingestion. Start only after these checks pass;
first confirm the source tables, keys, and connection details.

## Phase 2 — Meltano PostgreSQL ingestion

### Goal and architecture

Meltano runs in its own Docker execution container. The PostgreSQL tap reads
only the source database's `public` schema and writes Singer records into the
separate analytics PostgreSQL database's `raw` schema:

```text
postgres-source.public
        │ tap-postgres (incremental by updated_at)
        ▼
Meltano state + Singer stream
        │ target-postgres (upsert by primary key)
        ▼
postgres-analytics.raw
```

Each raw stream maps to its source table: `players`, `games`, `platforms`,
`game_platforms`, `sessions`, `purchases`, `achievements`, and `reviews`.
The tap discovers source primary keys; the target uses them for idempotent
upserts. It also adds `_sdc_*` ingestion metadata columns.

### Incremental strategy

All eight streams use the source table's `updated_at` as the replication
bookmark. Players, games, and platforms already had this column. The
Phase 2 migration adds it to the other source tables and backfills historic
rows from their event timestamp; new generator rows initialize it from their
event time. The added indexes support bookmark lookups.

`updated_at` is the bookmark for mutable records and also tracks corrections
to event rows. It is **not CDC**: hard deletes are not replicated, and source
changes without a newer `updated_at` are not detected.

### Prerequisites and files

- Phase 1 source PostgreSQL is healthy and contains the generated dataset.
- Docker Desktop / Compose v2 is running.
- `.env` contains the source database settings from Phase 1.
- Versions are pinned: Meltano 4.4.1, `tap-postgres` 0.10.0, and
  `target-postgres` 0.8.1.

```text
docker/
└── meltano/
    ├── Dockerfile
    └── entrypoint.py
meltano/
└── meltano.yml
sql/
├── 002_meltano_incremental_migration.sql
├── phase2_incremental_demo.sql
└── phase2_incremental_demo_update.sql
```

The entrypoint percent-encodes the source connection URL from environment
variables, so special characters in the password do not break the tap
connection. Credentials are not stored in `meltano.yml`.

### Start and install

Run from the repository root:

```powershell
docker compose config
docker compose up -d --wait postgres-source postgres-analytics
```

For an existing Phase 1 volume, apply the idempotent migration (safe to repeat).
Fresh source volumes apply the numbered SQL files automatically.

```powershell
docker compose exec -T postgres-source sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/002_meltano_incremental_migration.sql'
```

Build the Meltano runner and install the plugins:

```powershell
docker compose build meltano
docker compose run --rm meltano install
```

The `.meltano` directory (installed plugin environments and local state) is
excluded from Git. This uses `INCREMENTAL` replication, not WAL-based
`LOG_BASED` replication, so logical replication configuration is not required.

### Initial load and validation

```powershell
docker compose run --rm meltano run tap-postgres target-postgres
```

On the first run, Meltano reports no prior state, imports the existing rows,
and saves bookmark state. Check RAW counts against the source with this local
PowerShell snippet:

```powershell
@'
from dotenv import dotenv_values
import psycopg

env = dotenv_values(".env")
source = psycopg.connect(env["DATABASE_URL"])
target = psycopg.connect(
    host="localhost",
    port=int(env.get("ANALYTICS_PORT") or 5433),
    user=env["POSTGRES_USER"],
    password=env["POSTGRES_PASSWORD"],
    dbname=env.get("ANALYTICS_DB") or "gaming_analytics",
)
try:
    with source.cursor() as src, target.cursor() as raw:
        for table in (
            "players", "platforms", "games", "game_platforms",
            "sessions", "purchases", "achievements", "reviews",
        ):
            src.execute(f"SELECT COUNT(*) FROM public.{table}")
            source_count = src.fetchone()[0]
            raw.execute(f"SELECT COUNT(*) FROM raw.{table}")
            raw_count = raw.fetchone()[0]
            print(f"{table}: source={source_count}, raw={raw_count}")
            assert source_count == raw_count, table
finally:
    source.close()
    target.close()
'@ | .\.venv\Scripts\python.exe -
```

The initial sync should load 8 streams and 183,547 records for the Phase 1
default dataset. Each table's source and RAW row counts must match.

### Validate incremental inserts and updates

1. Insert a demo game and platform mapping. The script is repeatable.

   ```powershell
   docker compose exec -T postgres-source sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /sql/phase2_incremental_demo.sql'
   docker compose run --rm meltano run tap-postgres target-postgres
   ```

2. Advance the bookmark while updating the game:

   ```powershell
   docker compose exec -T postgres-source sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /sql/phase2_incremental_demo_update.sql'
   docker compose run --rm meltano run tap-postgres target-postgres
   ```

3. Confirm the updated value reached RAW and there is only one row:

   ```powershell
   docker compose exec -T postgres-analytics sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT game_id, developer, COUNT(*) OVER (PARTITION BY game_id) AS copies FROM raw.games WHERE game_id = 9000001;"'
   ```

   Expected: developer is `Incremental Update Confirmed` and `copies` is `1`.
   Run Meltano once more without source changes and confirm row counts remain
   unchanged. The tap may resend rows with the maximum bookmark value; target
   primary-key upsert must prevent duplicates.

### Troubleshooting

- **Tap requires `sqlalchemy_url`:** run Meltano via Compose; its entrypoint
  builds an encoded URL from the source settings. Do not use
  `postgres-source` as a hostname from a locally run Meltano CLI.
- **Connection refused / DNS failure:** ensure both databases are healthy and
  execute Meltano through Compose so it shares the project network.
- **No state / repeated full imports:** keep using the same project and
  `meltano/.meltano` state directory; do not delete it.
- **Updates missing:** update `updated_at` with the business change. This
  incremental pipeline cannot detect deletes or changes that do not advance
  the bookmark.
- **Schema changes:** update `sql/source_schema.sql` and add an explicit
  migration for existing volumes; PostgreSQL init scripts do not rerun for an
  existing data volume.

### Completion checklist

- [x] Separate source and analytics PostgreSQL services are healthy.
- [x] All source tables have non-null, indexed `updated_at` bookmarks.
- [x] Meltano and both PostgreSQL plugins install.
- [x] Initial sync loads all 8 streams into analytics `raw`.
- [x] Meltano reuses state; target upserts avoid duplicate rows.
- [x] Demo insert and subsequent update are reflected in RAW exactly once.

Verified after incremental tests: source and RAW counts match for all streams
(10,001 players, 1,001 games, and 2,540 game-platform rows, including the demo
records), and
game `9000001` has developer `Incremental Update Confirmed` with one RAW row.
The no-change rerun completed and left source/RAW counts unchanged.

**Next phase:** Phase 3 — RAWG API ingestion.

## Phase 3 — RAWG API ingestion

### Goal and architecture

Use the documented RAWG REST API to collect external game metadata and load it
into the `raw` schema of the analytics PostgreSQL database:

```text
RAWG API (key + paginated JSON)
        │ Python stdlib HTTP client
        ▼
validate / retry / normalize selected columns
        │ psycopg upsert + raw JSON payload
        ▼
postgres-analytics.raw.rawg_*
```

The client supports `/games`, `/platforms`, `/genres`, `/publishers`, and
`/developers`. It retains each complete response object in `raw_payload` while
also exposing useful typed columns. RAWG IDs are the primary keys, so reruns
update existing rows rather than inserting duplicates. RAWG does not provide
the Meltano source's `updated_at` bookmark semantics here; rerunnable API
ingestion uses key-based upserts rather than claiming CDC or incremental
change capture.

### Prerequisites and files

- Phase 1/2 Docker services are running and analytics PostgreSQL is healthy.
- `.venv` is installed with project dependencies.
- A RAWG API key is obtained from [RAWG API docs](https://rawg.io/apidocs).
- The API requires a key on every request. Its OpenAPI specification documents
  the paginated endpoints and their `page`, `page_size`, `count`, `results`,
  and `next` fields: [RAWG OpenAPI](https://api.rawg.io/docs/?format=openapi).

```text
ingestion/
└── api/
    └── rawg.py
sql/
└── 003_rawg_schema.sql
tests/
└── test_rawg.py
```

No additional Python package is needed for HTTP requests: the client uses
Python's standard library, plus the project's existing Psycopg and
python-dotenv dependencies.

### Configure the key

Open the ignored local `.env` file and add your own key:

```powershell
code .env
```

Add:

```dotenv
RAWG_API_KEY=your_actual_key
```

Do not paste the key into source files, shell history, screenshots, or Git.
The tracked `.env.example` intentionally leaves `RAWG_API_KEY` blank.

### Run the ingestion

Inspect available options:

```powershell
.\.venv\Scripts\python.exe -m ingestion.api.rawg --help
```

The default ingests all five collections with page size 40 and a maximum of
five pages per endpoint (up to 200 records per collection). This cap is a
project safety limit, not a stated RAWG API page-size maximum or request
quota. The client applies this conservative limit because the official
OpenAPI page-size parameter does not publish a maximum. Adjust pages or run
only selected collections explicitly:

```powershell
.\.venv\Scripts\python.exe -m ingestion.api.rawg --resources games --page-size 40 --max-pages 2
.\.venv\Scripts\python.exe -m ingestion.api.rawg --resources platforms genres publishers developers --max-pages 3
```

`rawg.py` creates the schema and tables from `sql/003_rawg_schema.sql` before
fetching. It connects to the analytics database on localhost using
`ANALYTICS_PORT`, `ANALYTICS_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD`
from `.env`.

### Reliability and data quality

- Sends the API key on each HTTPS request; credentials and full request URLs
  are not written to logs.
- Requests the documented collection paths without a trailing slash
  (`/api/games`, `/api/platforms`, etc.); the trailing-slash form returned 404
  during API validation.
- Follows the documented `next` page, but only when the returned URL is HTTPS
  on `api.rawg.io` and matches the requested collection.
- Retries HTTP 429 and transient server/network errors up to five times.
  `Retry-After` is respected up to the five-minute safety limit; a longer
  server delay stops the run with an explicit error.
- Uses a 30-second request timeout and logs resource, page, and record counts.
- Validates response shape, positive IDs, non-empty names, and date values
  before loading them.
- Upserts each successfully fetched page. If a later request fails, rerunning
  safely upserts already loaded records and continues from page 1.
- Does not guarantee deletions from RAWG are reflected locally; this API
  ingestion is a repeatable snapshot of the pages requested.

### Expected output and validation

Successful runs print a JSON report showing pages and upserted records by
collection. Check RAWG table totals in analytics PostgreSQL:

```powershell
docker compose exec -T postgres-analytics sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT (SELECT COUNT(*) FROM raw.rawg_games) AS rawg_games, (SELECT COUNT(*) FROM raw.rawg_platforms) AS rawg_platforms, (SELECT COUNT(*) FROM raw.rawg_genres) AS rawg_genres, (SELECT COUNT(*) FROM raw.rawg_publishers) AS rawg_publishers, (SELECT COUNT(*) FROM raw.rawg_developers) AS rawg_developers;"'
```

Check that all rows have IDs, names, preserved payloads, and fetch timestamps:

```powershell
docker compose exec -T postgres-analytics sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) AS invalid_games FROM raw.rawg_games WHERE rawg_id IS NULL OR name IS NULL OR raw_payload IS NULL OR fetched_at IS NULL;"'
```

Expected invalid row count: `0`. Rerun the same command; per-resource row
counts should not grow merely because the same RAWG IDs are fetched again.

### Attribution and usage

RAWG's API documentation requires attribution for personal use and an active
RAWG hyperlink wherever its data or images are used. Preserve that attribution
in any downstream dashboard, report, or published documentation that displays
RAWG data. Review the current [RAWG API terms](https://rawg.io/apidocs) before
publishing or using the data beyond this learning project.

### Troubleshooting

- **`RAWG_API_KEY is missing`:** add the key to local `.env`; do not commit it.
- **HTTP 401/403:** verify the key is valid and active. The client reports the
  status without logging the secret-bearing URL.
- **HTTP 429 or transient 5xx:** the client retries within bounded limits. If
  it stops after a long `Retry-After`, wait and rerun.
- **Connection timeout:** check network access and rerun; successful pages are
  safe to ingest again.
- **RAW schema/database unavailable:** run
  `docker compose up -d --wait postgres-analytics` and confirm the `.env`
  analytics settings and port.
- **Invalid response or pagination URL:** the client stops rather than
  following an unexpected host or silently skipping malformed data.

### Completion checklist

- [x] Official endpoints, key requirement, and page response structure verified.
- [x] Python client, RAW schema, page limits, retry logic, validation, and upsert implemented.
- [x] Unit tests cover pagination validation, malformed records, date conversion, and HTTP 429 retry.
- [x] Analytics schema creation and primary-key upsert/update tested against PostgreSQL.
- [x] Authenticated live API ingestion completed for all five collections.
- [x] RAW table contents, unique IDs, non-null payloads, and rerun idempotency validated.

**Verified bounded snapshot (2026-10-26):** games 200, platforms 51, genres 19,
publishers 200, developers 200. The API-reported totals for games, publishers,
and developers are much larger than this project snapshot; the default
five-page cap intentionally does not download the full RAWG catalog. A
second complete run kept the same distinct-ID counts and all rows retained
their payload and fetch timestamp.

**Phase 3 status: Complete.** RAWG API ingestion is operational for the
configured bounded collection sample. Preserve the required RAWG attribution
when displaying its data. **Next phase:** Phase 4 — Excel business data
ingestion.

## Phase 4 — Excel business data ingestion

### Goal and architecture

The three synthetic marketing/business workbooks are generated against the
game IDs already present in analytics `raw.games`, validated together, then
loaded as a consistent snapshot:

```text
data/business/*.xlsx
        │ openpyxl read-only + strict validation
        ▼
deduplicate / cross-file and game-ID checks / SHA-256 change detection
        │ one PostgreSQL transaction
        ▼
raw.raw_marketing_campaigns
raw.raw_game_targets
raw.raw_campaign_budget
```

Campaign budgets must agree between `marketing_campaigns.xlsx` and
`budget.xlsx`; every referenced game must exist in `raw.games`. `budget`
references campaign, and campaigns/targets reference games. Each load replaces
the three related tables as a unit only after all inputs pass validation.
Identical duplicate primary-key rows are skipped and counted; conflicting
duplicates, empty workbooks, unknown games, and inconsistent budgets fail
without replacing the existing snapshot.

### Workbook contracts

| File | Required columns | Primary key |
|---|---|---|
| `marketing_campaigns.xlsx` | `campaign_id`, `game_id`, `campaign`, `budget`, `start_date`, `end_date` | `campaign_id` |
| `game_targets.xlsx` | `game_id`, `target_revenue`, `target_players`, `target_sessions`, `target_rating` | `game_id` |
| `budget.xlsx` | `campaign_id`, `budget`, `actual_spend` | `campaign_id` |

Dates are read as Excel dates or ISO date strings. IDs and whole-number targets
must be integers. Budgets, spend, and revenue targets are nonnegative;
campaign end date cannot precede start date; target rating is 0–5. Header names
and order must match the contract exactly. Entirely blank rows are skipped.
Formula cells without a stored calculated value are not accepted as populated
business values; supply values rather than formulas in these input files.

### Files

```text
data/
└── business/
    ├── marketing_campaigns.xlsx
    ├── game_targets.xlsx
    └── budget.xlsx
ingestion/
└── excel/
    ├── generate_business_data.py
    └── load_excel.py
sql/
└── 004_excel_business_schema.sql
tests/
└── test_excel_ingestion.py
```

`openpyxl` 3.1.5 is pinned to the compatible 3.1.x release line through
`requirements.txt`. The loader explicitly uses its supported `.xlsx` reader
with `read_only=True` and `data_only=True`.

### Generate the synthetic files

Phase 2 must have loaded source games into analytics `raw.games`. Create
reproducible business values using source game IDs:

```powershell
.\.venv\Scripts\python.exe -m ingestion.excel.generate_business_data
```

This creates 100 campaigns, one target row for every game currently in
`raw.games`, and one budget/spend row for each campaign. To generate a
different number of campaigns:

```powershell
.\.venv\Scripts\python.exe -m ingestion.excel.generate_business_data --seed 42 --campaign-count 50
```

The generator replaces only the three named workbook files under
`data/business`. It does not change source or RAW database records.

### Load Excel into analytics RAW

Start the analytics database, then run the loader:

```powershell
docker compose up -d --wait postgres-analytics
.\.venv\Scripts\python.exe -m ingestion.excel.load_excel
```

The first load creates the RAW tables, validates the workbook headers/types,
keys and cross-file rules, checks that game IDs exist, then transactionally
replaces the existing Excel snapshot. It records each file's SHA-256, row
count, and load timestamp in `raw.business_file_loads`.

The loader computes file hashes on each run. If all three validated file
hashes match the last successful load, it reports `skipped_unchanged` and
leaves the current RAW snapshot untouched. When a file changes, all three
workbooks are validated and the coordinated snapshot is replaced together.
`--force` explicitly reloads the same snapshot:

```powershell
.\.venv\Scripts\python.exe -m ingestion.excel.load_excel --force
```

This is a whole-file snapshot process: deleting a row from a workbook removes
that row from RAW on the next changed-file load. Keep backups or version the
input workbooks if you need to review prior business plans.

### Validate

The default generated files contain 100 campaigns, 1,001 game targets, and
100 campaign budgets (the actual target count follows the number of rows in
`raw.games`). Check loaded counts, referential integrity and last-loaded file
hashes:

```powershell
docker compose exec -T postgres-analytics sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT (SELECT COUNT(*) FROM raw.raw_marketing_campaigns) AS campaigns, (SELECT COUNT(*) FROM raw.raw_game_targets) AS game_targets, (SELECT COUNT(*) FROM raw.raw_campaign_budget) AS budgets;" -c "SELECT COUNT(*) AS orphan_campaign_games FROM raw.raw_marketing_campaigns c LEFT JOIN raw.games g USING (game_id) WHERE g.game_id IS NULL;" -c "SELECT COUNT(*) AS orphan_target_games FROM raw.raw_game_targets t LEFT JOIN raw.games g USING (game_id) WHERE g.game_id IS NULL;" -c "SELECT COUNT(*) AS mismatched_budgets FROM raw.raw_marketing_campaigns c JOIN raw.raw_campaign_budget b USING (campaign_id) WHERE c.budget <> b.budget;" -c "SELECT file_name, row_count, sha256, loaded_at FROM raw.business_file_loads ORDER BY file_name;"'
```

Expected: 100 campaigns, 1,001 targets, 100 budgets, all integrity counts zero,
and three manifest rows. Then run the loader again; it should report
`skipped_unchanged`. Re-run the SQL checks to confirm row counts did not
change.

### Tests and troubleshooting

Focused tests:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

- **Missing or invalid workbook:** check exact filenames, `.xlsx` format,
  required worksheet names, and exact ordered headers.
- **Duplicate ID:** identical duplicate rows are discarded; conflicting
  business values for a repeated primary key are rejected.
- **Unknown game:** regenerate the files after the intended games exist in
  analytics `raw.games`, or correct the workbook's `game_id`.
- **Budget mismatch:** make the value in `budget.xlsx` equal the matching
  campaign's budget.
- **Unchanged file was skipped:** use `--force` only if you intentionally need
  to reapply the same snapshot.
- **Load error:** validation runs before database replacement; PostgreSQL
  applies the replacement and manifest update in one transaction. The prior
  snapshot remains if the replacement fails.

### Completion checklist

- [x] Three synthetic Excel workbooks generated using existing RAW game IDs.
- [x] Strict schema/type/date/range validation and documented duplicate policy.
- [x] Cross-file campaign/budget checks and game referential validation.
- [x] RAW schema and file-hash manifest created.
- [x] Initial snapshot loads atomically; identical reruns are detected and skipped.
- [x] Workbook change triggers a validated, idempotent snapshot replacement.
- [x] Unit tests and database row-count/integrity queries pass.

**Phase 4 status: Complete.** **Next phase:** Phase 5 — dbt staging models.

## Phase 5 — dbt staging models

### Goal and architecture

Use dbt to expose consistent, typed staging views over the existing analytics
`raw` schema without changing or reloading source data:

```text
raw (Meltano + RAWG + Excel)
        │ dbt sources and staging SELECTs
        ▼
staging (16 views, soft-deleted Meltano rows excluded)
        │ dbt generic and singular data tests
        ▼
validated staging layer for downstream analytics
```

The project covers all 16 data tables: eight Meltano operational tables, five
RAWG tables, and three Excel business tables. Meltano's `_sdc_*` metadata is
not exposed in staging; rows with `_sdc_deleted_at` set are excluded.
Staging models cast fields to explicit PostgreSQL types and otherwise preserve
the source values. Models are views, so rerunning dbt does not duplicate or
copy the RAW snapshots.

### Files and setup

```text
dbt/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── sources.yml
│   └── staging/
│       ├── stg_*.sql
│       └── staging.yml
└── tests/
    ├── campaign_budget_matches.sql
    ├── campaign_dates_valid.sql
    ├── game_targets_valid.sql
    ├── session_platforms_exist.sql
    └── session_timestamps_valid.sql
```

dbt Core and the PostgreSQL adapter are constrained to the compatible 1.11
release line in `requirements.txt`. Install them in the project virtual
environment if they are not already present:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

`dbt/profiles.yml` reads the database credentials from environment variables;
it contains no password. In PowerShell, load the values from your local,
git-ignored `.env` into the current session, then run dbt from the project root:

```powershell
$localEnv = Get-Content .env -Raw | ConvertFrom-StringData
$env:POSTGRES_USER = $localEnv.POSTGRES_USER
$env:POSTGRES_PASSWORD = $localEnv.POSTGRES_PASSWORD
$env:ANALYTICS_DB = $localEnv.ANALYTICS_DB
$env:ANALYTICS_PORT = $localEnv.ANALYTICS_PORT
$env:DBT_HOST = "localhost"

.\.venv\Scripts\dbt.exe debug --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe run --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe test --project-dir dbt --profiles-dir dbt
```

The profile writes views into the `staging` schema of the analytics database.
The configured user needs permission to create schemas and views there. The
RAW source tables are read-only inputs to the project.

### Tests and validation

The generic tests verify primary keys, required fields, accepted payment
methods and ratings, and relationships between staging models. Singular tests
check that session game/platform pairs exist and validate session date ranges,
campaign date ranges, campaign/budget agreement, and game-target ranges.
Inspect the generated model/test inventory or compile SQL with:

```powershell
.\.venv\Scripts\dbt.exe ls --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe compile --project-dir dbt --profiles-dir dbt
```

### Completion checklist

- [x] dbt Core and PostgreSQL adapter installed and connection profile verified.
- [x] All 16 RAW data tables declared as dbt sources.
- [x] Sixteen typed staging views created, excluding Meltano soft-deleted rows.
- [x] Generic key, null, domain, and relationship tests defined.
- [x] Singular session, date, target-range, and campaign-budget integrity tests defined.
- [x] dbt parse, run, and all data tests pass against analytics PostgreSQL.

**Phase 5 status: Complete.** **Next phase:** Phase 6 — core analytics models.

## Phase 6 — core analytics models

### Goal and architecture

Build a star-schema-style analytics layer on top of the validated staging
models. The models are stored as tables in the `staging_core` schema (dbt
combines the target `staging` schema with the configured `core` custom schema):

```text
staging dimensions ─┬─> fact_sessions       (one row per session)
                    ├─> fact_purchases      (one row per purchase)
                    ├─> fact_reviews        (one row per review)
                    ├─> fact_achievements   (one row per achievement)
                    └─> fact_campaigns      (one row per campaign)

dim_games ── bridge_game_platforms ── dim_platforms
dim_date, dim_players, dim_games, dim_platforms ──> event facts
```

`dim_players`, `dim_games`, and `dim_platforms` expose descriptive attributes;
game targets are optional attributes on `dim_games`. `dim_date` covers the
continuous date range represented by session starts, purchases, reviews,
achievements, and campaign boundaries. Event facts retain their original event
timestamps and include `date_day` for the date relationship.

`fact_sessions` preserves the source `duration_minutes` and separately exposes
elapsed seconds calculated from the start/end timestamps. `fact_purchases`
stores purchase amount in the source currency without currency conversion.
`fact_campaigns` joins the validated campaign and budget workbook records and
calculates remaining budget and spend-to-budget ratio; the ratio is null when
the budget is zero. The RAWG catalog is not joined to synthetic `games`
because their IDs are from unrelated systems.

### Run models and tests

From the project root, load `.env` variables as documented in Phase 5, then
run:

```powershell
.\.venv\Scripts\dbt.exe run --project-dir dbt --profiles-dir dbt --select path:models/core
.\.venv\Scripts\dbt.exe test --project-dir dbt --profiles-dir dbt
```

Core tests enforce unique dimension and fact keys, required fact attributes,
dimension relationships, unique game/platform pairs, and valid session
game/platform combinations. Re-running dbt replaces the core tables from
current staging data and does not modify the RAW tables.

### Completion checklist

- [x] Player, game, platform, and continuous date dimensions created.
- [x] Game/platform bridge created with unique pair validation.
- [x] Session, purchase, review, achievement, and campaign facts created at documented grains.
- [x] Fact keys, dimension relationships, and game/platform integrity tested.
- [x] All core models built and core data tests pass against analytics PostgreSQL.
- [x] Full dbt data-test suite passes.

**Phase 6 status: Complete.** **Next phase:** Phase 7 — Airflow orchestration.

## Phase 7 — Airflow orchestration

### Goal and workflow

Apache Airflow 3.3.2 runs in the optional Compose `orchestration` profile.
The `gaming_analytics_pipeline` DAG runs daily at 02:00 Mongolia time
(`Asia/Ulaanbaatar`) and can also be triggered manually:

```text
check_databases
    -> sync_source_to_raw (Meltano incremental sync)
    -> ingest_rawg (bounded pages from all five RAWG collections)
    -> load_business_workbooks (validate and load existing Excel files)
    -> build_dbt_models
    -> test_dbt_models
```

Airflow does not generate or overwrite business workbooks. The task sequence
uses the existing `.xlsx` inputs and retries failed tasks once. RAWG usage is
bounded by `RAWG_MAX_PAGES` (default 5). The UI binds to `127.0.0.1` only;
Airflow metadata and Meltano bookmark/plugin state are stored in named Docker
volumes. Airflow uses its local SQLite metadata DB and LocalExecutor,
appropriate for this single-user learning deployment rather than production
or high availability. Airflow 3 serves its UI and API from the API server.

### Start Airflow

Set `RAWG_API_KEY` and a unique `AIRFLOW__API_AUTH__JWT_SECRET` in the local,
git-ignored `.env` before starting Airflow. Generate the JWT secret locally
with `python -c "import secrets; print(secrets.token_urlsafe(64))"` and keep it
private; the same value must be retained across restarts. Optionally set
`AIRFLOW_PORT` (default `8080`) and `RAWG_MAX_PAGES` there.
Build and start the orchestration profile:

```powershell
docker compose --profile orchestration up -d --build --wait airflow
docker compose ps
docker compose exec airflow airflow dags list
```

Airflow 3.3.2 upgrades the metadata database from the previous Airflow 2
version. Before upgrading an existing deployment, stop Airflow and back up the
`airflow_home` named volume. Then build the new image, run the Airflow 3
database migration, and start the service:

```powershell
docker compose --profile orchestration stop airflow
docker compose --profile orchestration build airflow
docker compose --profile orchestration run --rm --entrypoint airflow airflow db migrate
docker compose --profile orchestration up -d --wait airflow
```

The Airflow health endpoint is
`http://localhost:8080/api/v2/monitor/health`.

On first start, Airflow creates a local admin login and saves its generated
password in the Airflow home volume. Read it when needed:

```powershell
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

Open `http://localhost:8080`, sign in as `admin`, and trigger
`gaming_analytics_pipeline`. It can also be started with the CLI:

```powershell
docker compose exec airflow airflow dags trigger gaming_analytics_pipeline
docker compose logs --follow airflow
```

The orchestration container connects to Compose services by their internal
names; local ingestion defaults remain `localhost` for existing manual use.
Stop the Airflow service without deleting its metadata or Meltano state:

```powershell
docker compose --profile orchestration stop airflow
```

### Validation and troubleshooting

The DAG uses the existing tested CLI entry points and dbt project. Parse the
DAG and inspect its task graph before triggering it:

```powershell
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow tasks list gaming_analytics_pipeline
docker compose exec airflow airflow dags show gaming_analytics_pipeline
```

- **Airflow is unhealthy:** check `docker compose --profile orchestration logs airflow` and
  `docker compose ps`; startup initializes Airflow's local metadata DB.
- **DAG import error:** rebuild after DAG or dependency changes with
  `docker compose --profile orchestration build airflow`.
- **RAWG task fails:** verify the local `.env` API key and the configured page
  cap; failed API requests are reported by the existing RAWG client.
- **Excel task fails:** correct or restore the three expected workbook files
  under `data/business`; they are validated before the RAW snapshot changes.
- **Database connection fails:** confirm both PostgreSQL services are healthy
  and that `.env` database credentials are correct.

### Completion checklist

- [x] Airflow Compose profile starts and becomes healthy.
- [x] Airflow imports the DAG without errors and exposes all six tasks.
- [x] The database reachability task succeeds inside the Airflow container.
- [x] A manual pipeline run succeeds from database checks through dbt tests.
- [x] Source and active RAW counts match for all eight Meltano streams.
- [x] Excel manifest, RAWG collections, and core model tables verified after the run.

**Verified manual run (2026-10-26):** all six tasks succeeded. Meltano loaded
the eight operational streams with source and active RAW counts matching;
RAWG ingested its bounded sample (games 200, platforms 51, genres 19,
publishers 200, developers 200); and the Excel loader confirmed its three
unchanged workbook snapshots. dbt rebuilt 26 staging/core models and all 129
data tests passed. RAWG row counts reflect distinct IDs currently stored;
ingestion upserts the bounded page sample by RAWG ID, so repeated runs update
matching IDs rather than duplicating them.

**Phase 7 status: Complete.** **Next phase:** Phase 8 — OpenMetadata setup and
metadata ingestion.

## Phase 8 — OpenMetadata setup and metadata ingestion

### Goal and scope

OpenMetadata runs in the optional Compose `openmetadata` profile and catalogs
the analytics PostgreSQL database. The profile uses the official OpenMetadata
2.0.5 server/database/ingestion images and OpenSearch 3.4.0. Ingestion is
run externally using OpenMetadata's version-matched ingestion package; the
main Airflow profile and the existing PostgreSQL data volumes remain separate.

```text
postgres-analytics (raw, staging, staging_core)
        │ OpenMetadata PostgreSQL metadata workflow
        ▼
OpenMetadata catalog ── MySQL metadata store
                   └── Elasticsearch search index

dbt/target/manifest.json ── OpenMetadata dbt artifact ingestion
```

This is a local learning deployment, not a production configuration. The
OpenMetadata server UI binds to `127.0.0.1`; OpenSearch has no host port
mapping and has security disabled only on the isolated Compose network. Keep
`.env` private. OpenMetadata's production guidance recommends substantially
more resources than a local quickstart; this machine's Docker allocation is
limited, so start this profile only when sufficient memory is available.

### Configure and start OpenMetadata

Copy the OpenMetadata settings into `.env` if they are not present already.
Choose a non-default local MySQL root password. After the server starts,
create an ingestion bot in OpenMetadata and put its JWT in
`OPENMETADATA_JWT_TOKEN` in `.env`. Do not commit or print the token.

```powershell
docker compose config --quiet
docker compose --profile openmetadata up -d --wait openmetadata-server
docker compose ps
Invoke-RestMethod http://localhost:8585/healthcheck
```

The UI is at `http://localhost:8585`. Check startup or migration failures
with:

```powershell
docker compose logs openmetadata-migrate
docker compose logs openmetadata-server
```

### Ingest PostgreSQL and dbt metadata

The PostgreSQL workflow registers the `gaming-analytics` database service and
ingests tables and views from `raw`, `staging`, and `staging_core`. It reads
the existing database connection values from `.env`; it does not load or
modify data.

```powershell
docker compose --profile openmetadata run --rm openmetadata-ingestion
```

To add dbt model metadata and lineage, first regenerate dbt artifacts after a
successful model build. Follow the Phase 5 PowerShell instructions to load the
database variables from `.env`, then run:

```powershell
.\.venv\Scripts\dbt.exe docs generate --project-dir dbt --profiles-dir dbt
docker compose --profile openmetadata run --rm --entrypoint metadata openmetadata-ingestion ingest-dbt -c /project/dbt
```

The dbt project reads OpenMetadata URL, JWT, and service-name values from
environment variables, so the token stays in `.env` rather than in tracked
project configuration. `manifest.json` is required; `catalog.json` from
`dbt docs generate` is recommended. The database service must be ingested
before the dbt workflow so it can associate models with catalog tables.

Confirm the `gaming-analytics` service and its schemas/models in the UI.
Stop the optional services without deleting catalog state with:

```powershell
docker compose --profile openmetadata stop openmetadata-server openmetadata-mysql openmetadata-opensearch
```

Named volumes preserve OpenMetadata's MySQL database and OpenSearch
indexes. Do not use `docker compose down --volumes` unless deleting all project
database and catalog data is intentional.

### Completion checklist

- [x] OpenMetadata 2.0.5 server, MySQL, and OpenSearch 3.4.0 start; the system
  version API returns HTTP 200.
- [x] PostgreSQL metadata ingestion succeeds for `raw`, `staging`, and
  `staging_core`.
- [x] dbt artifacts are ingested, and catalog lineage connects core models to
  staging and RAW assets.

**Verified ingestion:** PostgreSQL metadata ingestion completed with 100%
workflow success and zero errors. The catalog API returned 43 assets across
the three schemas (`raw`: 17, `staging`: 16, `staging_core`: 10). `dbt docs
generate` created `catalog.json`; dbt artifact ingestion completed with 100%
workflow success, zero errors, and 108 ingested assets / 108 model links.
OpenMetadata API lineage lookups for `fact_sessions`, `dim_players`, and
`fact_campaigns` each returned upstream edges to the corresponding staging
and/or RAW tables. The dbt workflow reported 42 owner warnings because the
dbt profile's `user_gaming` account does not exist as an OpenMetadata user;
this did not block ingestion or lineage.

**Phase 8 status: Complete.** At the time of the catalog setup, Airflow was
stopped to fit within the available Docker memory while OpenMetadata was
running. These optional profiles can still contend for memory; stop the
OpenMetadata services before starting Airflow on a memory-constrained machine.
See the [project handoff](docs/PROJECT_HANDOFF.md) for current operating
guidance. **Next phase at that point:** Phase 9 — data quality and monitoring.

## Phase 9 — data quality and monitoring

### Goal and monitoring workflow

After the existing manual ingestion/dbt pipeline, the DAG checks the health
of all 43 RAW, staging, and core relations, records a persistent snapshot, and
sends a summary email. Existing dbt data tests remain the authoritative
integrity checks and run before the monitoring task:

```text
Meltano + RAWG + Excel ingestion
        -> dbt run -> dbt test
        -> data quality monitor
        -> email quality report
```

The monitor checks non-empty relations, compares row counts with the prior
snapshot, and measures RAW freshness using Meltano `_sdc_extracted_at`, RAWG
`fetched_at`, and Excel `loaded_at`. It writes one run and per-relation
snapshots to the analytics database's `monitoring` schema. The first run is a
baseline for volume comparison; subsequent changes above 50% are warnings.
RAW timestamps warn after 48 hours and fail after 168 hours; Excel snapshot
timestamps warn after 30 days and fail after 60 days. Thresholds can be
overridden in `.env`. Freshness is based on the latest loaded row timestamp,
so a run that did not refresh a source is surfaced rather than silently
treated as fresh.

### Configure email

Set the following in the local `.env` before triggering the Airflow pipeline:

```dotenv
AIRFLOW__SMTP__SMTP_HOST=smtp.example.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_USER=your-smtp-user
AIRFLOW__SMTP__SMTP_PASSWORD=your-smtp-password
AIRFLOW__SMTP__SMTP_MAIL_FROM=gaming-analytics@example.com
AIRFLOW__SMTP__SMTP_STARTTLS=true
AIRFLOW__SMTP__SMTP_SSL=false
QUALITY_ALERT_EMAIL_TO=recipient@example.com
```

Use the SMTP provider's supported authentication and TLS settings; keep
credentials in the ignored `.env`, never in the repository. The notification
task reports dbt test task state, data-quality status, and any relation-level
freshness or volume warnings. Missing SMTP host, sender, or recipient is an
explicit task error, not a silently skipped email.

### Run and validate

Stop OpenMetadata before starting Airflow if Docker memory is constrained;
the named OpenMetadata volumes retain the catalog. Start the existing
orchestration profile and trigger `gaming_analytics_pipeline` as documented
in Phase 7. The graph now ends with `monitor_data_quality` and
`email_data_quality_report`.

The monitor can also be exercised locally after loading PostgreSQL settings
from `.env` into PowerShell (`POSTGRES_USER`, `POSTGRES_PASSWORD`,
`ANALYTICS_DB`, `ANALYTICS_PORT`) and setting `ANALYTICS_HOST=localhost`:

```powershell
.\.venv\Scripts\python.exe -m monitoring.data_quality
docker compose exec -T postgres-analytics psql -U $env:POSTGRES_USER -d $env:ANALYTICS_DB -c "SELECT run_id, checked_at, status, passed_count, warning_count, failed_count FROM monitoring.data_quality_runs ORDER BY checked_at DESC LIMIT 10;"
```

Focused monitor unit tests:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_data_quality.py -v
```

### Completion checklist

- [x] Data quality checks cover non-empty relations, freshness timestamps,
  and row-volume changes across all 43 relations.
- [x] Quality snapshots persist in analytics PostgreSQL; the first real
  monitoring run passed all 43 checks with zero warnings and failures.
- [x] Unit tests cover freshness thresholds, baseline comparisons, volume
  anomalies, and empty relation failures.
- [x] Existing Airflow pipeline includes post-dbt monitoring and email
  notification tasks.
- [x] Configure SMTP in the ignored `.env` and verify the email task succeeds
  during a complete Airflow pipeline run.

**Phase 9 status: Complete.** The manual Airflow run completed all eight tasks
successfully. Its persisted quality-monitor result passed all 43 relations with
zero warnings and failures, and the email task reported a successful send
through the configured SMTP backend.

## Phase 10 — GitHub Actions CI

### Goal and workflow

Run the project's automated checks on pushes, pull requests, and manual
workflow dispatches. The workflow uses Python 3.11 and validates Python
syntax, the unit-test suite, dbt project parsing, and Docker Compose
configuration:

```text
Python compile check -> unit tests -> dbt parse -> Compose config validation
```

The CI job does not start databases, call RAWG, send email, or deploy
infrastructure. Its dbt profile uses placeholder credentials only for parsing.
The Compose check copies `.env.example` to a temporary `.env` in the runner;
do not add actual credentials or API keys to the workflow.

### Run the same checks locally

From the project root in PowerShell:

```powershell
python -m compileall -q ingestion monitoring tests
python -m unittest discover -s tests -v
$env:DBT_HOST = "localhost"
$env:ANALYTICS_PORT = "5433"
$env:POSTGRES_USER = "gaming"
$env:POSTGRES_PASSWORD = "ci-only-placeholder"
$env:ANALYTICS_DB = "gaming_analytics"
$env:OPENMETADATA_HOST_PORT = "http://localhost:8585/api"
$env:OPENMETADATA_JWT_TOKEN = "ci-only-placeholder"
$env:OPENMETADATA_SERVICE_NAME = "gaming-analytics-ci"
dbt parse --no-partial-parse --project-dir .\dbt --profiles-dir .\dbt
docker compose config --quiet
```

### Completion checklist

- [x] Add GitHub Actions validation for pushes, pull requests, and manual runs.
- [x] Keep CI checks independent of production secrets and live services.
- [x] Validate the equivalent checks locally: 22 unit tests, Python compilation,
  dbt parse, Compose configuration, and workflow YAML parsing.

**Phase 10 status: Complete.** CI configuration and its equivalent local
validation checks are complete. A hosted GitHub Actions run is not required
for this phase's completion.

## Phase 11 — Scheduled pipeline operations

The Airflow DAG runs daily at 02:00 `Asia/Ulaanbaatar` time. The timezone is
explicitly set on the DAG, and `catchup=False` prevents a backlog of historical
runs when Airflow is stopped. Manual triggering remains available. Each
scheduled run executes Meltano source sync, the bounded RAWG API ingestion,
the Excel snapshot loader, dbt build/tests, data-quality monitoring, and the
configured email report. Set `RAWG_API_KEY` and the SMTP settings in the
git-ignored `.env` before enabling the Airflow orchestration profile. Be aware
that these are real ingestion/API and email operations, not a dry run.

Verify the schedule and next run after starting Airflow:

```powershell
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags next-execution gaming_analytics_pipeline
docker compose exec airflow airflow dags list-import-errors
```

### Completion checklist

- [x] Schedule the full pipeline daily at 02:00 Mongolia time.
- [x] Keep manual triggers, retries, and single-active-run protection.
- [x] Disable catchup so a stopped instance does not enqueue historical runs.
- [x] Observe a scheduled run complete successfully, including quality
  monitoring and SMTP notification.

**Phase 11 status: Complete.** Airflow is healthy, its scheduler heartbeat is
active, and the first scheduled run completed all eight tasks successfully.
The persisted quality result passed all 43 relations with zero warnings and
failures, and the SMTP email task confirmed delivery.

## Phase 12 — Streamlit analytics dashboard

The optional `dashboard` Compose profile serves a local Streamlit app at
`http://localhost:8501`. It reads the dbt `staging_core` facts and dimensions
for engagement, purchase value, and player feedback. Filters include date
range, game, and purchase currency. Purchase amounts are never combined across
currencies. The dashboard connection is set to read-only transactions, and the
published port binds to `127.0.0.1`.

Start the dashboard and its analytics database dependency from PowerShell:

```powershell
docker compose --profile dashboard up -d --build --wait dashboard
docker compose ps dashboard postgres-analytics
```

Open `http://localhost:8501`. The analytics models must have been built by the
pipeline first; otherwise, the app explains that core data is not available.
Refresh the page after the scheduled pipeline updates the models. Stop only
the dashboard without removing database volumes:

```powershell
docker compose --profile dashboard stop dashboard
```

### Completion checklist

- [x] Add an optional local Streamlit dashboard profile and health check.
- [x] Report sessions, active players, session duration, currency-aware
  purchases, platform engagement, and game ratings from dbt core models.
- [x] Restrict dashboard DB transactions to read-only and bind the UI locally.
- [x] Build the dashboard container and verify its live database-backed render.

**Phase 12 status: Complete.** The dashboard container is healthy, its web and
health endpoints return HTTP 200, and a live Streamlit render showed all six
metrics, four charts, and the game ratings table with no app exceptions.

## Phase 13 — Project documentation and handoff

The handoff guide is in [`docs/PROJECT_HANDOFF.md`](docs/PROJECT_HANDOFF.md).
It summarizes the architecture and data flow, Compose profiles, setup and
operations, validation commands, environment secret handling, and known
deployment limitations.

### Completion checklist

- [x] Document the as-built architecture and end-to-end data flow.
- [x] Provide startup, scheduled-run, dashboard, shutdown, and troubleshooting
  instructions.
- [x] Record security boundaries, resource constraints, and known limitations.

**Phase 13 status: Complete.** The project handoff guide is available alongside
the phase-by-phase README.

## Phase 14 — Local deployment security hardening

The PostgreSQL source and analytics host ports are bound to `127.0.0.1`, like
the Airflow and dashboard UIs. This keeps those published ports unavailable
from other network devices by default while preserving host-local development
connections such as `localhost:5432` and `localhost:5433`. Containers on the
Compose network continue to connect to PostgreSQL by service name.

### Completion checklist

- [x] Review the local Compose security posture without exposing `.env` values.
- [x] Bind both PostgreSQL host ports to loopback.
- [x] Update handoff documentation and validate the effective Compose port
  bindings.

**Phase 14 status: Complete.** PostgreSQL host bindings are restricted to
loopback; the container-to-container data pipeline remains unchanged.

## Phase 15 — Local production-readiness hardening

This phase applies safe defense-in-depth controls to the local Compose
deployment; it does not convert the learning stack into a production service.
The dashboard receives only the analytics database connection variables
instead of the complete `.env`, drops all Linux capabilities, and runs with
`no-new-privileges`. Its image already runs as a non-root user, and its
PostgreSQL transactions remain read-only.

### Completion checklist

- [x] Limit dashboard environment variables to its database connection.
- [x] Drop dashboard Linux capabilities and prevent privilege escalation.
- [x] Verify the dashboard can still query analytics data and passes health
  checks after the Compose update.
- [x] Document the remaining production-readiness gaps.

**Phase 15 status: Complete for local hardening only.** This project is not
production-ready: Airflow uses SQLite and `LocalExecutor`,
database services use a shared administrative account, and there is no
configured TLS termination, external secret manager, HA, or tested
disaster-recovery process. Do not expose it as a production service.
