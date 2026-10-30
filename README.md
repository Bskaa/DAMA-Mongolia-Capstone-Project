# Gaming Analytics Data Platform

An end-to-end, local data platform for ingesting, transforming, validating, cataloging, and presenting gaming analytics data. The project is designed as a hands-on learning implementation of a modern analytics workflow rather than as a production deployment.

It combines a synthetic operational source, a public games API, and business-maintained Excel workbooks into a PostgreSQL analytics warehouse. dbt creates a governed analytics layer, Airflow schedules the pipeline, data-quality checks monitor it, and Streamlit presents the resulting metrics.

## Project Overview

The platform has three data inputs:

- A synthetic PostgreSQL operational database containing players, games, sessions, purchases, achievements, reviews, and supported platforms.
- The RAWG public API, which provides a bounded catalog of external game metadata.
- Excel workbooks containing marketing campaigns, budgets, and game targets.

All inputs land in the analytics database's `raw` schema. dbt then creates typed `staging` views and star-schema-style tables in `staging_core`. The dashboard queries only the core layer, not operational or raw tables.

```text
                     ┌────────────────────────────────────┐
                     │          Source systems            │
                     │                                    │
                     │  Synthetic PostgreSQL  RAWG  Excel │
                     └──────────────┬─────────────┬───────┘
                                    │             │
                   Meltano sync ───┘             └── Python loaders
                                    │
                                    v
                  PostgreSQL analytics database
                  raw -> staging -> staging_core
                                    │
                      ┌─────────────┼─────────────┐
                      v             v             v
                Data quality   OpenMetadata    Streamlit
                monitoring       catalog        dashboard
                                    ^
                                    │
                       Airflow orchestrates the pipeline
```

## Learning Objectives

This project demonstrates how to:

- Design a relational source schema with primary keys, foreign keys, constraints, and indexes.
- Generate deterministic synthetic data for repeatable local development.
- Replicate operational data incrementally with Meltano and Singer.
- Ingest REST API data safely with validation, bounded pagination, retries, and idempotent upserts.
- Validate business-managed Excel inputs before loading them into a warehouse.
- Model RAW data with dbt sources, staging models, dimensions, facts, and data tests.
- Orchestrate a daily data workflow with Airflow.
- Monitor freshness, volume changes, and empty relations.
- Document lineage and database assets with OpenMetadata.
- Build a read-only Streamlit analytics dashboard.
- Apply basic local security controls such as loopback-only ports, non-root dashboard execution, and environment-based secrets.

## Technology Stack

| Area | Technology | Purpose |
| --- | --- | --- |
| Containers | Docker Compose | Local services, networking, profiles, and persistent volumes |
| Operational store | PostgreSQL 17 | Synthetic gaming source database |
| Analytics warehouse | PostgreSQL 17 | RAW, staging, core, and monitoring schemas |
| Data generation | Python, Faker | Reproducible synthetic gaming data |
| Replication | Meltano, tap-postgres, target-postgres | Incremental source-to-warehouse replication |
| API ingestion | Python standard library, psycopg | Validated and retry-aware RAWG ingestion |
| Spreadsheet ingestion | openpyxl, psycopg | Excel contract validation and snapshot loading |
| Transformation | dbt Core, dbt-postgres | Typed staging and core analytics models |
| Orchestration | Apache Airflow | Scheduled pipeline execution and notification |
| Data quality | Python, PostgreSQL | Freshness and volume monitoring with persisted results |
| Metadata | OpenMetadata, MySQL, OpenSearch | Local catalog and dbt lineage ingestion |
| Visualization | Streamlit, pandas | Read-only analytics dashboard |
| CI | GitHub Actions workflow | Python, dbt, and Compose validation definition |

## Project Structure

```text
.
├── airflow/
│   └── dags/
│       └── gaming_analytics_pipeline.py  # Daily orchestration DAG
├── dashboard/
│   ├── app.py                            # Streamlit application
│   └── requirements.txt
├── data/
│   └── business/                         # Marketing, budget, and target workbooks
├── dbt/
│   ├── models/
│   │   ├── staging/                      # Typed RAW views
│   │   └── core/                         # Dimensions, facts, and bridge table
│   ├── tests/                            # Custom dbt SQL tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── docker/
│   ├── airflow/
│   ├── dashboard/
│   └── meltano/
├── ingestion/
│   ├── api/rawg.py                       # RAWG API client and loader
│   ├── excel/                            # Workbook generation and loading
│   └── generate/                         # Synthetic source-data generator
├── metadata/
│   └── ingest_postgres.py                # OpenMetadata PostgreSQL workflow
├── meltano/
│   └── meltano.yml                       # Singer extractor/loader configuration
├── monitoring/
│   └── data_quality.py                   # Freshness and volume checks
├── sql/                                  # Source and RAW-layer DDL
├── tests/                                # Python unit tests
├── .env.example                          # Environment-variable template
├── docker-compose.yml                    # Service topology and profiles
└── requirements.txt                      # Local Python dependencies
```

## Architecture

### Data zones

| Zone | Schema | Contents | Owner / writer |
| --- | --- | --- | --- |
| Operational source | `public` in `postgres-source` | Synthetic transactional entities and events | Data generator |
| Landing zone | `raw` in `postgres-analytics` | Meltano streams, RAWG API tables, Excel snapshots | Meltano and Python loaders |
| Standardized layer | `staging` | Typed dbt views; Meltano soft-deleted rows excluded | dbt |
| Analytics layer | `staging_core` | Dimensions, facts, and game-platform bridge | dbt |
| Observability | `monitoring` | Data-quality runs and relation snapshots | Quality monitor |

### Core model grains

- `dim_players`, `dim_games`, `dim_platforms`, and `dim_date` store descriptive dimension attributes.
- `bridge_game_platforms` stores one row per supported game/platform pair.
- `fact_sessions` stores one row per play session.
- `fact_purchases` stores one row per purchase in its original currency.
- `fact_reviews` stores one row per player review.
- `fact_achievements` stores one row per earned achievement.
- `fact_campaigns` stores one row per marketing campaign with budget calculations.

RAWG data is deliberately kept separate from synthetic game IDs because the systems do not share a key.

### Docker Compose profiles

| Profile | Service(s) | Use when |
| --- | --- | --- |
| Default | `postgres-source`, `postgres-analytics` | Running the source and analytics databases |
| `ingestion` | `meltano` | Installing or running the Meltano pipeline manually |
| `orchestration` | `airflow` | Scheduling or manually triggering the complete pipeline |
| `dashboard` | `dashboard` | Viewing analytics through Streamlit |
| `openmetadata` | OpenMetadata, MySQL, OpenSearch, ingestion | Browsing metadata and lineage |

## Prerequisites

- Docker Desktop with Docker Compose v2.
- Python 3.10 or newer; Python 3.11 is used by the project containers and CI workflow.
- PowerShell on Windows.
- A RAWG API key if the RAWG workflow or Airflow pipeline will be run.
- SMTP settings only if email notifications are required from Airflow.

## Configuration

Create a private local configuration file from the template:

```powershell
Copy-Item .env.example .env
```

Set a strong local `POSTGRES_PASSWORD` and make the password in `DATABASE_URL` match it. Do not commit `.env` or expose its API keys, JWTs, or SMTP credentials.

| Variables | Required for | Notes |
| --- | --- | --- |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `DATABASE_URL` | Source generation and database services | `DATABASE_URL` targets the source database from the host |
| `ANALYTICS_DB`, `ANALYTICS_PORT` | Analytics loaders and dbt | The default host port is `5433` |
| `RAWG_API_KEY`, `RAWG_MAX_PAGES` | RAWG ingestion | Page count limits API usage |
| `AIRFLOW__SMTP__*`, `QUALITY_ALERT_EMAIL_TO` | Airflow email report | Optional until notification is enabled |
| `OPENMETADATA_MYSQL_ROOT_PASSWORD`, `OPENMETADATA_JWT_TOKEN` | OpenMetadata | Needed only for the metadata profile |
| `DATA_QUALITY_*` | Quality monitor | Optional threshold overrides |

Validate the resolved configuration before starting services:

```powershell
docker compose config --quiet
```

## Quick Start

The following sequence creates a usable local environment and builds the analytics models.

```powershell
# 1. Start the source and analytics databases.
docker compose up -d --wait postgres-source postgres-analytics

# 2. Create the local Python environment and install dependencies.
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 3. Seed the source operational database.
.\.venv\Scripts\python.exe -m ingestion.generate.generate_gaming_data

# 4. Install and run incremental source replication.
docker compose build meltano
docker compose run --rm meltano install
docker compose run --rm meltano run tap-postgres target-postgres

# 5. Load the business workbooks. This requires raw.games from the Meltano sync.
.\.venv\Scripts\python.exe -m ingestion.excel.load_excel

# 6. Load dbt connection variables, then build and test the analytics layer.
$localEnv = Get-Content .env -Raw | ConvertFrom-StringData
$env:POSTGRES_USER = $localEnv.POSTGRES_USER
$env:POSTGRES_PASSWORD = $localEnv.POSTGRES_PASSWORD
$env:ANALYTICS_DB = $localEnv.ANALYTICS_DB
$env:ANALYTICS_PORT = $localEnv.ANALYTICS_PORT
$env:DBT_HOST = "localhost"
.\.venv\Scripts\dbt.exe run --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe test --project-dir dbt --profiles-dir dbt
```

The RAWG workflow is intentionally not in the quick-start sequence because it needs a valid API key. Run it after the analytics database is available if external catalog data is needed.

## Workflows

### 1. Synthetic source-data workflow

**Purpose:** Create deterministic, referentially valid operational gaming data in `postgres-source.public`.

The generator creates players, platforms, games, game-platform pairs, sessions, purchases, achievements, and reviews. It uses a fixed seed by default and emits UTC timestamps. Database constraints enforce primary-key, foreign-key, uniqueness, and domain rules.

```powershell
# Default data volume
.\.venv\Scripts\python.exe -m ingestion.generate.generate_gaming_data

# Small data volume for fast local testing
.\.venv\Scripts\python.exe -m ingestion.generate.generate_gaming_data --players 100 --games 20 --sessions 500 --purchases 100 --reviews 100

# Destructive: replace all generated source rows
.\.venv\Scripts\python.exe -m ingestion.generate.generate_gaming_data --reset
```

Use `--reset` only when replacing the source dataset is intentional. Without it, the generator refuses to insert into populated source tables.

### 2. Incremental source-to-RAW replication workflow

**Purpose:** Replicate the eight operational source tables into `postgres-analytics.raw`.

Meltano uses `tap-postgres` to read the source `public` schema. Each stream uses `updated_at` as its incremental bookmark, and `target-postgres` applies primary-key upserts. The RAW tables also retain Singer `_sdc_*` ingestion metadata.

```powershell
# First use only: download the Meltano plugins.
docker compose run --rm meltano install

# Run the incremental replication pipeline.
docker compose run --rm meltano run tap-postgres target-postgres
```

This is incremental replication, not change-data capture: hard deletes and updates that do not advance `updated_at` are not detected.

### 3. RAWG API ingestion workflow

**Purpose:** Ingest a bounded external catalog of games, platforms, genres, publishers, and developers into `raw.rawg_*` tables.

The loader validates API responses and records, checks pagination URLs, retries transient failures and rate limits, stores the original payload as JSONB, and upserts records by RAWG ID.

```powershell
# See all options.
.\.venv\Scripts\python.exe -m ingestion.api.rawg --help

# Fetch a small games sample.
.\.venv\Scripts\python.exe -m ingestion.api.rawg --resources games --page-size 40 --max-pages 2

# Fetch several catalog resources.
.\.venv\Scripts\python.exe -m ingestion.api.rawg --resources platforms genres publishers developers --max-pages 3
```

Set `RAWG_API_KEY` in `.env` before running this workflow. Preserve required RAWG attribution whenever API-derived data is displayed.

### 4. Excel business-data workflow

**Purpose:** Load controlled business inputs into `raw` after validating their structure and relationships.

Required workbooks are stored in `data/business`:

- `marketing_campaigns.xlsx`
- `game_targets.xlsx`
- `budget.xlsx`

The loader requires exact sheet names and headers, normalizes data types, rejects invalid dates and values, checks campaign/budget agreement, confirms referenced games exist in `raw.games`, deduplicates identical rows, and records file hashes in `raw.business_file_loads`.

```powershell
# Optional: create deterministic sample workbooks.
.\.venv\Scripts\python.exe -m ingestion.excel.generate_business_data

# Validate and load changed workbooks.
.\.venv\Scripts\python.exe -m ingestion.excel.load_excel

# Reload even when workbook hashes have not changed.
.\.venv\Scripts\python.exe -m ingestion.excel.load_excel --force
```

Run the Meltano replication workflow first, because workbook game IDs are validated against `raw.games`.

### 5. dbt transformation and testing workflow

**Purpose:** Convert raw ingestion tables into consistent staging views and analytics-ready core tables.

`staging` models are views that cast fields to explicit PostgreSQL types and exclude Meltano soft-deleted rows. `staging_core` models are tables that implement dimensions, facts, and the game-platform bridge. dbt never reloads the RAW layer.

Load the database settings into the PowerShell process before running dbt:

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

Useful dbt inspection commands:

```powershell
.\.venv\Scripts\dbt.exe ls --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe compile --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe docs generate --project-dir dbt --profiles-dir dbt
```

### 6. Data-quality monitoring workflow

**Purpose:** Detect empty tables, stale ingestion, and significant row-count changes across RAW, staging, and core relations.

The monitor writes every result to the analytics database's `monitoring` schema. The first successful check establishes the baseline for volume comparisons. Freshness comes from Meltano `_sdc_extracted_at`, RAWG `fetched_at`, and Excel `loaded_at` timestamps.

```powershell
.\.venv\Scripts\python.exe -m monitoring.data_quality
```

Configure thresholds with `DATA_QUALITY_VOLUME_CHANGE_WARN_PERCENT`, `DATA_QUALITY_FRESHNESS_WARN_HOURS`, `DATA_QUALITY_FRESHNESS_ERROR_HOURS`, and the corresponding Excel freshness variables.

### 7. Airflow orchestration workflow

**Purpose:** Run the full ingestion, transformation, test, monitoring, and notification workflow daily.

The DAG runs at `02:00` in the `Asia/Ulaanbaatar` timezone, has `catchup=False`, allows one active run, and retries failed tasks once. It is intended for a single-user local learning environment.

```powershell
# Build and start Airflow.
docker compose --profile orchestration up -d --build --wait airflow

# Inspect the DAG and import status.
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow tasks list gaming_analytics_pipeline

# Trigger a complete run now.
docker compose exec airflow airflow dags trigger gaming_analytics_pipeline
docker compose logs --follow airflow
```

The Airflow UI is available at `http://localhost:8080`. On first startup, the local admin password is stored inside the Airflow volume:

```powershell
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

Configure `RAWG_API_KEY` and the SMTP variables before triggering the complete DAG. Use Airflow for scheduled operation instead of manually running the individual ingestion and dbt steps.

### 8. Dashboard workflow

**Purpose:** Present analytics from the dbt core layer without modifying the warehouse.

The Streamlit application reads `staging_core` through a read-only PostgreSQL transaction. It provides date, game, and purchase-currency filters; purchase values from different currencies are intentionally not aggregated.

```powershell
docker compose --profile dashboard up -d --build --wait dashboard
docker compose ps dashboard postgres-analytics
```

Open `http://localhost:8501`. Build dbt models before opening the dashboard; otherwise, no core data is available to display.

### 9. Metadata and lineage workflow (optional)

**Purpose:** Catalog PostgreSQL schemas and dbt lineage in a local OpenMetadata instance.

```powershell
# Start the local metadata stack.
docker compose --profile openmetadata up -d --wait openmetadata-server
Invoke-RestMethod http://localhost:8585/healthcheck

# Ingest PostgreSQL metadata after setting OPENMETADATA_JWT_TOKEN.
docker compose --profile openmetadata run --rm openmetadata-ingestion

# Generate dbt artifacts, then ingest dbt metadata and lineage.
.\.venv\Scripts\dbt.exe docs generate --project-dir dbt --profiles-dir dbt
docker compose --profile openmetadata run --rm --entrypoint metadata openmetadata-ingestion ingest-dbt -c /project/dbt
```

OpenMetadata is available at `http://localhost:8585`. It is an optional, memory-intensive local service; stop it when not in use.

## Validation and Testing

Run the local validation suite from the project root:

```powershell
.\.venv\Scripts\python.exe -m compileall -q ingestion monitoring dashboard tests
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
docker compose config --quiet
```

To validate dbt parsing without a live database, export the placeholder environment variables used by CI and run:

```powershell
$env:DBT_HOST = "localhost"
$env:ANALYTICS_PORT = "5433"
$env:POSTGRES_USER = "gaming"
$env:POSTGRES_PASSWORD = "ci-only-placeholder"
$env:ANALYTICS_DB = "gaming_analytics"
$env:OPENMETADATA_HOST_PORT = "http://localhost:8585/api"
$env:OPENMETADATA_JWT_TOKEN = "ci-only-placeholder"
$env:OPENMETADATA_SERVICE_NAME = "gaming-analytics-ci"
.\.venv\Scriptsdbt.exe parse --no-partial-parse --project-dir dbt --profiles-dir dbt
```

The GitHub Actions workflow defines the same core checks for a repository-hosted CI environment.

## Operations

### Service status and logs

```powershell
docker compose ps
docker compose logs --tail 100 postgres-source postgres-analytics
docker compose --profile orchestration logs --tail 100 airflow
docker compose --profile dashboard logs --tail 100 dashboard
```

### Stop services while preserving data

```powershell
docker compose --profile orchestration stop airflow
docker compose --profile dashboard stop dashboard
docker compose --profile openmetadata stop openmetadata-server openmetadata-mysql openmetadata-opensearch
docker compose stop postgres-source postgres-analytics
```

Named Docker volumes preserve database, Airflow, Meltano, and OpenMetadata state. Do not run `docker compose down --volumes` unless deleting all local project data is intentional.

## Security and Deployment Notes

- Keep secrets only in the local, ignored `.env` file.
- PostgreSQL, Airflow, Streamlit, and OpenMetadata host ports bind to `127.0.0.1` by default.
- The dashboard runs as a non-root user, drops Linux capabilities, prevents privilege escalation, and opens read-only database transactions.
- This is not a production deployment. Airflow uses SQLite metadata and `SequentialExecutor`; no HA setup, remote secret manager, TLS termination, or tested disaster-recovery process is included.
- The OpenMetadata/OpenSearch configuration is for isolated local learning only.
- The source schema initialization scripts run only for a new PostgreSQL data volume. Apply changes to an existing database deliberately through migrations or `psql`.

## Troubleshooting

| Symptom | Recommended check |
| --- | --- |
| Docker service does not start | Run `docker compose ps` and `docker compose logs <service>`; confirm Docker Desktop is running. |
| Python loader cannot connect | Confirm both PostgreSQL services are healthy and that `.env` connection settings match the configured host ports. |
| Meltano reports plugin or state issues | Rebuild the image, run `docker compose run --rm meltano install`, then rerun the pipeline. |
| RAWG ingestion fails | Confirm `RAWG_API_KEY`, reduce `RAWG_MAX_PAGES`, and inspect the loader output for rate-limit or validation errors. |
| Excel loader fails | Verify workbook names, sheet names, exact headers, required fields, and referenced game IDs. |
| dbt connection fails | Reload `.env` values into PowerShell and run `dbt debug`. |
| Dashboard has no data | Run the ingestion workflows and `dbt run` successfully before starting the dashboard. |
| Airflow DAG is missing | Run `docker compose exec airflow airflow dags list-import-errors`, then rebuild the Airflow image after dependency or DAG changes. |

## License and Intended Use

This is an educational capstone project. The source gaming records are synthetic, and the RAWG collection is bounded for demonstration purposes. Follow RAWG's terms and attribution requirements when using API-derived data.
