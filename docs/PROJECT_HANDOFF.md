# Gaming Analytics Data Platform — Project Handoff

## Project summary

This is a local learning/capstone analytics platform. It creates synthetic
gaming operations data, ingests operational and external data, transforms it
into analytics models, runs quality checks, catalogs metadata, and serves a
local dashboard.

The project is **not a production deployment**. Airflow 3.3.2 uses SQLite
metadata and `LocalExecutor`; OpenMetadata's local profile disables OpenSearch
security on the private Compose network; and the PostgreSQL host ports are
published by Compose. Do not expose this setup to an untrusted network or use
it for sensitive production data.

## Architecture

```text
Synthetic Python generator ──> postgres-source.public
                                       │
                   Meltano tap-postgres (incremental upsert)
                                       ▼
External RAWG API ────────────> postgres-analytics.raw
Excel business workbooks ────>         │
                                       ▼
                              dbt staging views
                                       │
                                       ▼
                              dbt staging_core facts/dimensions
                                 │              │
                       Airflow daily DAG   Streamlit dashboard
                       02:00 Mongolia      localhost:8501
                         │
              quality snapshot + SMTP report

Optional: OpenMetadata catalogs PostgreSQL assets and dbt lineage.
```

### Main components

| Component | Purpose | Compose profile / access |
|---|---|---|
| `postgres-source` | Synthetic operational source database | Default profile; loopback port 5432 |
| `postgres-analytics` | RAW, staging, core, and monitoring schemas | Default profile; loopback port 5433 |
| `meltano` | Incremental PostgreSQL-to-PostgreSQL Singer sync | `ingestion` |
| `airflow` | Airflow 3.3.2; daily ingestion, dbt, quality checks, and notification | `orchestration`; `http://localhost:8080` |
| `dashboard` | Read-only analytics UI over dbt core tables | `dashboard`; `http://localhost:8501` |
| OpenMetadata services | Catalog and dbt lineage | `openmetadata`; `http://localhost:8585` |

Compose named volumes retain PostgreSQL data, Airflow metadata, Meltano state,
and OpenMetadata catalog data. `docker compose down --volumes` deletes that
persistent state; use it only when intentional.

## Data flow and project layout

1. `ingestion/generate/` produces deterministic synthetic operational records
   for `postgres-source`.
2. `meltano/` incrementally syncs eight operational streams into the analytics
   database's `raw` schema. The RAWG ingestion code in `ingestion/api/` writes
   a bounded API sample to RAW tables.
3. `ingestion/excel/` validates and loads business workbook snapshots from
   `data/business/`.
4. `dbt/models/staging/` cleans the sources; `dbt/models/core/` creates
   analytics facts and dimensions in `staging_core`.
5. `monitoring/` evaluates freshness, row counts, and volume changes over 43
   RAW, staging, and core relations. Run summaries are stored under the
   `monitoring` schema.
6. `airflow/dags/gaming_analytics_pipeline.py` orchestrates the above steps
   daily at 02:00 `Asia/Ulaanbaatar`, with manual triggering retained.
7. `dashboard/app.py` reads core models for engagement, purchases, and player
   feedback. Purchase totals remain separated by currency.
8. `metadata/` contains OpenMetadata ingestion configuration. `.github/`
   contains the GitHub Actions CI workflow.

## Initial setup

Prerequisites: Docker Desktop with Compose v2, Python 3.11 for the supplied
Airflow/dashboard images and CI, and PowerShell on Windows.

From the project root:

```powershell
Copy-Item .env.example .env
```

Edit `.env` locally. Set a non-example `POSTGRES_PASSWORD`, keep the password
consistent with `DATABASE_URL`, and configure `RAWG_API_KEY` and SMTP values
if running the automated ingestion/reporting pipeline. Never commit or share
`.env`, API keys, passwords, SMTP credentials, or the OpenMetadata JWT.

Start the base databases:

```powershell
docker compose config --quiet
docker compose up -d postgres-source postgres-analytics
docker compose ps
```

Wait until both databases are healthy before starting ingestion or optional
services. Existing volumes may have been initialized with earlier credentials;
changing `.env` does not rotate passwords stored in PostgreSQL.

## Common operations

### Run the scheduled analytics pipeline

The Airflow profile performs real ingestion, RAWG API requests, workbook
loading, dbt builds/tests, quality monitoring, and email delivery. Verify the
API and SMTP settings before starting it:

```powershell
docker compose --profile orchestration up -d --build --wait airflow
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags next-execution gaming_analytics_pipeline
```

The DAG is scheduled every day at 02:00 `Asia/Ulaanbaatar`, has `catchup=False`,
and allows one active run. It can also be started manually:

```powershell
docker compose exec airflow airflow dags trigger gaming_analytics_pipeline
docker compose exec airflow airflow dags list-runs --dag-id gaming_analytics_pipeline
docker compose logs --tail 100 airflow
```

### Open the dashboard

```powershell
docker compose --profile dashboard up -d --build --wait dashboard
```

Open `http://localhost:8501`. The dashboard depends on successful dbt core
models and binds to loopback only. Refresh it after the pipeline updates the
analytics models.

### OpenMetadata

OpenMetadata is optional and resource intensive. The available Docker memory
has not supported running it alongside Airflow reliably. Stop Airflow and the
dashboard first if memory is constrained; the catalog's named volumes persist:

```powershell
docker compose --profile orchestration stop airflow
docker compose --profile dashboard stop dashboard
docker compose --profile openmetadata up -d --wait openmetadata-server
```

Keep its JWT and local database credentials private. OpenSearch security is
disabled on the Compose network for this learning environment.

### Stop and preserve data

Stop only the services you started, preserving volumes:

```powershell
docker compose --profile orchestration stop airflow
docker compose --profile dashboard stop dashboard
docker compose --profile openmetadata stop openmetadata-server openmetadata-mysql openmetadata-opensearch
docker compose stop postgres-source postgres-analytics
```

Do not use `docker compose down --volumes` for normal shutdown.

## Validation and support checks

Run the local CI-equivalent checks from the project root:

```powershell
.\.venv\Scripts\python.exe -m compileall -q ingestion monitoring dashboard tests
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
docker compose config --quiet
```

For dbt parsing, use the placeholder/profile environment shown in the Phase 10
section of the root README; parsing does not require the database to be
available. For runtime issues, check service state and recent logs:

```powershell
docker compose ps
docker compose logs --tail 100 postgres-source postgres-analytics
docker compose --profile orchestration logs --tail 100 airflow
docker compose --profile dashboard logs --tail 100 dashboard
docker compose exec airflow airflow jobs check --job-type SchedulerJob --local
docker compose exec airflow airflow dags list-import-errors
```

The Airflow 3 API health endpoint is
`http://localhost:8080/api/v2/monitor/health`; the
Streamlit health endpoint is `http://localhost:8501/_stcore/health`.
Airflow also requires a persistent, unique
`AIRFLOW__API_AUTH__JWT_SECRET` value in the ignored `.env` file.
OpenMetadata may be unhealthy if the Docker memory allocation is insufficient.

## Security, reliability, and known limitations

- Keep all credentials in the ignored `.env`; `.env.example` contains templates,
  not credentials.
- The dashboard receives only the analytics database connection settings, runs
  without Linux capabilities or privilege escalation, and connects with
  PostgreSQL transactions configured read-only.
- Dashboard, Airflow, and PostgreSQL host ports bind to `127.0.0.1`; other
  devices cannot connect through the published host ports by default.
- These local controls do not make the stack production-ready. Airflow uses SQLite
  metadata and `LocalExecutor`; no HA, remote secret manager, TLS
  termination, or tested disaster-recovery backup is configured.
- The CI workflow is defined and its equivalent checks passed locally. A
  hosted GitHub Actions run has not been recorded; this workspace is not
  connected to a Git repository.
- RAWG collection size is capped by `RAWG_MAX_PAGES`; preserve required RAWG
  attribution when displaying its data.
- Purchase amounts in different currencies are intentionally not consolidated.
- Daily pipeline runs may send email and consume API quota. Monitor configured
  recipient, provider limits, and `RAWG_MAX_PAGES`.

## Phase status

Phases 1–15 are documented as complete in the root README. Phase 15 applies
local dashboard hardening only; it does not certify or make this stack
production-ready. See the phase-by-phase status and remaining limitations in
the root README.
