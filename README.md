# E-commerce Transaction Data Platform

Dockerized data platform: synthetic data -> MinIO -> Airflow -> PostgreSQL -> Metabase.

## Prerequisites

Docker Desktop (Compose v2). Nothing else — Python runs inside the containers.

## Setup

Run once. `-n` matters: it refuses to overwrite an `.env` you already have.

```bash
cp -n .env.example .env
```

Then generate the two Airflow keys and paste them into `.env`:

```bash
openssl rand -base64 32 | tr '+/' '-_'   # AIRFLOW_FERNET_KEY
openssl rand -hex 32                     # AIRFLOW_SECRET_KEY
```

> Passwords in `.env` are only applied when a volume is first created. Changing
> `POSTGRES_PASSWORD` later breaks authentication against the existing volume —
> either set it back, or `docker compose down -v` to rebuild from scratch.

## Start

```bash
docker compose up -d --build
```

First run builds the Airflow image, migrates the metadata DB, creates the admin
user and the MinIO bucket. Wait for everything to be healthy:

```bash
docker compose ps
```

| Service | URL | Credentials |
| :--- | :--- | :--- |
| Airflow | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| MinIO console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Metabase | http://localhost:3000 | set on first visit |
| PostgreSQL | localhost:5432 | `POSTGRES_USER` / `POSTGRES_PASSWORD` |

Ports are configurable in `.env` if any are already taken.

## Generate data

Runs inside the Airflow container:

```bash
docker compose exec airflow-scheduler python -m data_generator.synthetic --dest both
```

| Flag | Default | Notes |
| :--- | :--- | :--- |
| `--dest` | `local` | `local`, `minio`, or `both` |
| `--seed` | `42` | same seed -> identical output; `0` for random |
| `--customers` | 1000 | |
| `--products` | 500 | |
| `--orders` | 5000 | |

`--dest local` writes to `./data/raw/` on the host. `--dest minio` uploads to
`<dataset>/ingest_date=YYYY-MM-DD/<dataset>_HHMMSS.csv` in the raw bucket.

To run it standalone instead of in Docker:

```bash
pip install -r requirements.txt
python -m data_generator.synthetic --dest local
```

## Run the pipeline

The `ecommerce_ingestion` DAG reads the raw CSVs from MinIO, cleans them,
upserts into PostgreSQL, then moves consumed objects under `processed/` so the
next run skips them. Loads follow the foreign keys: customers and products,
then orders, then order_items and payments.

```bash
# Trigger from the CLI (or use the Airflow UI)
docker compose exec airflow-scheduler airflow dags trigger ecommerce_ingestion

# Run synchronously and watch the output
docker compose exec airflow-scheduler airflow dags test ecommerce_ingestion 2026-01-01
```

Upserts are keyed on the primary key, so re-running loads the same data without
duplicating it.

### Data quality

Every row is checked before load: required fields present, primary keys unique,
numbers non-negative, statuses within their allowed set, foreign keys resolving
against rows already in the database, and cross-field consistency
(`total_amount = items_subtotal + shipping_fee`, `line_total = quantity × unit_price`,
unsettled payments carrying no amount).

Failing rows are **not dropped** — they are written to
`quarantine/<dataset>/<timestamp>.csv` in the raw bucket with a
`rejection_reason` column. If more than 5% of a dataset is rejected the task
fails without loading, on the assumption that the feed itself is broken.

After loading, a `verify` task re-checks referential integrity and business
consistency in PostgreSQL and fails the run if anything is violated.

```bash
# Inspect quarantined rows
docker compose exec minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls --recursive local/ecommerce-raw/quarantine'
```

## Dashboard

`docker compose up -d` provisions Metabase automatically — admin user, the
PostgreSQL connection, eight KPI cards and the **E-commerce KPIs** dashboard —
via [config/metabase/provision.py](config/metabase/provision.py). It is
idempotent, so it reruns safely.

Open http://localhost:3000, sign in with `METABASE_ADMIN_EMAIL` /
`METABASE_ADMIN_PASSWORD`, and the dashboard is on the home page.

| Card | Metric |
| :--- | :--- |
| Total Orders | count of orders |
| Total Revenue | `sum(amount_paid)` where payment completed |
| Average Order Value | revenue ÷ completed payments |
| Orders Over Time | orders per month |
| Revenue Over Time | revenue per month |
| Revenue by Category | `sum(line_total)` by product category |
| Order Status Distribution | orders per status |
| Payment Status Distribution | payments per status |

To re-provision after changing the cards:

```bash
docker compose up -d --force-recreate metabase-init
```

On a fresh stack the tables do not exist until the DAG has run once; the script
warns and still creates the cards, which fill in on the next run.

## Tests

```bash
# Everything
docker compose exec -w /opt/airflow airflow-scheduler pytest

# Unit tests only — pure functions, no running stack needed
docker compose exec -w /opt/airflow airflow-scheduler pytest -m "not integration"

# Skip the full DAG runs
docker compose exec -w /opt/airflow airflow-scheduler pytest -m "not slow"
```

| Suite | Covers |
| :--- | :--- |
| `tests/unit/test_generator.py` | reproducibility, key uniqueness, referential integrity, monetary and temporal consistency |
| `tests/unit/test_quality.py` | cleaning and every validation rule, including a regression test for mixed timestamp precision |
| `tests/integration/test_database.py` | tables, columns, primary and foreign keys, constraint enforcement, loaded-data integrity |
| `tests/integration/test_pipeline.py` | MinIO → Airflow → PostgreSQL end to end, archival, upsert idempotency |
| `tests/integration/test_metabase.py` | health, auth, registered database, dashboard, every card returning rows |

Integration tests create `TEST-*` rows and objects and remove them afterwards.

## CI

[.github/workflows/main.yml](.github/workflows/main.yml) runs on every push and
pull request to `main`:

| Job | Does |
| :--- | :--- |
| `lint` | `ruff check`, hadolint on the Dockerfile, `docker compose config` |
| `unit-tests` | the 40 tests that need no containers |
| `integration` | builds the image, starts the stack, generates data, runs the DAG, re-provisions Metabase, runs the 43 integration tests |

`integration` only runs if `lint` and `unit-tests` pass, and always tears the
stack down afterwards. To reproduce the lint job locally:

```bash
docker compose exec -w /opt/airflow airflow-scheduler ruff check .
```

### CD

[.github/workflows/cd.yml](.github/workflows/cd.yml) is triggered by the CI
workflow completing — not by a push — and its first job is gated on
`github.event.workflow_run.conclusion == 'success'`. Nothing deploys unless all
three CI jobs passed on that commit.

| Job | Does |
| :--- | :--- |
| `publish` | builds the Airflow image and pushes it to GHCR, tagged with the validated commit SHA |
| `deploy` | pulls that published image, brings the stack up from it, smoke-tests the data flow, then runs the 43 integration tests against the deployment |

`deploy` targets the `test` GitHub Environment. It deploys the **published
artifact** rather than rebuilding, so what is validated is exactly what was
published. `AIRFLOW_IMAGE` in the compose file is what makes that swap possible.

### Branch protection

[config/ruleset.json](config/ruleset.json) is a GitHub repository ruleset that
makes all three CI jobs required before anything reaches `main`, and blocks
force-pushes and branch deletion. Apply it under
**Settings → Rules → Rulesets → New ruleset → Import a ruleset**, or:

```bash
gh api repos/:owner/:repo/rulesets --input config/ruleset.json
```

It requires a pull request but zero approvals, so a solo maintainer is not
locked out. Note that it does stop direct pushes to `main` — work on a branch
and open a PR once it is active.

## Inspect

```bash
# List objects in MinIO
docker compose exec minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls --recursive local/ecommerce-raw'

# Query the analytics database
docker compose exec postgres psql -U ecommerce_user -d ecommerce_db

# Logs
docker compose logs -f airflow-scheduler
```

## Rebuild and reset

```bash
# Rebuild after changing requirements.txt or Dockerfile
docker compose build && docker compose up -d

# Stop, keeping data
docker compose down

# Wipe everything including volumes (re-runs the DB init script)
docker compose down -v
```

## Layout

```text
dags/                 Airflow DAGs
dags/sql/schema.sql   analytics table definitions
data_generator/       synthetic data generator
config/postgres/init/ runs once on first Postgres boot
data/raw/             local CSV output (gitignored)
Dockerfile            Airflow image + generator dependencies
docker-compose.yml
```

## Diagrams

PlantUML sources live in [docs/](docs); the PNGs beside them are rendered output.

```bash
docker run --rm -v "$PWD/docs":/work -w /work plantuml/plantuml -tpng *.puml
```

### Workflow

![Workflow summary](docs/workflow.png)

### Architecture

Solid arrows are data flow, dotted arrows orchestration.

![Architecture](docs/architecture.png)

### CI/CD

![CI/CD pipeline](docs/cicd.png)

### Data model

![Data model](docs/data-model.png)

## Status

- [x] Infrastructure (Postgres, MinIO, Airflow, Metabase)
- [x] Synthetic data generator -> MinIO
- [x] Airflow DAG: MinIO -> transform -> PostgreSQL
- [x] Metabase dashboard
- [x] Automated tests
- [x] GitHub Actions CI
- [x] Continuous deployment
- [x] Architecture and workflow diagrams
- [ ] reflection.md
