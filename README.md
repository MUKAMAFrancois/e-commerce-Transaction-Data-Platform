# E-commerce Transaction Data Platform

Dockerized data platform: synthetic data → MinIO → Airflow → PostgreSQL → Metabase.

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
| `--seed` | `42` | same seed → identical output; `0` for random |
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
docker compose exec minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls --recursive local/raw-transactions/quarantine'
```

## Inspect

```bash
# List objects in MinIO
docker compose exec minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls --recursive local/raw-transactions'

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

## Status

- [x] Infrastructure (Postgres, MinIO, Airflow, Metabase)
- [x] Synthetic data generator → MinIO
- [x] Airflow DAG: MinIO → transform → PostgreSQL
- [ ] Metabase dashboard
- [ ] CI/CD
