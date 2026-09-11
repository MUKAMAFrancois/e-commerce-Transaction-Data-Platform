"""MinIO -> validate -> clean -> PostgreSQL.

detect -> load (read, clean, validate, quarantine, upsert) -> verify -> archive

Rows failing a quality rule are written to `quarantine/` in the raw bucket with
a rejection_reason rather than being dropped. A task fails outright if more than
MAX_REJECT_RATE of a dataset is rejected, so a broken upstream feed stops the
pipeline instead of quietly loading a fraction of the data.

Consumed objects move under `processed/` so the next run does not reread them.
"""

import io
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from common.quality import LOAD_ORDER, SPECS, clean, validate

BUCKET = os.getenv("MINIO_RAW_BUCKET", "ecommerce-raw")
PROCESSED_PREFIX = "processed/"
QUARANTINE_PREFIX = "quarantine/"
SCHEMA_SQL = Path(__file__).parent / "sql" / "schema.sql"

MAX_REJECT_RATE = 0.05

POST_LOAD_CHECKS = [
    (
        "orders reference a missing customer",
        "SELECT count(*) FROM orders o LEFT JOIN customers c USING (customer_id)"
        " WHERE c.customer_id IS NULL",
    ),
    (
        "order_items reference a missing order",
        "SELECT count(*) FROM order_items i LEFT JOIN orders o USING (order_id)"
        " WHERE o.order_id IS NULL",
    ),
    (
        "order_items reference a missing product",
        "SELECT count(*) FROM order_items i LEFT JOIN products p USING (product_id)"
        " WHERE p.product_id IS NULL",
    ),
    (
        "payments reference a missing order",
        "SELECT count(*) FROM payments p LEFT JOIN orders o USING (order_id)"
        " WHERE o.order_id IS NULL",
    ),
    (
        "negative monetary values",
        "SELECT (SELECT count(*) FROM orders WHERE total_amount < 0)"
        " + (SELECT count(*) FROM payments WHERE amount_paid < 0)"
        " + (SELECT count(*) FROM products WHERE price < 0 OR cost_price < 0)",
    ),
    (
        "order total does not match its line items",
        "SELECT count(*) FROM (SELECT o.order_id FROM orders o"
        " JOIN order_items i USING (order_id)"
        " GROUP BY o.order_id, o.total_amount, o.shipping_fee"
        " HAVING abs(sum(i.line_total) + o.shipping_fee - o.total_amount) > 0.011) bad",
    ),
    (
        "orders placed before the customer existed",
        "SELECT count(*) FROM orders o JOIN customers c USING (customer_id)"
        " WHERE o.order_date < c.created_at",
    ),
]


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id="analytics_db")


def _upsert(frame: pd.DataFrame, table: str, pk: str) -> int:
    if frame.empty:
        return 0

    columns = list(frame.columns)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != pk)
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({pk}) DO UPDATE SET {updates}"
    )

    # NaN/NaT reach psycopg2 as floats; they have to become real NULLs.
    safe = frame.astype(object).where(pd.notna(frame), None)
    rows = list(safe.itertuples(index=False, name=None))

    connection = _hook().get_conn()
    try:
        with connection.cursor() as cursor:
            execute_values(cursor, sql, rows, page_size=1000)
        connection.commit()
    finally:
        connection.close()

    return len(rows)


def _parent_keys(dataset: str) -> dict[str, set]:
    """Primary keys already in the database, for referential checks before load."""
    keys = {}
    for parent, parent_pk in SPECS[dataset]["foreign_keys"].values():
        rows = _hook().get_records(f"SELECT {parent_pk} FROM {parent}")
        keys[parent] = {row[0] for row in rows}
    return keys


def _quarantine(hook: S3Hook, frame: pd.DataFrame, dataset: str) -> None:
    key = f"{QUARANTINE_PREFIX}{dataset}/{datetime.now():%Y-%m-%dT%H%M%S}.csv"
    hook.load_string(
        frame.to_csv(index=False),
        key=key,
        bucket_name=BUCKET,
        replace=True,
    )
    print(f"quarantined {len(frame)} row(s) -> s3://{BUCKET}/{key}")
    print(frame["rejection_reason"].value_counts().to_string())


@dag(
    dag_id="ecommerce_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["ecommerce"],
)
def ecommerce_ingestion():
    @task
    def create_schema() -> None:
        _hook().run(SCHEMA_SQL.read_text(encoding="utf-8"))

    @task
    def detect() -> dict[str, list[str]]:
        hook = S3Hook(aws_conn_id="minio_s3")
        found = {}
        for dataset in LOAD_ORDER:
            keys = hook.list_keys(bucket_name=BUCKET, prefix=f"{dataset}/") or []
            found[dataset] = [key for key in keys if key.endswith(".csv")]
            print(f"{dataset}: {len(found[dataset])} new file(s)")
        return found

    @task
    def load(dataset: str, detected: dict[str, list[str]]) -> int:
        keys = detected[dataset]
        if not keys:
            print(f"{dataset}: nothing to load")
            return 0

        hook = S3Hook(aws_conn_id="minio_s3")
        frames = [
            pd.read_csv(io.BytesIO(hook.get_key(key, BUCKET).get()["Body"].read()))
            for key in keys
        ]
        frame = clean(pd.concat(frames, ignore_index=True), dataset)

        accepted, rejected = validate(frame, dataset, _parent_keys(dataset))

        if not rejected.empty:
            _quarantine(hook, rejected, dataset)
            rate = len(rejected) / len(frame)
            if rate > MAX_REJECT_RATE:
                # Not retryable: the same bad file would fail identically, and
                # each attempt writes another quarantine copy.
                raise AirflowFailException(
                    f"{dataset}: {rate:.1%} of rows rejected, above the "
                    f"{MAX_REJECT_RATE:.0%} threshold - refusing to load"
                )

        rows = _upsert(accepted, dataset, SPECS[dataset]["pk"])
        print(f"{dataset}: loaded {rows}, rejected {len(rejected)}, from {len(keys)} file(s)")
        return rows

    @task
    def verify() -> dict[str, int]:
        hook = _hook()
        failures = []
        for description, sql in POST_LOAD_CHECKS:
            count = hook.get_first(sql)[0]
            print(f"{'FAIL' if count else 'ok  '}  {description}: {count}")
            if count:
                failures.append(f"{description} ({count})")

        if failures:
            raise AirflowFailException(
                "post-load checks failed: " + "; ".join(failures)
            )

        counts = {
            table: hook.get_first(f"SELECT count(*) FROM {table}")[0]
            for table in LOAD_ORDER
        }
        print("row counts:", counts)
        return counts

    @task
    def archive(detected: dict[str, list[str]]) -> int:
        hook = S3Hook(aws_conn_id="minio_s3")
        moved = 0
        for key in (key for keys in detected.values() for key in keys):
            hook.copy_object(
                source_bucket_name=BUCKET,
                dest_bucket_name=BUCKET,
                source_bucket_key=key,
                dest_bucket_key=f"{PROCESSED_PREFIX}{key}",
            )
            hook.delete_objects(bucket=BUCKET, keys=[key])
            moved += 1
        print(f"archived {moved} object(s)")
        return moved

    detected = detect()

    loads = {
        dataset: load.override(task_id=f"load_{dataset}")(dataset, detected)
        for dataset in LOAD_ORDER
    }

    create_schema() >> detected
    [loads["customers"], loads["products"]] >> loads["orders"]
    loads["orders"] >> [loads["order_items"], loads["payments"]]
    [loads["order_items"], loads["payments"]] >> verify() >> archive(detected)


ecommerce_ingestion()
