"""MinIO -> clean -> PostgreSQL.

Reads raw CSVs from the MinIO bucket, cleans them, upserts into the analytics
database, then moves the consumed objects under `processed/` so the next run
does not pick them up again.

Loads follow the foreign keys: dimensions first, then orders, then the tables
that reference orders.
"""

import io
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

BUCKET = os.getenv("MINIO_RAW_BUCKET", "raw-transactions")
PROCESSED_PREFIX = "processed/"
SCHEMA_SQL = Path(__file__).parent / "sql" / "schema.sql"

SPECS = {
    "customers": {
        "pk": "customer_id",
        "dates": ["created_at"],
        "lower": ["email"],
        "numeric": [],
    },
    "products": {
        "pk": "product_id",
        "dates": [],
        "lower": [],
        "numeric": ["price", "cost_price", "stock_quantity"],
    },
    "orders": {
        "pk": "order_id",
        "dates": ["order_date"],
        "lower": ["order_status"],
        "numeric": ["items_subtotal", "shipping_fee", "total_amount"],
    },
    "order_items": {
        "pk": "order_item_id",
        "dates": [],
        "lower": [],
        "numeric": ["quantity", "unit_price", "line_total"],
    },
    "payments": {
        "pk": "payment_id",
        "dates": ["paid_at"],
        "lower": ["payment_method", "payment_status"],
        "numeric": ["amount_paid"],
    },
}


def _clean(frame: pd.DataFrame, spec: dict) -> pd.DataFrame:
    pk = spec["pk"]

    for column in frame.select_dtypes(include="object"):
        frame[column] = frame[column].str.strip()

    for column in spec["lower"]:
        frame[column] = frame[column].str.lower()

    for column in spec["dates"]:
        frame[column] = pd.to_datetime(frame[column], errors="coerce")

    for column in spec["numeric"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.dropna(subset=[pk])
    # Re-running the generator produces overlapping snapshots; last row wins.
    return frame.drop_duplicates(subset=[pk], keep="last")


def _upsert(frame: pd.DataFrame, table: str, pk: str) -> int:
    columns = list(frame.columns)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != pk)
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({pk}) DO UPDATE SET {updates}"
    )

    # NaN/NaT are floats to psycopg2; they have to become real NULLs.
    safe = frame.astype(object).where(pd.notna(frame), None)
    rows = list(safe.itertuples(index=False, name=None))

    connection = PostgresHook(postgres_conn_id="analytics_db").get_conn()
    try:
        with connection.cursor() as cursor:
            execute_values(cursor, sql, rows, page_size=1000)
        connection.commit()
    finally:
        connection.close()

    return len(rows)


@dag(
    dag_id="ecommerce_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ecommerce"],
)
def ecommerce_ingestion():
    @task
    def create_schema() -> None:
        PostgresHook(postgres_conn_id="analytics_db").run(
            SCHEMA_SQL.read_text(encoding="utf-8")
        )

    @task
    def load(dataset: str) -> list[str]:
        hook = S3Hook(aws_conn_id="minio_s3")
        keys = [
            key
            for key in (hook.list_keys(bucket_name=BUCKET, prefix=f"{dataset}/") or [])
            if key.endswith(".csv")
        ]
        if not keys:
            return []

        frames = [
            pd.read_csv(io.BytesIO(hook.get_key(key, BUCKET).get()["Body"].read()))
            for key in keys
        ]
        frame = _clean(pd.concat(frames, ignore_index=True), SPECS[dataset])
        rows = _upsert(frame, dataset, SPECS[dataset]["pk"])

        print(f"{dataset}: {rows} rows from {len(keys)} file(s)")
        return keys

    @task
    def archive(batches: list[list[str]]) -> int:
        hook = S3Hook(aws_conn_id="minio_s3")
        moved = 0
        for key in (key for batch in batches for key in batch):
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

    schema = create_schema()

    customers = load.override(task_id="load_customers")("customers")
    products = load.override(task_id="load_products")("products")
    orders = load.override(task_id="load_orders")("orders")
    order_items = load.override(task_id="load_order_items")("order_items")
    payments = load.override(task_id="load_payments")("payments")

    schema >> [customers, products] >> orders >> [order_items, payments]

    archive([customers, products, orders, order_items, payments])


ecommerce_ingestion()
