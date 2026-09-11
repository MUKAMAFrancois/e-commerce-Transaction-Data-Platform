"""End-to-end: MinIO -> Airflow -> PostgreSQL.

Uploads a small self-consistent dataset under TEST- keys, runs the DAG, then
asserts the rows reached PostgreSQL and the raw objects were archived.
"""

import subprocess
import uuid
from datetime import datetime, timedelta

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.slow]

DELETE_ORDER = [
    ("payments", "payment_id"),
    ("order_items", "order_item_id"),
    ("orders", "order_id"),
    ("customers", "customer_id"),
    ("products", "product_id"),
]


@pytest.fixture
def marker():
    return uuid.uuid4().hex[:8].upper()


def build_csvs(marker):
    created = datetime.now() - timedelta(days=400)
    ordered = created + timedelta(days=10)
    paid = ordered + timedelta(hours=6)

    unit_price, quantity, shipping = 25.0, 4, 7.5
    line_total = unit_price * quantity
    total = line_total + shipping

    return {
        "customers": (
            "customer_id,first_name,last_name,email,phone_number,shipping_address,"
            "billing_address,city,state,country,postal_code,created_at\n"
            f"TEST-CUST-{marker},Test,User,test-{marker.lower()}@example.com,555,"
            f"1 Test St,1 Test St,Testville,TS,Testland,00000,{created:%Y-%m-%d %H:%M:%S}\n"
        ),
        "products": (
            "product_id,product_name,description,category,price,cost_price,"
            "stock_quantity,is_active\n"
            f"TEST-PROD-{marker},Test Widget,A widget,Electronics,{unit_price},10.0,5,True\n"
        ),
        "orders": (
            "order_id,customer_id,order_date,items_subtotal,shipping_fee,total_amount,"
            "order_status,tracking_number\n"
            f"TEST-ORD-{marker},TEST-CUST-{marker},{ordered:%Y-%m-%d %H:%M:%S},"
            f"{line_total},{shipping},{total},delivered,TRK-{marker}\n"
        ),
        "order_items": (
            "order_item_id,order_id,product_id,quantity,unit_price,line_total\n"
            f"TEST-ITEM-{marker},TEST-ORD-{marker},TEST-PROD-{marker},"
            f"{quantity},{unit_price},{line_total}\n"
        ),
        "payments": (
            "payment_id,order_id,payment_method,payment_status,transaction_reference,"
            "amount_paid,paid_at\n"
            f"TEST-PAY-{marker},TEST-ORD-{marker},credit_card,completed,TXN-{marker},"
            f"{total},{paid:%Y-%m-%d %H:%M:%S}\n"
        ),
    }


@pytest.fixture
def uploaded(s3, bucket, marker, db):
    keys = []
    for dataset, body in build_csvs(marker).items():
        key = f"{dataset}/ingest_date={datetime.now():%Y-%m-%d}/test_{marker}.csv"
        s3.put_object(Bucket=bucket, Key=key, Body=body.encode())
        keys.append(key)

    yield keys

    with db.cursor() as cursor:
        for table, pk in DELETE_ORDER:
            cursor.execute(f"DELETE FROM {table} WHERE {pk} LIKE %s", (f"TEST-%{marker}",))
    db.commit()

    stale = [
        {"Key": obj["Key"]}
        for obj in s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        if marker in obj["Key"]
    ]
    if stale:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": stale})


def run_dag():
    logical_date = datetime.now().strftime("2027-%m-%dT%H:%M:%S")
    result = subprocess.run(
        ["airflow", "dags", "test", "ecommerce_ingestion", logical_date],
        capture_output=True,
        text=True,
        timeout=900,
    )
    return result.stdout + result.stderr


def test_pipeline_moves_data_from_minio_to_postgres(uploaded, query, s3, bucket, marker):
    output = run_dag()
    assert "state=success" in output or "Marking run" in output, output[-2000:]

    rows = query(
        "SELECT o.order_id, o.total_amount, c.customer_id, i.product_id, p.payment_status"
        " FROM orders o"
        " JOIN customers c USING (customer_id)"
        " JOIN order_items i USING (order_id)"
        " JOIN payments p USING (order_id)"
        " WHERE o.order_id = %s",
        (f"TEST-ORD-{marker}",),
    )
    assert len(rows) == 1, "the test order did not arrive intact across all four tables"

    order_id, total, customer_id, product_id, payment_status = rows[0]
    assert float(total) == 107.5
    assert customer_id == f"TEST-CUST-{marker}"
    assert product_id == f"TEST-PROD-{marker}"
    assert payment_status == "completed"


def test_consumed_objects_are_archived(uploaded, s3, bucket, marker):
    run_dag()

    keys = [obj["Key"] for obj in s3.list_objects_v2(Bucket=bucket).get("Contents", [])]
    mine = [key for key in keys if marker in key]

    assert mine, "test objects vanished entirely"
    assert all(key.startswith("processed/") for key in mine), mine


def test_reloading_the_same_data_does_not_duplicate_rows(uploaded, query, s3, bucket, marker):
    """The first run archives the files, so re-upload them to prove the upsert
    is what prevents duplication, not merely that nothing was found."""
    run_dag()
    before = query("SELECT count(*) FROM orders")[0][0]

    for dataset, body in build_csvs(marker).items():
        s3.put_object(
            Bucket=bucket,
            Key=f"{dataset}/ingest_date={datetime.now():%Y-%m-%d}/test_{marker}_again.csv",
            Body=body.encode(),
        )

    run_dag()
    assert query("SELECT count(*) FROM orders")[0][0] == before
