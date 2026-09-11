import os

import pytest

# psycopg2, boto3 and requests are imported inside the fixtures rather than here:
# conftest is loaded for every run, and the unit suite must stay runnable on a
# bare checkout that has no database or object-store drivers installed.

BUCKET = os.getenv("MINIO_RAW_BUCKET", "ecommerce-raw")
METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000").rstrip("/")


@pytest.fixture(scope="session")
def db():
    import psycopg2

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=5432,
        dbname=os.getenv("POSTGRES_DB", "ecommerce_db"),
        user=os.getenv("POSTGRES_USER", "ecommerce_user"),
        password=os.getenv("POSTGRES_PASSWORD", "ecommerce_password"),
    )
    yield connection
    connection.close()


@pytest.fixture
def query(db):
    def run(sql, params=None):
        with db.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    return run


@pytest.fixture(scope="session")
def s3():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio_password"),
    )


@pytest.fixture(scope="session")
def bucket():
    return BUCKET


@pytest.fixture(scope="session")
def metabase():
    import requests

    session = requests.Session()
    response = session.post(
        f"{METABASE_URL}/api/session",
        json={
            "username": os.getenv("METABASE_ADMIN_EMAIL", "admin@example.com"),
            "password": os.getenv("METABASE_ADMIN_PASSWORD", "MetabaseAdmin123!"),
        },
        timeout=30,
    )
    if not response.ok:
        pytest.skip(f"metabase login failed: {response.status_code}")
    session.headers["X-Metabase-Session"] = response.json()["id"]
    session.base = METABASE_URL
    return session
