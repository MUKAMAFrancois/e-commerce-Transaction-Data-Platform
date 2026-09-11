"""Generator settings. Every value can be overridden by an environment variable.

Lives here rather than in config/ because Airflow reserves $AIRFLOW_HOME/config
and puts that directory itself on sys.path, which breaks `config.config` imports
inside a DAG.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

OUTPUT_DIR = Path(os.getenv("GENERATOR_OUTPUT_DIR", PROJECT_ROOT / "data" / "raw"))

NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", 1_000))
NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", 500))
NUM_ORDERS = int(os.getenv("NUM_ORDERS", 5_000))

MIN_ITEMS_PER_ORDER = 1
MAX_ITEMS_PER_ORDER = 4

# Seeds both `random` and Faker; 0 means don't seed.
SEED = int(os.getenv("GENERATOR_SEED", 42))

# Dates are measured back from here, not from the wall clock, so a seeded run
# reproduces. Defaults to midnight today: stable within a day, still drifts
# forward over time. CI can pin an exact value.
_reference = os.getenv("GENERATOR_REFERENCE_TIME")
REFERENCE_TIME = (
    datetime.fromisoformat(_reference)
    if _reference
    else datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
)

CUSTOMER_HISTORY = timedelta(days=730)
MAX_PAYMENT_DELAY = timedelta(days=3)

MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
MINIO_RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "ecommerce-raw")

DATASETS = ("customers", "products", "orders", "order_items", "payments")
