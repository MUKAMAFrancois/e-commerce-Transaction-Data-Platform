"""E-commerce synthetic data generator.

Star schema: customers, products (dimensions), orders, order_items, payments
(facts). orders.total_amount is derived from order_items, so every dashboard
figure traces back to line items.

payments.amount_paid is the amount charged, so a refund keeps its original
amount. Net revenue is SUM(amount_paid) WHERE payment_status = 'completed'.

    python -m data_generator.synthetic --dest local
    python -m data_generator.synthetic --dest minio
"""

import argparse
import io
import random
from datetime import datetime

import pandas as pd
from faker import Faker

from data_generator import settings

fake = Faker()


def _seed_everything(seed: int) -> None:
    # fake.unique keeps its own registry of already-issued values and survives
    # Faker.seed(); leaving it set makes a second run in the same process draw
    # extra values on collision and diverge from the first.
    fake.unique.clear()
    if seed:
        random.seed(seed)
        Faker.seed(seed)


def generate_customers(count: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "customer_id": f"CUST-{i:06d}",
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.unique.email(),
                "phone_number": fake.phone_number(),
                "shipping_address": fake.street_address(),
                "billing_address": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "country": fake.country(),
                "postal_code": fake.postcode(),
                "created_at": fake.date_time_between_dates(
                    datetime_start=settings.REFERENCE_TIME - settings.CUSTOMER_HISTORY,
                    datetime_end=settings.REFERENCE_TIME,
                ),
            }
            for i in range(1, count + 1)
        ]
    )


def generate_products(count: int) -> pd.DataFrame:
    categories = [
        "Electronics",
        "Clothing",
        "Home",
        "Books",
        "Sports",
        "Beauty",
        "Toys",
    ]

    products = []
    for i in range(1, count + 1):
        cost_price = round(random.uniform(5, 500), 2)
        products.append(
            {
                "product_id": f"PROD-{i:06d}",
                "product_name": fake.catch_phrase(),
                "description": fake.text(max_nb_chars=200),
                "category": random.choice(categories),
                "price": round(cost_price * random.uniform(1.1, 2.0), 2),
                "cost_price": cost_price,
                "stock_quantity": random.randint(0, 1_000),
                "is_active": random.choice([True, True, True, False]),
            }
        )
    return pd.DataFrame(products)


def generate_orders_and_items(
    count: int,
    customers: pd.DataFrame,
    products: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Headers and line items in one pass, so total_amount matches the items."""
    statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]

    customer_records = customers[["customer_id", "created_at"]].to_dict("records")
    product_records = products[["product_id", "price"]].to_dict("records")

    orders = []
    items = []
    item_seq = 1

    for i in range(1, count + 1):
        customer = random.choice(customer_records)

        # Anchored to signup: an order can never predate its customer.
        order_date = fake.date_time_between_dates(
            datetime_start=customer["created_at"],
            datetime_end=settings.REFERENCE_TIME,
        )

        basket = random.sample(
            product_records,
            k=random.randint(
                settings.MIN_ITEMS_PER_ORDER,
                settings.MAX_ITEMS_PER_ORDER,
            ),
        )

        order_id = f"ORD-{i:07d}"
        items_subtotal = 0.0

        for product in basket:
            quantity = random.randint(1, 5)
            unit_price = float(product["price"])
            line_total = round(unit_price * quantity, 2)
            items_subtotal += line_total

            items.append(
                {
                    "order_item_id": f"ITEM-{item_seq:08d}",
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )
            item_seq += 1

        shipping_fee = round(random.uniform(0, 30), 2)
        status = random.choice(statuses)

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_date": order_date,
                "items_subtotal": round(items_subtotal, 2),
                "shipping_fee": shipping_fee,
                "total_amount": round(items_subtotal + shipping_fee, 2),
                "order_status": status,
                "tracking_number": (
                    fake.bothify(text="TRK-##########")
                    if status in ("shipped", "delivered")
                    else None
                ),
            }
        )

    return pd.DataFrame(orders), pd.DataFrame(items)


def generate_payments(orders: pd.DataFrame) -> pd.DataFrame:
    payment_methods = ["credit_card", "debit_card", "mobile_money", "bank_transfer"]

    payments = []
    for i, order in enumerate(orders.to_dict("records"), start=1):
        status = order["order_status"]

        if status == "cancelled":
            payment_status = random.choice(["failed", "refunded", "refunded"])
        elif status in ("shipped", "delivered"):
            payment_status = "completed"
        else:
            payment_status = random.choice(["completed", "completed", "pending"])

        settled = payment_status in ("completed", "refunded")

        paid_at = None
        if settled:
            paid_at = fake.date_time_between_dates(
                datetime_start=order["order_date"],
                datetime_end=min(
                    order["order_date"] + settings.MAX_PAYMENT_DELAY,
                    settings.REFERENCE_TIME,
                ),
            )

        payments.append(
            {
                "payment_id": f"PAY-{i:07d}",
                "order_id": order["order_id"],
                "payment_method": random.choice(payment_methods),
                "payment_status": payment_status,
                # Not uuid4: it reads os.urandom and ignores the seed.
                "transaction_reference": f"TXN-{random.getrandbits(48):012X}",
                "amount_paid": order["total_amount"] if settled else 0.0,
                "paid_at": paid_at,
            }
        )

    return pd.DataFrame(payments)


def write_local(frames: dict[str, pd.DataFrame], output_dir) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, frame in frames.items():
        frame.to_csv(output_dir / f"{name}.csv", index=False)
    print(f"wrote {len(frames)} files to {output_dir}")


def write_minio(frames: dict[str, pd.DataFrame]) -> None:
    """Upload to <dataset>/ingest_date=YYYY-MM-DD/<dataset>_HHMMSS.csv.

    Uses boto3 rather than Airflow's S3Hook so this runs standalone and in CI.
    """
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=settings.MINIO_ENDPOINT_URL,
        aws_access_key_id=settings.MINIO_ACCESS_KEY,
        aws_secret_access_key=settings.MINIO_SECRET_KEY,
    )

    stamp = datetime.now()
    for name, frame in frames.items():
        key = f"{name}/ingest_date={stamp:%Y-%m-%d}/{name}_{stamp:%H%M%S}.csv"
        buffer = io.BytesIO(frame.to_csv(index=False).encode("utf-8"))
        client.put_object(
            Bucket=settings.MINIO_RAW_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
        )
        print(f"uploaded s3://{settings.MINIO_RAW_BUCKET}/{key}")


def build_datasets(
    num_customers: int,
    num_products: int,
    num_orders: int,
) -> dict[str, pd.DataFrame]:
    customers = generate_customers(num_customers)
    products = generate_products(num_products)
    orders, order_items = generate_orders_and_items(num_orders, customers, products)
    payments = generate_payments(orders)

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data.")
    parser.add_argument("--dest", choices=("local", "minio", "both"), default="local")
    parser.add_argument("--seed", type=int, default=settings.SEED)
    parser.add_argument("--customers", type=int, default=settings.NUM_CUSTOMERS)
    parser.add_argument("--products", type=int, default=settings.NUM_PRODUCTS)
    parser.add_argument("--orders", type=int, default=settings.NUM_ORDERS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _seed_everything(args.seed)

    frames = build_datasets(args.customers, args.products, args.orders)

    if args.dest in ("local", "both"):
        write_local(frames, settings.OUTPUT_DIR)
    if args.dest in ("minio", "both"):
        write_minio(frames)

    print(f"\ndone (seed={args.seed or 'random'})")
    for name, frame in frames.items():
        print(f"  {name:<12} {len(frame):>8,} rows")


if __name__ == "__main__":
    main()
