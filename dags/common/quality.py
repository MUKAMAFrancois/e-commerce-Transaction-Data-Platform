"""Cleaning and data-quality rules for the ingestion pipeline.

Validation splits a frame rather than dropping rows: everything that fails a
rule is returned with a rejection_reason so it can be quarantined and inspected
instead of disappearing.
"""

import pandas as pd

MONEY_TOLERANCE = 0.011

ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]
PAYMENT_STATUSES = ["completed", "pending", "failed", "refunded"]
PAYMENT_METHODS = ["credit_card", "debit_card", "mobile_money", "bank_transfer"]

SPECS = {
    "customers": {
        "pk": "customer_id",
        "dates": ["created_at"],
        "lower": ["email"],
        "numeric": [],
        "required": ["customer_id", "email", "created_at"],
        "non_negative": [],
        "allowed": {},
        "foreign_keys": {},
    },
    "products": {
        "pk": "product_id",
        "dates": [],
        "lower": [],
        "numeric": ["price", "cost_price", "stock_quantity"],
        "required": ["product_id", "product_name", "price", "cost_price"],
        "non_negative": ["price", "cost_price", "stock_quantity"],
        "allowed": {},
        "foreign_keys": {},
    },
    "orders": {
        "pk": "order_id",
        "dates": ["order_date"],
        "lower": ["order_status"],
        "numeric": ["items_subtotal", "shipping_fee", "total_amount"],
        "required": ["order_id", "customer_id", "order_date", "total_amount"],
        "non_negative": ["items_subtotal", "shipping_fee", "total_amount"],
        "allowed": {"order_status": ORDER_STATUSES},
        "foreign_keys": {"customer_id": ("customers", "customer_id")},
    },
    "order_items": {
        "pk": "order_item_id",
        "dates": [],
        "lower": [],
        "numeric": ["quantity", "unit_price", "line_total"],
        "required": ["order_item_id", "order_id", "product_id", "quantity"],
        "non_negative": ["quantity", "unit_price", "line_total"],
        "allowed": {},
        "foreign_keys": {
            "order_id": ("orders", "order_id"),
            "product_id": ("products", "product_id"),
        },
    },
    "payments": {
        "pk": "payment_id",
        "dates": ["paid_at"],
        "lower": ["payment_method", "payment_status"],
        "numeric": ["amount_paid"],
        "required": ["payment_id", "order_id", "payment_status"],
        "non_negative": ["amount_paid"],
        "allowed": {
            "payment_status": PAYMENT_STATUSES,
            "payment_method": PAYMENT_METHODS,
        },
        "foreign_keys": {"order_id": ("orders", "order_id")},
    },
}

LOAD_ORDER = ["customers", "products", "orders", "order_items", "payments"]


def clean(frame: pd.DataFrame, dataset: str) -> pd.DataFrame:
    spec = SPECS[dataset]

    for column in frame.select_dtypes(include="object"):
        frame[column] = frame[column].str.strip()

    for column in spec["lower"]:
        frame[column] = frame[column].str.lower()

    for column in spec["dates"]:
        # format="mixed" per value: inferring one format from the first row
        # coerces every timestamp of a different shape to NaT, which then looks
        # like a missing value to validation.
        frame[column] = pd.to_datetime(frame[column], format="mixed", errors="coerce")

    for column in spec["numeric"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    # Re-uploading a snapshot repeats primary keys; the newest copy wins.
    return frame.drop_duplicates(subset=[spec["pk"]], keep="last").reset_index(drop=True)


def _business_rule(frame: pd.DataFrame, dataset: str):
    """Cross-field consistency that a column-level rule cannot express."""
    if dataset == "orders":
        drift = (frame["items_subtotal"] + frame["shipping_fee"] - frame["total_amount"]).abs()
        return drift > MONEY_TOLERANCE, "total_amount != items_subtotal + shipping_fee"

    if dataset == "order_items":
        drift = (frame["quantity"] * frame["unit_price"] - frame["line_total"]).abs()
        return drift > MONEY_TOLERANCE, "line_total != quantity * unit_price"

    if dataset == "payments":
        unpaid = frame["payment_status"].isin(["pending", "failed"])
        return (unpaid & (frame["amount_paid"] > 0)), "unsettled payment has amount_paid > 0"

    return None


def validate(
    frame: pd.DataFrame,
    dataset: str,
    parent_keys: dict[str, set] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (accepted, rejected). Rejected carries a rejection_reason column."""
    spec = SPECS[dataset]
    parent_keys = parent_keys or {}
    reasons = pd.Series("", index=frame.index, dtype="object")

    def flag(mask, reason: str) -> None:
        reasons.loc[mask.fillna(False) & (reasons == "")] = reason

    for column in spec["required"]:
        flag(frame[column].isna(), f"missing {column}")

    for column in spec["non_negative"]:
        flag(frame[column] < 0, f"negative {column}")

    for column, allowed in spec["allowed"].items():
        flag(~frame[column].isin(allowed), f"unexpected {column}")

    for column, (parent, _) in spec["foreign_keys"].items():
        known = parent_keys.get(parent)
        if known is not None:
            flag(~frame[column].isin(known), f"{column} not found in {parent}")

    rule = _business_rule(frame, dataset)
    if rule is not None:
        flag(*rule)

    rejected = frame[reasons != ""].copy()
    rejected["rejection_reason"] = reasons[reasons != ""]
    return frame[reasons == ""].copy(), rejected
