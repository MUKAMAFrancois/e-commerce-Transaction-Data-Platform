import pandas as pd
import pytest

from common.quality import clean, validate


def order_frame(**overrides):
    row = {
        "order_id": "ORD-0000001",
        "customer_id": "CUST-000001",
        "order_date": "2026-05-01 10:00:00",
        "items_subtotal": 10.0,
        "shipping_fee": 5.0,
        "total_amount": 15.0,
        "order_status": "delivered",
        "tracking_number": "TRK-1",
    }
    row.update(overrides)
    return pd.DataFrame([row])


CUSTOMERS = {"customers": {"CUST-000001"}}


class TestClean:
    def test_strips_whitespace(self):
        frame = clean(order_frame(order_status="  delivered  "), "orders")
        assert frame.loc[0, "order_status"] == "delivered"

    def test_lowercases_configured_columns(self):
        frame = clean(order_frame(order_status="DELIVERED"), "orders")
        assert frame.loc[0, "order_status"] == "delivered"

    def test_lowercases_email(self):
        frame = pd.DataFrame(
            [{"customer_id": "C1", "email": "Mixed.Case@Example.COM", "created_at": "2026-01-01"}]
        )
        assert clean(frame, "customers").loc[0, "email"] == "mixed.case@example.com"

    def test_coerces_numerics(self):
        frame = clean(order_frame(total_amount="15.00"), "orders")
        assert frame.loc[0, "total_amount"] == 15.0

    def test_deduplicates_on_primary_key_keeping_last(self):
        frame = pd.concat(
            [order_frame(total_amount=15.0), order_frame(total_amount=99.0)],
            ignore_index=True,
        )
        cleaned = clean(frame, "orders")
        assert len(cleaned) == 1
        assert cleaned.loc[0, "total_amount"] == 99.0

    def test_mixed_timestamp_precision_survives(self):
        """Regression: inferring one format from the first row turned any
        timestamp of a different shape into NaT, which validation then read as
        a missing value."""
        frame = pd.concat(
            [
                order_frame(order_id="ORD-1", order_date="2026-05-20 22:06:37.395775"),
                order_frame(order_id="ORD-2", order_date="2026-05-01 10:00:00"),
            ],
            ignore_index=True,
        )
        cleaned = clean(frame, "orders")
        assert cleaned["order_date"].notna().all()

    def test_unparseable_date_becomes_null(self):
        cleaned = clean(order_frame(order_date="not-a-date"), "orders")
        assert pd.isna(cleaned.loc[0, "order_date"])


class TestValidate:
    def test_clean_row_is_accepted(self):
        accepted, rejected = validate(clean(order_frame(), "orders"), "orders", CUSTOMERS)
        assert len(accepted) == 1
        assert rejected.empty

    @pytest.mark.parametrize(
        "overrides, reason",
        [
            ({"customer_id": None}, "missing customer_id"),
            ({"total_amount": -15.0}, "negative total_amount"),
            ({"order_status": "banana"}, "unexpected order_status"),
            ({"customer_id": "CUST-999999"}, "customer_id not found in customers"),
            ({"total_amount": 999.0}, "total_amount != items_subtotal + shipping_fee"),
        ],
    )
    def test_each_rule_rejects_with_its_reason(self, overrides, reason):
        frame = clean(order_frame(**overrides), "orders")
        accepted, rejected = validate(frame, "orders", CUSTOMERS)
        assert accepted.empty
        assert rejected.loc[0, "rejection_reason"] == reason

    def test_rejected_rows_carry_a_reason_column(self):
        frame = clean(order_frame(total_amount=-1.0), "orders")
        _, rejected = validate(frame, "orders", CUSTOMERS)
        assert "rejection_reason" in rejected.columns

    def test_good_and_bad_rows_are_split_not_dropped(self):
        frame = pd.concat(
            [order_frame(order_id="ORD-1"), order_frame(order_id="ORD-2", total_amount=-5.0)],
            ignore_index=True,
        )
        accepted, rejected = validate(clean(frame, "orders"), "orders", CUSTOMERS)
        assert len(accepted) == 1 and len(rejected) == 1
        assert len(accepted) + len(rejected) == 2

    def test_line_total_consistency(self):
        frame = pd.DataFrame(
            [
                {
                    "order_item_id": "ITEM-1",
                    "order_id": "ORD-1",
                    "product_id": "PROD-1",
                    "quantity": 2,
                    "unit_price": 10.0,
                    "line_total": 999.0,
                }
            ]
        )
        _, rejected = validate(clean(frame, "order_items"), "order_items")
        assert rejected.loc[0, "rejection_reason"] == "line_total != quantity * unit_price"

    def test_unsettled_payment_cannot_carry_an_amount(self):
        frame = pd.DataFrame(
            [
                {
                    "payment_id": "PAY-1",
                    "order_id": "ORD-1",
                    "payment_method": "credit_card",
                    "payment_status": "failed",
                    "transaction_reference": "TXN-1",
                    "amount_paid": 50.0,
                    "paid_at": None,
                }
            ]
        )
        _, rejected = validate(clean(frame, "payments"), "payments")
        assert rejected.loc[0, "rejection_reason"] == "unsettled payment has amount_paid > 0"

    def test_foreign_keys_are_skipped_when_parents_are_unknown(self):
        frame = clean(order_frame(customer_id="CUST-999999"), "orders")
        accepted, _ = validate(frame, "orders")
        assert len(accepted) == 1
