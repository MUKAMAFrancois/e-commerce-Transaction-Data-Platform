import pandas as pd
import pytest

from data_generator import settings
from data_generator.synthetic import _seed_everything, build_datasets

SMALL = dict(num_customers=40, num_products=25, num_orders=150)


@pytest.fixture(scope="module")
def data():
    _seed_everything(42)
    return build_datasets(**SMALL)


def generate(seed):
    _seed_everything(seed)
    return build_datasets(**SMALL)


class TestReproducibility:
    def test_same_seed_gives_identical_data(self):
        first, second = generate(42), generate(42)
        for name in first:
            pd.testing.assert_frame_equal(first[name], second[name])

    def test_different_seed_gives_different_data(self):
        assert not generate(42)["orders"].equals(generate(99)["orders"])


class TestKeys:
    @pytest.mark.parametrize(
        "dataset, pk",
        [
            ("customers", "customer_id"),
            ("products", "product_id"),
            ("orders", "order_id"),
            ("order_items", "order_item_id"),
            ("payments", "payment_id"),
        ],
    )
    def test_primary_keys_are_unique(self, data, dataset, pk):
        assert not data[dataset][pk].duplicated().any()

    def test_emails_are_unique(self, data):
        assert not data["customers"]["email"].duplicated().any()


class TestReferentialIntegrity:
    def test_orders_reference_real_customers(self, data):
        assert data["orders"]["customer_id"].isin(data["customers"]["customer_id"]).all()

    def test_items_reference_real_orders_and_products(self, data):
        assert data["order_items"]["order_id"].isin(data["orders"]["order_id"]).all()
        assert data["order_items"]["product_id"].isin(data["products"]["product_id"]).all()

    def test_payments_reference_real_orders(self, data):
        assert data["payments"]["order_id"].isin(data["orders"]["order_id"]).all()

    def test_every_order_has_items_and_one_payment(self, data):
        orders = set(data["orders"]["order_id"])
        assert set(data["order_items"]["order_id"]) == orders
        assert len(data["payments"]) == len(orders)


class TestBusinessConsistency:
    def test_total_amount_matches_line_items(self, data):
        summed = data["order_items"].groupby("order_id")["line_total"].sum()
        merged = data["orders"].join(summed, on="order_id", rsuffix="_calc")
        drift = (merged["line_total"] + merged["shipping_fee"] - merged["total_amount"]).abs()
        assert (drift <= 0.011).all()

    def test_line_total_matches_quantity_times_price(self, data):
        items = data["order_items"]
        drift = (items["quantity"] * items["unit_price"] - items["line_total"]).abs()
        assert (drift <= 0.011).all()

    def test_monetary_values_are_not_negative(self, data):
        assert (data["orders"]["total_amount"] >= 0).all()
        assert (data["payments"]["amount_paid"] >= 0).all()
        assert (data["products"][["price", "cost_price"]] >= 0).all().all()

    def test_selling_price_exceeds_cost(self, data):
        assert (data["products"]["price"] > data["products"]["cost_price"]).all()


class TestTemporalConsistency:
    def test_orders_never_predate_their_customer(self, data):
        merged = data["orders"].merge(
            data["customers"][["customer_id", "created_at"]], on="customer_id"
        )
        assert (merged["order_date"] >= merged["created_at"]).all()

    def test_payments_settle_after_their_order(self, data):
        merged = data["payments"].merge(
            data["orders"][["order_id", "order_date"]], on="order_id"
        ).dropna(subset=["paid_at"])
        assert (merged["paid_at"] >= merged["order_date"]).all()

    def test_no_dates_in_the_future(self, data):
        assert (data["orders"]["order_date"] <= settings.REFERENCE_TIME).all()
        assert (data["customers"]["created_at"] <= settings.REFERENCE_TIME).all()


class TestStatuses:
    def test_dispatched_orders_are_paid(self, data):
        merged = data["orders"].merge(data["payments"], on="order_id")
        dispatched = merged[merged["order_status"].isin(["shipped", "delivered"])]
        assert (dispatched["payment_status"] == "completed").all()

    def test_unsettled_payments_have_no_amount(self, data):
        unsettled = data["payments"][data["payments"]["payment_status"].isin(["pending", "failed"])]
        assert (unsettled["amount_paid"] == 0).all()

    def test_only_dispatched_orders_have_tracking(self, data):
        tracked = data["orders"].dropna(subset=["tracking_number"])
        assert tracked["order_status"].isin(["shipped", "delivered"]).all()
