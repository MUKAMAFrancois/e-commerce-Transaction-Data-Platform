import os

import pytest

requests = pytest.importorskip("requests")

pytestmark = pytest.mark.integration

METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000").rstrip("/")

DATABASE_NAME = "E-commerce Analytics"
DASHBOARD_NAME = "E-commerce KPIs"
EXPECTED_CARDS = {
    "Total Orders",
    "Total Revenue",
    "Average Order Value",
    "Orders Over Time",
    "Revenue Over Time",
    "Revenue by Category",
    "Order Status Distribution",
    "Payment Status Distribution",
}


def test_metabase_is_healthy():
    response = requests.get(f"{METABASE_URL}/api/health", timeout=30)
    assert response.ok


def test_session_can_authenticate(metabase):
    user = metabase.get(f"{metabase.base}/api/user/current", timeout=30).json()
    assert user["is_superuser"]


def test_analytics_database_is_registered(metabase):
    payload = metabase.get(f"{metabase.base}/api/database", timeout=30).json()
    databases = payload.get("data", payload if isinstance(payload, list) else [])
    assert DATABASE_NAME in {database["name"] for database in databases}


def test_all_five_tables_are_visible(metabase):
    payload = metabase.get(f"{metabase.base}/api/database", timeout=30).json()
    databases = payload.get("data", payload if isinstance(payload, list) else [])
    database = next(d for d in databases if d["name"] == DATABASE_NAME)

    metadata = metabase.get(
        f"{metabase.base}/api/database/{database['id']}/metadata", timeout=60
    ).json()
    tables = {table["name"] for table in metadata.get("tables", [])}
    assert {"customers", "products", "orders", "order_items", "payments"} <= tables


def test_dashboard_exists_with_every_card(metabase):
    dashboards = metabase.get(f"{metabase.base}/api/dashboard", timeout=30).json()
    dashboard = next((d for d in dashboards if d["name"] == DASHBOARD_NAME), None)
    assert dashboard is not None, f"dashboard '{DASHBOARD_NAME}' was not provisioned"

    detail = metabase.get(
        f"{metabase.base}/api/dashboard/{dashboard['id']}", timeout=30
    ).json()
    names = {card["card"]["name"] for card in detail["dashcards"]}
    assert EXPECTED_CARDS <= names


@pytest.mark.parametrize("card_name", sorted(EXPECTED_CARDS))
def test_each_card_returns_data(metabase, card_name):
    cards = metabase.get(f"{metabase.base}/api/card", timeout=30).json()
    card = next((c for c in cards if c["name"] == card_name), None)
    assert card is not None, f"card '{card_name}' is missing"

    result = metabase.post(
        f"{metabase.base}/api/card/{card['id']}/query", timeout=120
    ).json()
    assert result["status"] == "completed", result.get("error")
    assert result["data"]["rows"], f"card '{card_name}' returned no rows"
