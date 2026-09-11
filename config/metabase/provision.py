"""Provision Metabase over its API: admin user, database connection, KPI cards,
dashboard.

Idempotent - existing objects are reused or updated, so it can run on every
`docker compose up`.

Revenue is SUM(amount_paid) WHERE payment_status = 'completed', matching the
convention the generator documents.
"""

import os
import sys
import time

import requests

BASE = os.getenv("METABASE_URL", "http://metabase:3000").rstrip("/")
EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@example.com")
PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD", "MetabaseAdmin123!")

DB_NAME = "E-commerce Analytics"
DASHBOARD_NAME = "E-commerce KPIs"

DB_DETAILS = {
    "host": "postgres",
    "port": 5432,
    "dbname": os.getenv("POSTGRES_DB", "ecommerce_db"),
    "user": os.getenv("POSTGRES_USER", "ecommerce_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "ecommerce_password"),
    "ssl": False,
}

CARDS = [
    {
        "name": "Total Orders",
        "display": "scalar",
        "sql": "SELECT count(*) AS orders FROM orders",
        "settings": {},
        "pos": (0, 0, 8, 3),
    },
    {
        "name": "Total Revenue",
        "display": "scalar",
        "sql": (
            "SELECT round(sum(amount_paid), 2) AS revenue FROM payments"
            " WHERE payment_status = 'completed'"
        ),
        "settings": {"column_settings": {'["name","revenue"]': {"number_style": "currency"}}},
        "pos": (0, 8, 8, 3),
    },
    {
        "name": "Average Order Value",
        "display": "scalar",
        "sql": (
            "SELECT round(sum(amount_paid) / nullif(count(*), 0), 2) AS aov"
            " FROM payments WHERE payment_status = 'completed'"
        ),
        "settings": {"column_settings": {'["name","aov"]': {"number_style": "currency"}}},
        "pos": (0, 16, 8, 3),
    },
    {
        "name": "Orders Over Time",
        "display": "line",
        "sql": (
            "SELECT date_trunc('month', order_date)::date AS month, count(*) AS orders"
            " FROM orders GROUP BY 1 ORDER BY 1"
        ),
        "settings": {"graph.dimensions": ["month"], "graph.metrics": ["orders"]},
        "pos": (3, 0, 12, 5),
    },
    {
        "name": "Revenue Over Time",
        "display": "line",
        "sql": (
            "SELECT date_trunc('month', paid_at)::date AS month,"
            " round(sum(amount_paid), 2) AS revenue FROM payments"
            " WHERE payment_status = 'completed' GROUP BY 1 ORDER BY 1"
        ),
        "settings": {"graph.dimensions": ["month"], "graph.metrics": ["revenue"]},
        "pos": (3, 12, 12, 5),
    },
    {
        "name": "Revenue by Category",
        "display": "bar",
        "sql": (
            "SELECT p.category, round(sum(i.line_total), 2) AS revenue"
            " FROM order_items i JOIN products p USING (product_id)"
            " GROUP BY 1 ORDER BY 2 DESC"
        ),
        "settings": {"graph.dimensions": ["category"], "graph.metrics": ["revenue"]},
        "pos": (8, 0, 12, 5),
    },
    {
        "name": "Order Status Distribution",
        "display": "pie",
        "sql": (
            "SELECT order_status, count(*) AS orders FROM orders"
            " GROUP BY 1 ORDER BY 2 DESC"
        ),
        "settings": {"pie.dimension": "order_status", "pie.metric": "orders"},
        "pos": (8, 12, 6, 5),
    },
    {
        "name": "Payment Status Distribution",
        "display": "pie",
        "sql": (
            "SELECT payment_status, count(*) AS payments FROM payments"
            " GROUP BY 1 ORDER BY 2 DESC"
        ),
        "settings": {"pie.dimension": "payment_status", "pie.metric": "payments"},
        "pos": (8, 18, 6, 5),
    },
]


class Metabase:
    def __init__(self) -> None:
        self.session = requests.Session()

    def _url(self, path: str) -> str:
        return f"{BASE}/api/{path.lstrip('/')}"

    def request(self, method: str, path: str, **kwargs):
        response = self.session.request(method, self._url(path), timeout=60, **kwargs)
        if not response.ok:
            raise RuntimeError(f"{method} {path} -> {response.status_code} {response.text[:400]}")
        return response.json() if response.text else None

    def get(self, path):
        return self.request("GET", path)

    def post(self, path, payload):
        return self.request("POST", path, json=payload)

    def put(self, path, payload):
        return self.request("PUT", path, json=payload)


def wait_for_metabase(mb: Metabase, attempts: int = 60) -> dict:
    for attempt in range(attempts):
        try:
            return mb.get("session/properties")
        except Exception:
            if attempt == attempts - 1:
                raise
            time.sleep(5)
    raise RuntimeError("unreachable")


def authenticate(mb: Metabase, properties: dict) -> None:
    if not properties.get("has-user-setup"):
        token = properties["setup-token"]
        mb.post(
            "setup",
            {
                "token": token,
                "user": {
                    "first_name": "Platform",
                    "last_name": "Admin",
                    "email": EMAIL,
                    "password": PASSWORD,
                    "site_name": "E-commerce Data Platform",
                },
                "prefs": {
                    "site_name": "E-commerce Data Platform",
                    "site_locale": "en",
                    "allow_tracking": False,
                },
            },
        )
        print(f"created admin user {EMAIL}")
        return

    session = mb.post("session", {"username": EMAIL, "password": PASSWORD})
    mb.session.headers["X-Metabase-Session"] = session["id"]
    print(f"signed in as {EMAIL}")


def ensure_database(mb: Metabase) -> int:
    existing = mb.get("database")
    for database in existing.get("data", existing if isinstance(existing, list) else []):
        if database["name"] == DB_NAME:
            print(f"database '{DB_NAME}' already registered (id={database['id']})")
            return database["id"]

    created = mb.post(
        "database",
        {
            "name": DB_NAME,
            "engine": "postgres",
            "details": DB_DETAILS,
            "is_full_sync": True,
        },
    )
    print(f"registered database '{DB_NAME}' (id={created['id']})")
    return created["id"]


def sync_tables(mb: Metabase, database_id: int, expected: int = 5, attempts: int = 10) -> None:
    """Best effort: on a fresh stack the DAG has not created the tables yet.

    Native SQL cards do not need synced metadata, so this never blocks the
    dashboard. sync_schema is asynchronous, so an empty first response means
    "not finished", not "nothing to find" - it has to be polled out.
    """
    mb.post(f"database/{database_id}/sync_schema", {})
    tables: list[str] = []

    for _ in range(attempts):
        metadata = mb.get(f"database/{database_id}/metadata")
        tables = [t["name"] for t in metadata.get("tables", [])]
        if len(tables) >= expected:
            print(f"synced {len(tables)} tables: {', '.join(sorted(tables))}")
            return
        time.sleep(3)

    print(f"{len(tables)} of {expected} tables synced - run the DAG, then rerun this")


def ensure_cards(mb: Metabase, database_id: int) -> dict[str, int]:
    existing = {card["name"]: card for card in mb.get("card")}
    ids = {}

    for spec in CARDS:
        payload = {
            "name": spec["name"],
            "display": spec["display"],
            "dataset_query": {
                "type": "native",
                "native": {"query": spec["sql"]},
                "database": database_id,
            },
            "visualization_settings": spec["settings"],
        }

        if spec["name"] in existing:
            card_id = existing[spec["name"]]["id"]
            mb.put(f"card/{card_id}", payload)
            print(f"updated card '{spec['name']}'")
        else:
            card_id = mb.post("card", payload)["id"]
            print(f"created card '{spec['name']}'")

        ids[spec["name"]] = card_id

    return ids


def ensure_dashboard(mb: Metabase, card_ids: dict[str, int]) -> int:
    matches = mb.get("dashboard")
    dashboard = next((d for d in matches if d["name"] == DASHBOARD_NAME), None)

    if dashboard is None:
        dashboard = mb.post(
            "dashboard",
            {"name": DASHBOARD_NAME, "description": "Core e-commerce KPIs."},
        )
        print(f"created dashboard '{DASHBOARD_NAME}' (id={dashboard['id']})")

    dashcards = []
    for index, spec in enumerate(CARDS):
        row, col, size_x, size_y = spec["pos"]
        dashcards.append(
            {
                "id": -(index + 1),
                "card_id": card_ids[spec["name"]],
                "row": row,
                "col": col,
                "size_x": size_x,
                "size_y": size_y,
                "parameter_mappings": [],
                "visualization_settings": {},
            }
        )

    mb.put(f"dashboard/{dashboard['id']}", {"dashcards": dashcards})
    print(f"placed {len(dashcards)} cards on the dashboard")
    return dashboard["id"]


def main() -> int:
    mb = Metabase()
    properties = wait_for_metabase(mb)
    authenticate(mb, properties)

    database_id = ensure_database(mb)
    card_ids = ensure_cards(mb, database_id)
    dashboard_id = ensure_dashboard(mb, card_ids)
    sync_tables(mb, database_id)

    print(f"\ndashboard ready: {BASE}/dashboard/{dashboard_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
