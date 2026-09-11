import psycopg2
import pytest

pytestmark = pytest.mark.integration

TABLES = ["customers", "products", "orders", "order_items", "payments"]

EXPECTED_COLUMNS = {
    "customers": {"customer_id", "email", "created_at", "city", "country"},
    "products": {"product_id", "product_name", "category", "price", "cost_price"},
    "orders": {"order_id", "customer_id", "order_date", "total_amount", "order_status"},
    "order_items": {"order_item_id", "order_id", "product_id", "quantity", "line_total"},
    "payments": {"payment_id", "order_id", "payment_status", "amount_paid"},
}

EXPECTED_FOREIGN_KEYS = {
    ("orders", "customers"),
    ("order_items", "orders"),
    ("order_items", "products"),
    ("payments", "orders"),
}


@pytest.mark.parametrize("table", TABLES)
def test_table_exists(query, table):
    rows = query(
        "SELECT 1 FROM information_schema.tables"
        " WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    assert rows, f"table {table} is missing"


@pytest.mark.parametrize("table", TABLES)
def test_expected_columns_exist(query, table):
    rows = query(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    actual = {row[0] for row in rows}
    assert EXPECTED_COLUMNS[table] <= actual


@pytest.mark.parametrize("table", TABLES)
def test_primary_key_is_defined(query, table):
    rows = query(
        "SELECT 1 FROM information_schema.table_constraints"
        " WHERE table_name = %s AND constraint_type = 'PRIMARY KEY'",
        (table,),
    )
    assert rows, f"{table} has no primary key"


def test_foreign_keys_are_defined(query):
    rows = query(
        """
        SELECT tc.table_name, ccu.table_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
        """
    )
    assert EXPECTED_FOREIGN_KEYS <= set(rows)


@pytest.mark.parametrize("table", TABLES)
def test_tables_hold_data(query, table):
    assert query(f"SELECT count(*) FROM {table}")[0][0] > 0


def test_primary_key_rejects_duplicates(db):
    with db.cursor() as cursor:
        cursor.execute("SELECT customer_id FROM customers LIMIT 1")
        existing = cursor.fetchone()[0]
        with pytest.raises(psycopg2.errors.UniqueViolation):
            cursor.execute(
                "INSERT INTO customers (customer_id, email) VALUES (%s, %s)",
                (existing, "duplicate@example.com"),
            )
    db.rollback()


def test_foreign_key_rejects_unknown_parent(db):
    with db.cursor() as cursor:
        with pytest.raises(psycopg2.errors.ForeignKeyViolation):
            cursor.execute(
                "INSERT INTO orders (order_id, customer_id, total_amount)"
                " VALUES ('FK-TEST-1', 'CUST-DOES-NOT-EXIST', 10.0)"
            )
    db.rollback()


class TestLoadedDataIntegrity:
    def test_no_orphan_rows(self, query):
        orphans = query(
            """
            SELECT
              (SELECT count(*) FROM orders o LEFT JOIN customers c USING (customer_id)
                 WHERE c.customer_id IS NULL),
              (SELECT count(*) FROM order_items i LEFT JOIN orders o USING (order_id)
                 WHERE o.order_id IS NULL),
              (SELECT count(*) FROM payments p LEFT JOIN orders o USING (order_id)
                 WHERE o.order_id IS NULL)
            """
        )[0]
        assert orphans == (0, 0, 0)

    def test_order_totals_match_line_items(self, query):
        mismatches = query(
            """
            SELECT count(*) FROM (
              SELECT o.order_id FROM orders o JOIN order_items i USING (order_id)
              GROUP BY o.order_id, o.total_amount, o.shipping_fee
              HAVING abs(sum(i.line_total) + o.shipping_fee - o.total_amount) > 0.011
            ) bad
            """
        )[0][0]
        assert mismatches == 0

    def test_no_negative_money(self, query):
        negatives = query(
            "SELECT (SELECT count(*) FROM orders WHERE total_amount < 0)"
            " + (SELECT count(*) FROM payments WHERE amount_paid < 0)"
            " + (SELECT count(*) FROM products WHERE price < 0 OR cost_price < 0)"
        )[0][0]
        assert negatives == 0

    def test_orders_never_predate_their_customer(self, query):
        early = query(
            "SELECT count(*) FROM orders o JOIN customers c USING (customer_id)"
            " WHERE o.order_date < c.created_at"
        )[0][0]
        assert early == 0
