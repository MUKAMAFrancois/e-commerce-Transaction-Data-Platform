CREATE TABLE IF NOT EXISTS customers (
    customer_id      TEXT PRIMARY KEY,
    first_name       TEXT,
    last_name        TEXT,
    email            TEXT,
    phone_number     TEXT,
    shipping_address TEXT,
    billing_address  TEXT,
    city             TEXT,
    state            TEXT,
    country          TEXT,
    postal_code      TEXT,
    created_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id     TEXT PRIMARY KEY,
    product_name   TEXT,
    description    TEXT,
    category       TEXT,
    price          NUMERIC(12, 2),
    cost_price     NUMERIC(12, 2),
    stock_quantity INTEGER,
    is_active      BOOLEAN
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    customer_id     TEXT REFERENCES customers (customer_id),
    order_date      TIMESTAMP,
    items_subtotal  NUMERIC(12, 2),
    shipping_fee    NUMERIC(12, 2),
    total_amount    NUMERIC(12, 2),
    order_status    TEXT,
    tracking_number TEXT
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id      TEXT REFERENCES orders (order_id),
    product_id    TEXT REFERENCES products (product_id),
    quantity      INTEGER,
    unit_price    NUMERIC(12, 2),
    line_total    NUMERIC(12, 2)
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id            TEXT PRIMARY KEY,
    order_id              TEXT REFERENCES orders (order_id),
    payment_method        TEXT,
    payment_status        TEXT,
    transaction_reference TEXT,
    amount_paid           NUMERIC(12, 2),
    paid_at               TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders (order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (order_status);
CREATE INDEX IF NOT EXISTS idx_items_order ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_items_product ON order_items (product_id);
CREATE INDEX IF NOT EXISTS idx_payments_order ON payments (order_id);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments (payment_status);
