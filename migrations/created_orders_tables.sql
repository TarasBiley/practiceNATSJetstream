CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    status TEXT NOT NULL
        CHECK (status IN ('created', 'paid', 'shipped', 'delivered')),
    comment TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);