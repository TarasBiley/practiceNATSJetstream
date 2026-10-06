BEGIN;

CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    order_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Old incomplete records have no durable publication intent. Reconcile those
-- against JetStream before upgrading; silently replaying them could duplicate events.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'idempotency_keys'
          AND column_name = 'expected_sequence'
    ) AND EXISTS (SELECT 1 FROM idempotency_keys WHERE NOT completed) THEN
        RAISE EXCEPTION 'Reconcile incomplete legacy idempotency_keys before migrating';
    END IF;
END $$;

ALTER TABLE idempotency_keys ADD COLUMN IF NOT EXISTS expected_sequence BIGINT;
ALTER TABLE idempotency_keys ADD COLUMN IF NOT EXISTS published_sequence BIGINT;

-- One unfinished update per order preserves the last JetStream message for
-- recovery even after the server deduplication window has expired.
CREATE UNIQUE INDEX IF NOT EXISTS idempotency_pending_order
    ON idempotency_keys (order_id) WHERE NOT completed;

COMMIT;
