ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS delivered_at timestamptz;
