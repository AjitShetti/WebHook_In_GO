CREATE TABLE outbox(
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type  text NOT NULL,
    payload     jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
