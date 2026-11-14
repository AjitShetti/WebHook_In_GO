ALTER TABLE outbox
ADD COLUMN attempt_count integer NOT NULL DEFAULT 0,
ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now(),
ADD COLUMN last_error text;

CREATE TABLE dead_letters (
    event_id         uuid PRIMARY KEY,
    event_type       text NOT NULL,
    payload          jsonb NOT NULL,
    created_at       timestamptz NOT NULL,
    attempts         integer NOT NULL,
    last_error       text NOT NULL,
    dead_lettered_at timestamptz NOT NULL DEFAULT now()
);