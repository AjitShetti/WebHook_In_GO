CREATE TABLE delivery_attempts (
    id              bigserial PRIMARY KEY,
    event_id        uuid NOT NULL,
    attempt_number  integer NOT NULL,
    attempted_at    timestamptz NOT NULL DEFAULT now(),
    status_code     integer NOT NULL DEFAULT 0,
    succeeded       boolean NOT NULL,
    error_message   text NOT NULL DEFAULT '',
    duration_ms     bigint NOT NULL
);

CREATE INDEX delivery_attempts_attempted_at_idx
    ON delivery_attempts (attempted_at DESC);

CREATE INDEX delivery_attempts_event_id_idx
    ON delivery_attempts (event_id, attempt_number);
