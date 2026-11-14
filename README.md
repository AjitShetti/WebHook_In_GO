# Go Webhook Delivery Service

A small at-least-once webhook delivery system built with Go, PostgreSQL, and Redis Streams.

## How an event moves through the system

1. `POST /events` validates the JSON and stores it in PostgreSQL's `outbox` table. It responds with `202 Accepted` and an event ID.
2. The dispatcher finds due outbox rows and publishes their IDs to the Redis Stream `webhook:deliveries`.
3. The worker reads the stream, loads the event from PostgreSQL, waits for its per-process rate limiter, signs the exact request body with HMAC-SHA256, and sends it to the subscriber.
4. A successful 2xx response marks the event delivered. A failed request is recorded and retried with exponential backoff and jitter.
5. After five failed HTTP attempts, the event moves to `dead_letters`. `POST /dead-letters/{id}/replay` puts it back into the outbox.

The worker opens its circuit breaker after five consecutive subscriber failures. While it is open, events are deferred without making another HTTP request or consuming a retry attempt. It allows traffic again after 30 seconds.

## Run locally

The Compose file expects the external Docker network `webhook_default` and a PostgreSQL container reachable as `webhook-postgres`. Copy `.env.example` to `.env`, then set `DATABASE_URL` and a private `WEBHOOK_SECRET`. Keep `.env` out of Git.

For a fresh database, apply these migration files in order: `001_create_outbox.sql`, `002_add_delivered_at.sql`, `002_add_outbox_delivery_status.sql`, `003_add_retries_and_dead_letters.sql`, `004_add_published_at.sql`, and `005_create_delivery_attempts.sql`. If you already applied migrations through `004`, apply only `005_create_delivery_attempts.sql`. Then start the services:

```sh
docker compose up -d --build
```

Send an event:

```sh
curl -i http://localhost:18080/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"demo.created","data":{"message":"hello"}}'
```

View recent HTTP attempts at [http://localhost:18080/admin/attempts](http://localhost:18080/admin/attempts). The worker exposes Prometheus text metrics at `/metrics` on port `9090` inside the Compose network.

To replay a dead-letter event, use its ID:

```sh
curl -i -X POST http://localhost:18080/dead-letters/EVENT_ID/replay
```

## Delivery guarantee

Delivery is **at least once**, so a subscriber can receive the same event more than once. A worker can send the request and then stop before saving the success, causing a retry. Subscribers should deduplicate using the stable `X-Webhook-ID` header. The HMAC is sent in `X-Webhook-Signature` as `sha256=<hex>` and is calculated over the exact request body.

The rate limiter and circuit breaker currently apply to the single `SUBSCRIBER_URL` configured for this worker process. Run a separate worker configuration for each subscriber endpoint if you need independent limits and breaker state.
