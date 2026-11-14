#!/bin/sh
set -eu

# The host-side DATABASE_URL uses localhost. Inside this Compose network,
# the existing Postgres container is reachable by its container name.
if [ -n "${DATABASE_URL:-}" ]; then
    DATABASE_URL="$(printf '%s' "$DATABASE_URL" | sed \
        -e 's/@localhost:/@webhook-postgres:/' \
        -e 's/@127\.0\.0\.1:/@webhook-postgres:/')"
    export DATABASE_URL
fi

exec "$@"
