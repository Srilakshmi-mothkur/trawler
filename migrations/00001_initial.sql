-- +goose Up

-- Subscriber destinations.
-- Each endpoint receives webhook events via HTTP POST.
CREATE TABLE endpoints (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    url        TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Immutable application events accepted by trawler.
-- One logical event may fan out to many delivery jobs.
CREATE TABLE events (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per (event, endpoint) pair. 
-- Workers poll this table using FOR UPDATE SKIP LOCKED.
-- Deliveries acts as the durable queue, a row is considered queued work until successfully delivered.
CREATE TABLE deliveries (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID        NOT NULL REFERENCES events(id),
    endpoint_id     UUID        NOT NULL REFERENCES endpoints(id),
    status          TEXT        NOT NULL DEFAULT 'pending',
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX ON deliveries (status, next_attempt_at) WHERE status = 'pending';

-- Immutable audit log of every delivery attempt.
-- Stores HTTP responses, network errors, and timing data.
CREATE TABLE attempts (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id   UUID        NOT NULL REFERENCES deliveries(id),
    status_code   INT,
    response_body TEXT,
    error         TEXT,
    duration_ms   INT         NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE attempts;
DROP TABLE deliveries;
DROP TABLE events;
DROP TABLE endpoints;
