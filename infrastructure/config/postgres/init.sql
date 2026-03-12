-- Crucible schema initialisation
-- Runs automatically on first container startup via docker-entrypoint-initdb.d.

-- Distributed load-test synchronization.
-- One row per active run; workers check in and poll for the global START signal.
CREATE TABLE IF NOT EXISTS waiting_room (
    run_id      TEXT        PRIMARY KEY,
    ready_count INTEGER     NOT NULL DEFAULT 0,
    target_count INTEGER    NOT NULL,
    signal      TEXT        NOT NULL DEFAULT 'WAIT',  -- WAIT | START | ABORT
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
