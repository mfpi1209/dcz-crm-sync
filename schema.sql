-- DataCrazy CRM Sync - Schema PostgreSQL
-- Executado automaticamente pelo sync.py na primeira execução

CREATE TABLE IF NOT EXISTS sync_state (
    entity_type  TEXT PRIMARY KEY,
    last_sync_at TIMESTAMPTZ,
    last_full_sync_at TIMESTAMPTZ,
    run_count    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS leads (
    id         TEXT PRIMARY KEY,
    data       JSONB NOT NULL,
    data_hash  TEXT NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS businesses (
    id         TEXT PRIMARY KEY,
    data       JSONB NOT NULL,
    data_hash  TEXT NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS pipelines (
    id        TEXT PRIMARY KEY,
    data      JSONB NOT NULL,
    synced_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_stages (
    id          TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    data        JSONB NOT NULL,
    synced_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS tags (
    id        TEXT PRIMARY KEY,
    data      JSONB NOT NULL,
    synced_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leads_synced ON leads(synced_at);
CREATE INDEX IF NOT EXISTS idx_biz_synced ON businesses(synced_at);
CREATE INDEX IF NOT EXISTS idx_leads_gin ON leads USING GIN(data);
CREATE INDEX IF NOT EXISTS idx_biz_gin ON businesses USING GIN(data);
