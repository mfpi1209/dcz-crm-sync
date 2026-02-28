-- eduit. — Schema PostgreSQL
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

CREATE TABLE IF NOT EXISTS turmas (
    id         SERIAL PRIMARY KEY,
    nivel      TEXT NOT NULL,
    nome       TEXT NOT NULL,
    dt_inicio  DATE NOT NULL,
    dt_fim     DATE NOT NULL,
    ano        INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(nivel, nome)
);

CREATE TABLE IF NOT EXISTS ciclos (
    id         SERIAL PRIMARY KEY,
    nivel      TEXT NOT NULL,
    nome       TEXT NOT NULL,
    dt_inicio  DATE NOT NULL,
    dt_fim     DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(nivel, nome)
);

CREATE TABLE IF NOT EXISTS schedules (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    cron_days TEXT NOT NULL DEFAULT '*',
    cron_hour INTEGER NOT NULL DEFAULT 2,
    cron_minute INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN DEFAULT TRUE,
    last_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app_users (
    id         SERIAL PRIMARY KEY,
    username   TEXT NOT NULL UNIQUE,
    pw_hash    TEXT NOT NULL,
    role       TEXT NOT NULL DEFAULT 'viewer',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_permissions (
    user_id    INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    page       TEXT NOT NULL,
    PRIMARY KEY (user_id, page)
);
