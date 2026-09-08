-- Create logs schema
CREATE SCHEMA IF NOT EXISTS logs;
-- Table to track overall ingestion runs and orchestrations
CREATE TABLE IF NOT EXISTS logs.ingestion_runs (
    run_id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    layer VARCHAR(20) NOT NULL,
    -- 'RAW' or 'ANALYTICS'
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    -- 'RUNNING', 'COMPLETED', 'FAILED'
    error_message TEXT
);
-- Table for Watermarks, Checkpoints and Fallback active/manual state
CREATE TABLE IF NOT EXISTS logs.ingestion_checkpoints (
    checkpoint_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name VARCHAR(100) NOT NULL,
    current_watermark BIGINT NOT NULL DEFAULT 0,
    fallback_watermark BIGINT NOT NULL DEFAULT 0,
    last_id INT NOT NULL DEFAULT 0,
    last_batch_id BIGINT NOT NULL DEFAULT 0,
    layer VARCHAR(20) NOT NULL,
    -- 'RAW' or 'ANALYTICS'
    offset_val INT NOT NULL DEFAULT 0,
    is_override_active BOOLEAN NOT NULL DEFAULT FALSE,
    last_successful_run_id UUID REFERENCES logs.ingestion_runs(run_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_name, layer)
);
ALTER TABLE logs.ingestion_checkpoints ADD COLUMN IF NOT EXISTS last_batch_id BIGINT NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS logs.fallback_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name VARCHAR(100) NOT NULL,
    layer VARCHAR(20) NOT NULL DEFAULT 'RAW',
    -- 'RAW' or 'ANALYTICS'
    start_watermark BIGINT NOT NULL,
    -- Timestamp Unix début
    end_watermark BIGINT NOT NULL,
    -- Timestamp Unix fin
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    -- PENDING, IN_PROGRESS, COMPLETED, FAILED
    records_processed INT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ
);
-- Table to log every batch fetch attempt (success or failure)
CREATE TABLE IF NOT EXISTS logs.batch_logs (
    batch_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES logs.ingestion_runs(run_id),
    table_name VARCHAR(100) NOT NULL,
    layer VARCHAR(20) NOT NULL,
    -- 'RAW' or 'ANALYTICS'
    status VARCHAR(20) NOT NULL,
    -- 'SUCCESS' or 'FAILED'
    cursor_value BIGINT NOT NULL,
    offset_value INT NOT NULL,
    records_count INT NOT NULL DEFAULT 0,
    duration_ms INT NOT NULL DEFAULT 0,
    query_sent TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- Table to audit and alert schema drift
CREATE TABLE IF NOT EXISTS logs.schema_history (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    schema_hash VARCHAR(100) NOT NULL,
    columns_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
    changed_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
    --! j'ai changé le type btw
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    detected_in_run_id UUID REFERENCES logs.ingestion_runs(run_id),
    included_at TIMESTAMPTZ,
    -- popule quand patch dans tables_schema.py
    status VARCHAR(30) NOT NULL DEFAULT 'NEW_COLUMN',
    -- 'NEW_COLUMN', 'BREAKING_CHANGE', 'QUARANTINE'
    action_taken TEXT,
    UNIQUE (table_name, schema_hash)
);
-- Indexes for performance tuning in operational dashboard / API
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_status ON logs.ingestion_runs(status, layer);
CREATE INDEX IF NOT EXISTS idx_batch_logs_run_id ON logs.batch_logs(run_id, layer);
CREATE INDEX IF NOT EXISTS idx_batch_logs_table_created ON logs.batch_logs(table_name, layer, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_schema_history_table ON logs.schema_history(table_name);
CREATE INDEX IF NOT EXISTS idx_fallback_events_status ON logs.fallback_events(status, created_at DESC);