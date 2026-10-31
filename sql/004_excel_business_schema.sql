CREATE SCHEMA IF NOT EXISTS bronze;

-- Bronze keeps what the business workbooks contain. Cross-source integrity
-- (campaign/target game_id -> games) is enforced by the loader and by dbt
-- relationship tests in Silver, NOT by a foreign key here: a Bronze FK onto a
-- Meltano-managed table can block Meltano reloads of bronze.games.

-- Current snapshot tables (replaced on every changed workbook load).
CREATE TABLE IF NOT EXISTS bronze.raw_marketing_campaigns (
    campaign_id BIGINT PRIMARY KEY,
    game_id BIGINT NOT NULL,
    campaign TEXT NOT NULL,
    budget NUMERIC(12, 2) NOT NULL CHECK (budget >= 0),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (end_date >= start_date)
);

CREATE TABLE IF NOT EXISTS bronze.raw_game_targets (
    game_id BIGINT PRIMARY KEY,
    target_revenue NUMERIC(12, 2) NOT NULL CHECK (target_revenue >= 0),
    target_players BIGINT NOT NULL CHECK (target_players >= 0),
    target_sessions BIGINT NOT NULL CHECK (target_sessions >= 0),
    target_rating NUMERIC(4, 2) NOT NULL CHECK (target_rating BETWEEN 0 AND 5),
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS bronze.raw_campaign_budget (
    campaign_id BIGINT PRIMARY KEY
        REFERENCES bronze.raw_marketing_campaigns (campaign_id),
    budget NUMERIC(12, 2) NOT NULL CHECK (budget >= 0),
    actual_spend NUMERIC(12, 2) NOT NULL CHECK (actual_spend >= 0),
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Latest hash per workbook; used to skip unchanged files.
CREATE TABLE IF NOT EXISTS bronze.business_file_loads (
    file_name TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Append-only audit trail: one row per workbook per load ...
CREATE TABLE IF NOT EXISTS bronze.business_load_log (
    load_id UUID NOT NULL,
    file_name TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    loaded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (load_id, file_name)
);

-- ... and every business record exactly as it was loaded, so earlier targets
-- and budgets can be audited after a workbook is edited.
CREATE TABLE IF NOT EXISTS bronze.business_record_history (
    load_id UUID NOT NULL,
    file_name TEXT NOT NULL,
    record_key BIGINT NOT NULL,
    record JSONB NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (load_id, file_name, record_key)
);

CREATE INDEX IF NOT EXISTS idx_raw_marketing_campaigns_game_dates
    ON bronze.raw_marketing_campaigns (game_id, start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_business_record_history_key
    ON bronze.business_record_history (file_name, record_key, loaded_at);

-- Migration for databases created before the cross-source FKs were removed.
ALTER TABLE bronze.raw_marketing_campaigns
    DROP CONSTRAINT IF EXISTS raw_marketing_campaigns_game_id_fkey;
ALTER TABLE bronze.raw_game_targets
    DROP CONSTRAINT IF EXISTS raw_game_targets_game_id_fkey;
