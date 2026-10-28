CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.raw_marketing_campaigns (
    campaign_id BIGINT PRIMARY KEY,
    game_id BIGINT NOT NULL REFERENCES raw.games (game_id),
    campaign TEXT NOT NULL,
    budget NUMERIC(12, 2) NOT NULL CHECK (budget >= 0),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (end_date >= start_date)
);

CREATE TABLE IF NOT EXISTS raw.raw_game_targets (
    game_id BIGINT PRIMARY KEY REFERENCES raw.games (game_id),
    target_revenue NUMERIC(12, 2) NOT NULL CHECK (target_revenue >= 0),
    target_players BIGINT NOT NULL CHECK (target_players >= 0),
    target_sessions BIGINT NOT NULL CHECK (target_sessions >= 0),
    target_rating NUMERIC(4, 2) NOT NULL CHECK (target_rating BETWEEN 0 AND 5),
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS raw.raw_campaign_budget (
    campaign_id BIGINT PRIMARY KEY
        REFERENCES raw.raw_marketing_campaigns (campaign_id),
    budget NUMERIC(12, 2) NOT NULL CHECK (budget >= 0),
    actual_spend NUMERIC(12, 2) NOT NULL CHECK (actual_spend >= 0),
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS raw.business_file_loads (
    file_name TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_raw_marketing_campaigns_game_dates
    ON raw.raw_marketing_campaigns (game_id, start_date, end_date);
