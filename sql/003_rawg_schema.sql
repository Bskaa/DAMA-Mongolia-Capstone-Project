CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.rawg_games (
    rawg_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT,
    released DATE,
    updated TIMESTAMPTZ,
    rating NUMERIC(6, 3),
    rating_top INTEGER,
    ratings_count INTEGER,
    metacritic INTEGER,
    playtime INTEGER,
    background_image TEXT,
    raw_payload JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.rawg_platforms (
    rawg_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT,
    games_count INTEGER,
    image_background TEXT,
    image TEXT,
    year_start INTEGER,
    year_end INTEGER,
    raw_payload JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.rawg_genres (
    rawg_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT,
    games_count INTEGER,
    image_background TEXT,
    raw_payload JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.rawg_publishers (
    rawg_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT,
    games_count INTEGER,
    image_background TEXT,
    raw_payload JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.rawg_developers (
    rawg_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT,
    games_count INTEGER,
    image_background TEXT,
    raw_payload JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rawg_games_released
    ON raw.rawg_games (released);
CREATE INDEX IF NOT EXISTS idx_rawg_games_updated
    ON raw.rawg_games (updated);
CREATE INDEX IF NOT EXISTS idx_rawg_games_name
    ON raw.rawg_games (name);
