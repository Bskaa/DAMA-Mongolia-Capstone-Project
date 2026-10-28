CREATE TABLE IF NOT EXISTS players (
    player_id BIGINT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    country CHAR(2) NOT NULL,
    birth_year SMALLINT NOT NULL CHECK (birth_year BETWEEN 1900 AND 2020),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS platforms (
    platform_id SMALLINT PRIMARY KEY,
    platform_name TEXT NOT NULL UNIQUE,
    manufacturer TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS games (
    game_id BIGINT PRIMARY KEY,
    game_name TEXT NOT NULL,
    developer TEXT NOT NULL,
    publisher TEXT NOT NULL,
    release_date DATE NOT NULL,
    genre TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS game_platforms (
    game_id BIGINT NOT NULL REFERENCES games (game_id) ON DELETE CASCADE,
    platform_id SMALLINT NOT NULL REFERENCES platforms (platform_id),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (updated_at >= created_at),
    PRIMARY KEY (game_id, platform_id)
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id BIGINT PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players (player_id),
    game_id BIGINT NOT NULL REFERENCES games (game_id),
    platform_id SMALLINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL,
    duration_minutes SMALLINT NOT NULL CHECK (duration_minutes BETWEEN 1 AND 1440),
    updated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (game_id, platform_id)
        REFERENCES game_platforms (game_id, platform_id),
    CHECK (ended_at > started_at)
);

CREATE TABLE IF NOT EXISTS purchases (
    purchase_id BIGINT PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players (player_id),
    game_id BIGINT NOT NULL REFERENCES games (game_id),
    purchase_date TIMESTAMPTZ NOT NULL,
    amount NUMERIC(10, 2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    payment_method TEXT NOT NULL
        CHECK (payment_method IN ('card', 'wallet', 'gift_card', 'mobile'))
);

CREATE TABLE IF NOT EXISTS achievements (
    achievement_id BIGINT PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players (player_id),
    game_id BIGINT NOT NULL REFERENCES games (game_id),
    achievement_name TEXT NOT NULL,
    achieved_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (player_id, game_id, achievement_name)
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id BIGINT PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players (player_id),
    game_id BIGINT NOT NULL REFERENCES games (game_id),
    rating SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_date TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (player_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_game_platforms_platform_id
    ON game_platforms (platform_id);
CREATE INDEX IF NOT EXISTS idx_sessions_player_started_at
    ON sessions (player_id, started_at);
CREATE INDEX IF NOT EXISTS idx_sessions_game_started_at
    ON sessions (game_id, started_at);
CREATE INDEX IF NOT EXISTS idx_sessions_platform_started_at
    ON sessions (platform_id, started_at);
CREATE INDEX IF NOT EXISTS idx_purchases_purchase_date
    ON purchases (purchase_date);
CREATE INDEX IF NOT EXISTS idx_purchases_game_purchase_date
    ON purchases (game_id, purchase_date);
CREATE INDEX IF NOT EXISTS idx_achievements_player_achieved_at
    ON achievements (player_id, achieved_at);
CREATE INDEX IF NOT EXISTS idx_reviews_game_review_date
    ON reviews (game_id, review_date);
CREATE INDEX IF NOT EXISTS idx_players_updated_at
    ON players (updated_at);
CREATE INDEX IF NOT EXISTS idx_platforms_updated_at
    ON platforms (updated_at);
CREATE INDEX IF NOT EXISTS idx_games_updated_at
    ON games (updated_at);
CREATE INDEX IF NOT EXISTS idx_game_platforms_updated_at
    ON game_platforms (updated_at);
CREATE INDEX IF NOT EXISTS idx_sessions_updated_at
    ON sessions (updated_at);
CREATE INDEX IF NOT EXISTS idx_purchases_updated_at
    ON purchases (updated_at);
CREATE INDEX IF NOT EXISTS idx_achievements_updated_at
    ON achievements (updated_at);
CREATE INDEX IF NOT EXISTS idx_reviews_updated_at
    ON reviews (updated_at);
