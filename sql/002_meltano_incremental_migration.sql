ALTER TABLE game_platforms
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE game_platforms
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
UPDATE game_platforms gp
SET created_at = g.created_at,
    updated_at = g.updated_at
FROM games g
WHERE g.game_id = gp.game_id
  AND (gp.created_at IS NULL OR gp.updated_at IS NULL);
ALTER TABLE game_platforms
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'game_platforms_updated_at_check'
          AND conrelid = 'game_platforms'::regclass
    ) THEN
        ALTER TABLE game_platforms
            ADD CONSTRAINT game_platforms_updated_at_check
            CHECK (updated_at >= created_at);
    END IF;
END
$$;

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
UPDATE sessions
SET updated_at = started_at
WHERE updated_at IS NULL;
ALTER TABLE sessions
    ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
UPDATE purchases
SET updated_at = purchase_date
WHERE updated_at IS NULL;
ALTER TABLE purchases
    ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE achievements
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
UPDATE achievements
SET updated_at = achieved_at
WHERE updated_at IS NULL;
ALTER TABLE achievements
    ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE reviews
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
UPDATE reviews
SET updated_at = review_date
WHERE updated_at IS NULL;
ALTER TABLE reviews
    ALTER COLUMN updated_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_game_platforms_updated_at
    ON game_platforms (updated_at);
CREATE INDEX IF NOT EXISTS idx_players_updated_at
    ON players (updated_at);
CREATE INDEX IF NOT EXISTS idx_platforms_updated_at
    ON platforms (updated_at);
CREATE INDEX IF NOT EXISTS idx_games_updated_at
    ON games (updated_at);
CREATE INDEX IF NOT EXISTS idx_sessions_updated_at
    ON sessions (updated_at);
CREATE INDEX IF NOT EXISTS idx_purchases_updated_at
    ON purchases (updated_at);
CREATE INDEX IF NOT EXISTS idx_achievements_updated_at
    ON achievements (updated_at);
CREATE INDEX IF NOT EXISTS idx_reviews_updated_at
    ON reviews (updated_at);
