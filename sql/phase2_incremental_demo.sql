INSERT INTO games (
    game_id,
    game_name,
    developer,
    publisher,
    release_date,
    genre,
    price,
    created_at,
    updated_at
)
VALUES (
    9000001,
    'Meltano Incremental Demo',
    'Initial Demo Developer',
    'Capstone Studio',
    CURRENT_DATE,
    'Strategy',
    9.99,
    clock_timestamp(),
    clock_timestamp()
)
ON CONFLICT (game_id) DO UPDATE
SET game_name = EXCLUDED.game_name,
    developer = EXCLUDED.developer,
    publisher = EXCLUDED.publisher,
    release_date = EXCLUDED.release_date,
    genre = EXCLUDED.genre,
    price = EXCLUDED.price,
    updated_at = clock_timestamp();

INSERT INTO game_platforms (game_id, platform_id, created_at, updated_at)
VALUES (9000001, 1, clock_timestamp(), clock_timestamp())
ON CONFLICT (game_id, platform_id) DO UPDATE
SET updated_at = clock_timestamp();
