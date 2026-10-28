UPDATE games
SET developer = 'Incremental Update Confirmed',
    updated_at = clock_timestamp()
WHERE game_id = 9000001;
