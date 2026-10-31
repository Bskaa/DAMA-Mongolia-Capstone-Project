-- CI only: emulate the Meltano sync by copying the generated source tables
-- (public schema) into Bronze with the Singer metadata columns dbt relies on.
CREATE SCHEMA IF NOT EXISTS bronze;
DO $$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'players', 'platforms', 'games', 'game_platforms',
        'sessions', 'purchases', 'achievements', 'reviews'
    ] LOOP
        EXECUTE format('DROP TABLE IF EXISTS bronze.%I CASCADE', table_name);
        EXECUTE format(
            'CREATE TABLE bronze.%1$I AS
             SELECT source.*,
                    now() AS _sdc_extracted_at,
                    now() AS _sdc_received_at,
                    now() AS _sdc_batched_at,
                    NULL::timestamptz AS _sdc_deleted_at,
                    NULL::bigint AS _sdc_sequence,
                    NULL::bigint AS _sdc_table_version
             FROM public.%1$I AS source',
            table_name
        );
    END LOOP;
END
$$;
