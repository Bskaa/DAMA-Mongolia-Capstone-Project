-- SCD Type 2 view of game attributes (price, genre) from the snap_games snapshot.
select
    dbt_scd_id as game_version_key,
    game_id,
    game_name,
    genre,
    price,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_games') }}
