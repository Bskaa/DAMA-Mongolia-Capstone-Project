-- SCD Type 2 view of player attributes built from the snap_players snapshot.
select
    dbt_scd_id as player_version_key,
    player_id,
    country,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_players') }}
