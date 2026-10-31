-- Current player attributes. Direct identifiers (username) and exact birth
-- year stay in Silver; Gold exposes only an age band (data minimisation).
select
    player_id,
    country,
    case
        when birth_year is null then 'unknown'
        when extract(year from current_date)::integer - birth_year < 18 then 'under_18'
        when extract(year from current_date)::integer - birth_year < 25 then '18_24'
        when extract(year from current_date)::integer - birth_year < 35 then '25_34'
        when extract(year from current_date)::integer - birth_year < 45 then '35_44'
        when extract(year from current_date)::integer - birth_year < 55 then '45_54'
        else '55_plus'
    end as age_band,
    created_at,
    updated_at
from {{ ref('stg_players') }}
