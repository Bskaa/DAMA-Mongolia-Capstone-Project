-- Uniqueness (DAMA: uniqueness) of the retention mart grain.
select cohort_month, months_since_first_session
from {{ ref('mart_player_retention') }}
group by cohort_month, months_since_first_session
having count(*) > 1
