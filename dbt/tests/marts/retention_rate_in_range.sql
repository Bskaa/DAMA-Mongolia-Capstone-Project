-- Validity: a retention rate is a share between 0 and 1, and month 0 is always 100%.
select cohort_month, months_since_first_session, retention_rate
from {{ ref('mart_player_retention') }}
where retention_rate < 0
    or retention_rate > 1
    or (months_since_first_session = 0 and retention_rate <> 1)
