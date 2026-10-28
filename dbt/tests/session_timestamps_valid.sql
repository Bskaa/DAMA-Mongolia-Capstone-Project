select session_id, started_at, ended_at, duration_minutes
from {{ ref('stg_sessions') }}
where ended_at <= started_at
    or duration_minutes < 1
    or duration_minutes > 1440
