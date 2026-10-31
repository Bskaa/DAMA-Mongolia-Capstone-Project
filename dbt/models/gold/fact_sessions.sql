{{
    config(
        materialized='incremental',
        unique_key='session_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

select
    sessions.session_id,
    {{ date_key(reporting_date('sessions.started_at')) }} as date_key,
    {{ reporting_date('sessions.started_at') }} as date_day,
    sessions.started_at,
    sessions.ended_at,
    sessions.player_id,
    sessions.game_id,
    sessions.platform_id,
    sessions.duration_minutes,
    extract(epoch from (sessions.ended_at - sessions.started_at))::bigint
        as elapsed_seconds,
    sessions.updated_at
from {{ ref('stg_sessions') }} as sessions
{% if is_incremental() %}
    {%- set existing_columns = adapter.get_columns_in_relation(this) | map(attribute='name') | list -%}
    {%- if 'updated_at' in existing_columns %}
where sessions.updated_at > (
    select coalesce(max(updated_at), '1900-01-01'::timestamptz)
        - interval '{{ var("incremental_lookback_days", 3) }} days'
    from {{ this }}
)
    {%- endif %}
{% endif %}
