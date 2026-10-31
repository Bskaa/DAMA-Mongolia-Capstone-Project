-- Latest health of every monitored relation (rows, columns, freshness, volume).
{% if monitoring_ready('data_quality_snapshots') | trim == 'yes'
      and monitoring_ready('data_quality_runs') | trim == 'yes' %}
with latest_run as (
    select run_id, checked_at
    from {{ source('monitoring', 'data_quality_runs') }}
    order by checked_at desc
    limit 1
)
select
    snapshots.schema_name || '.' || snapshots.table_name as relation,
    snapshots.schema_name,
    snapshots.table_name,
    snapshots.row_count,
    snapshots.column_count,
    snapshots.last_ingested_at,
    snapshots.freshness_status,
    snapshots.volume_status,
    snapshots.volume_change_percent,
    latest_run.checked_at
from {{ source('monitoring', 'data_quality_snapshots') }} as snapshots
join latest_run using (run_id)
{% else %}
select
    null::text as relation, null::text as schema_name, null::text as table_name,
    null::bigint as row_count, null::integer as column_count, null::timestamptz as last_ingested_at,
    null::text as freshness_status, null::text as volume_status, null::numeric as volume_change_percent,
    null::timestamptz as checked_at
where false
{% endif %}
