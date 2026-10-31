-- dbt node outcomes per day and resource type (tests passing, warning, failing over time).
{% if monitoring_ready('dbt_node_results') | trim == 'yes' %}
with latest_per_day as (
    -- the last invocation of each day represents that day
    select distinct on ({{ reporting_date('generated_at') }})
        {{ reporting_date('generated_at') }} as run_day,
        invocation_id
    from {{ source('monitoring', 'dbt_node_results') }}
    order by {{ reporting_date('generated_at') }}, generated_at desc
)
select
    latest_per_day.run_day,
    results.resource_type,
    count(*) as nodes,
    count(*) filter (where results.status in ('success', 'pass')) as passed,
    count(*) filter (where results.status = 'warn') as warned,
    count(*) filter (where results.status in ('error', 'fail')) as failed,
    count(*) filter (where results.status = 'skipped') as skipped,
    coalesce(sum(results.failures) filter (where results.status in ('warn', 'fail')), 0) as failing_rows,
    round(sum(results.execution_seconds), 1) as execution_seconds
from {{ source('monitoring', 'dbt_node_results') }} as results
join latest_per_day using (invocation_id)
group by latest_per_day.run_day, results.resource_type
{% else %}
select
    null::date as run_day, null::text as resource_type, null::bigint as nodes, null::bigint as passed,
    null::bigint as warned, null::bigint as failed, null::bigint as skipped,
    null::numeric as failing_rows, null::numeric as execution_seconds
where false
{% endif %}
