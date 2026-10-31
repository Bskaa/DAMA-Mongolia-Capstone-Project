-- Airflow task outcomes and durations per day (final try of each task run).
{% if monitoring_ready('task_runs') | trim == 'yes' %}
with final_tries as (
    select distinct on (dag_id, task_id, run_id) *
    from {{ source('monitoring', 'task_runs') }}
    order by dag_id, task_id, run_id, try_number desc
)
select
    {{ reporting_date('ended_at') }} as run_day,
    dag_id,
    task_id,
    count(*) as runs,
    count(*) filter (where state = 'success') as successes,
    count(*) filter (where state = 'failed') as failures,
    round(avg(duration_seconds), 1) as avg_seconds,
    max(duration_seconds) as max_seconds
from final_tries
group by 1, 2, 3
{% else %}
select
    null::date as run_day, null::text as dag_id, null::text as task_id, null::bigint as runs,
    null::bigint as successes, null::bigint as failures, null::numeric as avg_seconds,
    null::numeric as max_seconds
where false
{% endif %}
