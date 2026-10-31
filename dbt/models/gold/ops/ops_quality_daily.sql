-- One row per day: the last data-quality run of that day (pipeline health trend).
{% if monitoring_ready('data_quality_runs') | trim == 'yes' %}
select distinct on (check_day)
    check_day,
    checked_at,
    status,
    passed_count,
    warning_count,
    failed_count
from (
    select
        {{ reporting_date('checked_at') }} as check_day,
        checked_at,
        status,
        passed_count,
        warning_count,
        failed_count
    from {{ source('monitoring', 'data_quality_runs') }}
) as runs
order by check_day, checked_at desc
{% else %}
select
    null::date as check_day, null::timestamptz as checked_at, null::text as status,
    null::integer as passed_count, null::integer as warning_count, null::integer as failed_count
where false
{% endif %}
