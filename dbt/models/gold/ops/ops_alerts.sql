-- Alerts of the last 90 days (what fired, how bad, whether the email went out).
{% if monitoring_ready('alert_log') | trim == 'yes' %}
select
    alert_id,
    fired_at,
    alert_date,
    source,
    severity,
    check_name,
    object,
    detail,
    sent
from {{ source('monitoring', 'alert_log') }}
where alert_date >= current_date - 90
{% else %}
select
    null::bigint as alert_id, null::timestamptz as fired_at, null::date as alert_date,
    null::text as source, null::text as severity, null::text as check_name, null::text as object,
    null::text as detail, null::boolean as sent
where false
{% endif %}
