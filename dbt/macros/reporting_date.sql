{#-
    Calendar day of a timestamptz in the reporting time zone (var
    `reporting_timezone`, default UTC). Using an explicit zone keeps day
    boundaries identical no matter which session time zone runs dbt.
-#}
{% macro reporting_date(column_expression) -%}
    ({{ column_expression }} at time zone '{{ var("reporting_timezone", "UTC") }}')::date
{%- endmacro %}

{% macro date_key(date_expression) -%}
    to_char({{ date_expression }}, 'YYYYMMDD')::integer
{%- endmacro %}
