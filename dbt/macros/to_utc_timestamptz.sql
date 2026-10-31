{#-
    Return a timezone-aware (timestamptz) expression for a Bronze timestamp column.

    Meltano's target-postgres can land Singer date-time values as
    `timestamp without time zone` holding UTC wall-clock time. A plain
    `::timestamptz` cast would then silently apply the *session* time zone
    (for example Asia/Ulaanbaatar, +08:00) and shift every event by 8 hours.
    This macro inspects the column type at run time and interprets naive
    timestamps explicitly as UTC; timestamptz columns are passed through.
-#}
{% macro to_utc_timestamptz(relation, column_name) -%}
    {%- set ns = namespace(dtype='') -%}
    {%- if execute -%}
        {%- for column in adapter.get_columns_in_relation(relation) -%}
            {%- if column.name | lower == column_name | lower -%}
                {%- set ns.dtype = column.dtype | lower -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- if ns.dtype in ('timestamp without time zone', 'timestamp') -%}
        ({{ column_name }} at time zone 'UTC')
    {%- else -%}
        {{ column_name }}::timestamptz
    {%- endif -%}
{%- endmacro %}
