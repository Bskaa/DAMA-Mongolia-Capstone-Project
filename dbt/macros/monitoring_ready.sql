{#-
    'yes' when monitoring.<identifier> exists, otherwise 'no'.

    The monitoring tables are created by the monitoring scripts, which run after
    dbt in the daily DAG. On a brand-new database they do not exist yet, so the
    gold.ops_* models fall back to an empty, correctly typed result instead of
    failing the whole build. At parse time (execute = false) the answer is 'yes'
    so dbt still records the source dependency for lineage.
-#}
{% macro monitoring_ready(identifier) -%}
    {%- if execute -%}
        {%- set relation = adapter.get_relation(
            database=target.database, schema='monitoring', identifier=identifier) -%}
        {{- 'yes' if relation is not none else 'no' -}}
    {%- else -%}
        yes
    {%- endif -%}
{%- endmacro %}
