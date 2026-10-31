{#-
    Least-privilege access for BI consumers.

    `analytics_reader` is a NOLOGIN group role. dbt grants it SELECT on every
    Gold model (see `+grants` in dbt_project.yml); login users such as
    `dashboard_user` are created separately (sql/006_access_roles.sql) and are
    made members of the group. Nothing in Bronze, Silver or snapshots is
    readable by that role, so PII kept in Silver never reaches the dashboard.
-#}
{% macro ensure_reader_role() -%}
    do $$
    begin
        if not exists (select 1 from pg_roles where rolname = 'analytics_reader') then
            create role analytics_reader nologin;
        end if;
    end
    $$;
{%- endmacro %}

{% macro grant_reader_schema_usage() -%}
    {%- if execute -%}
        do $$
        begin
            if exists (select 1 from pg_namespace where nspname = 'gold') then
                grant usage on schema gold to analytics_reader;
            end if;
        end
        $$;
    {%- endif -%}
{%- endmacro %}
