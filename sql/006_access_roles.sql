-- Least-privilege access roles for the analytics database (DAMA: Data Security).
--
--   analytics_reader  NOLOGIN group  SELECT on Gold only (granted by dbt +grants)
--   dashboard_user    LOGIN          member of analytics_reader; used by Streamlit
--
-- Usage (password comes from DASHBOARD_DB_PASSWORD in .env, never from this file):
--   docker compose exec -T postgres-analytics psql -U "$POSTGRES_USER" -d "$ANALYTICS_DB" \
--       -v dashboard_password="$DASHBOARD_DB_PASSWORD" -f /sql/006_access_roles.sql
-- On a brand-new analytics volume docker/postgres-analytics/10-access-roles.sh runs it automatically.

\set ON_ERROR_STOP on

\o /dev/null
SELECT set_config('access.dashboard_password', :'dashboard_password', false);
\o

DO $$
DECLARE
    dashboard_password text := current_setting('access.dashboard_password');
BEGIN
    IF coalesce(dashboard_password, '') = '' THEN
        RAISE EXCEPTION 'dashboard_password is empty; set DASHBOARD_DB_PASSWORD in .env';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analytics_reader') THEN
        CREATE ROLE analytics_reader NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_user') THEN
        EXECUTE format(
            'CREATE ROLE dashboard_user LOGIN PASSWORD %L IN ROLE analytics_reader',
            dashboard_password
        );
    ELSE
        EXECUTE format('ALTER ROLE dashboard_user WITH LOGIN PASSWORD %L', dashboard_password);
        GRANT analytics_reader TO dashboard_user;
    END IF;

    -- Defence in depth: the dashboard role is read-only even if someone grants more later.
    ALTER ROLE dashboard_user SET default_transaction_read_only = on;
    ALTER ROLE dashboard_user SET statement_timeout = '30s';
END
$$;

\o /dev/null
SELECT set_config('access.dashboard_password', '', false);
\o

-- Nobody but the owner may use Bronze, Silver or snapshots (they hold PII).
REVOKE ALL ON SCHEMA public FROM PUBLIC;
DO $$
DECLARE
    schema_name text;
BEGIN
    FOREACH schema_name IN ARRAY ARRAY['bronze', 'silver', 'snapshots', 'monitoring'] LOOP
        IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = schema_name) THEN
            EXECUTE format('REVOKE ALL ON SCHEMA %I FROM PUBLIC', schema_name);
        END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'gold') THEN
        GRANT USAGE ON SCHEMA gold TO analytics_reader;
        GRANT SELECT ON ALL TABLES IN SCHEMA gold TO analytics_reader;
    END IF;
END
$$;

DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO analytics_reader', current_database());
END
$$;
