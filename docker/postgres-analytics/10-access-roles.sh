#!/bin/sh
# Runs once when the analytics volume is first initialised (docker-entrypoint-initdb.d).
# Creates the least-privilege dashboard login from DASHBOARD_DB_PASSWORD.
set -eu
if [ -z "${DASHBOARD_DB_PASSWORD:-}" ]; then
    echo "DASHBOARD_DB_PASSWORD is not set; skipping dashboard_user creation." >&2
    exit 0
fi
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v dashboard_password="$DASHBOARD_DB_PASSWORD" -f /sql/006_access_roles.sql
