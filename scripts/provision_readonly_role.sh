#!/usr/bin/env bash
# Idempotently provision the dedicated read-only PostgreSQL 16 role `sro_readonly`
# used by the analytical Streamlit dashboard.

set -euo pipefail

# Non-destructively load missing variables from .env (shell-exported values take precedence).
if [[ -f ".env" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
        # Skip empty lines and comments
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        # Strip optional leading `export `
        line="${line#export }"
        if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
            key="${BASH_REMATCH[1]}"
            val="${BASH_REMATCH[2]}"
            # Strip trailing CR (Windows line endings)
            val="${val%$'\r'}"
            # Strip matching surrounding single or double quotes
            if [[ "$val" =~ ^\"(.*)\"$ ]]; then
                val="${BASH_REMATCH[1]}"
            elif [[ "$val" =~ ^\'(.*)\'$ ]]; then
                val="${BASH_REMATCH[1]}"
            fi
            if [[ -z "${!key+x}" ]]; then
                export "$key=$val"
            fi
        fi
    done < ".env"
fi

POSTGRES_USER="${POSTGRES_USER:-sro_user}"
POSTGRES_DB="${POSTGRES_DB:-sro_rankings}"

if [[ -z "${DASHBOARD_DB_PASSWORD:-}" ]]; then
    echo "ERROR: DASHBOARD_DB_PASSWORD must be set in the environment or .env" >&2
    exit 1
fi

docker compose exec -T postgres psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -v ON_ERROR_STOP=1 \
    -v readonly_pass="$DASHBOARD_DB_PASSWORD" \
    -v db_name="$POSTGRES_DB" \
    -v owner_role="$POSTGRES_USER" <<'SQL'
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sro_readonly')
    THEN format(
        'ALTER ROLE sro_readonly WITH NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN CONNECTION LIMIT 5 PASSWORD %L',
        :'readonly_pass'
    )
    ELSE format(
        'CREATE ROLE sro_readonly WITH NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN CONNECTION LIMIT 5 PASSWORD %L',
        :'readonly_pass'
    )
END \gexec

SELECT format('GRANT CONNECT ON DATABASE %I TO sro_readonly', :'db_name') \gexec

GRANT USAGE ON SCHEMA public TO sro_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO sro_readonly;

SELECT format(
    'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT SELECT ON TABLES TO sro_readonly',
    :'owner_role'
) \gexec

REVOKE CREATE ON SCHEMA public FROM sro_readonly;

ALTER ROLE sro_readonly SET default_transaction_read_only = on;
ALTER ROLE sro_readonly SET statement_timeout = '15000';
ALTER ROLE sro_readonly SET lock_timeout = '3000';
ALTER ROLE sro_readonly SET work_mem = '16MB';
SQL

echo "Read-only role 'sro_readonly' provisioned successfully on database '${POSTGRES_DB}'."
