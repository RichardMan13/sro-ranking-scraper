# scripts

## Overview
Operational shell scripts supporting infrastructure and database administration for `sro-ranking-scraper`. Currently provides idempotent provisioning of the dedicated read-only PostgreSQL 16 role (`sro_readonly`) used by the analytical Streamlit dashboard `[provision_readonly_role.sh:L1-L3]`.

## Interface
- `provision_readonly_role.sh` `[provision_readonly_role.sh:L1-L77]`: Executable Bash script (`#!/usr/bin/env bash`) that connects to the `postgres` Docker Compose service via `docker compose exec -T postgres psql` and provisions the `sro_readonly` role `[provision_readonly_role.sh:L1,L40-L46]`.
- Environment variables (read from the current environment, falling back to unset keys in `./.env` `[provision_readonly_role.sh:L8-L30]`):
  - `DASHBOARD_DB_PASSWORD` (required): Password assigned to `sro_readonly` (`-v readonly_pass`) `[provision_readonly_role.sh:L35-L38,L44]`.
  - `POSTGRES_USER` (optional, default `sro_user`): PostgreSQL role used for the `psql` session (`-U`) and targeted in `ALTER DEFAULT PRIVILEGES FOR ROLE` (`-v owner_role`) `[provision_readonly_role.sh:L32,L41,L46,L64-L67]`.
  - `POSTGRES_DB` (optional, default `sro_rankings`): Target PostgreSQL database (`-d`, `-v db_name`) granted `CONNECT` `[provision_readonly_role.sh:L33,L42,L45,L59]`.

## Invariants
- Fail-fast execution: Runs with `set -euo pipefail` `[provision_readonly_role.sh:L5]` and passes `-v ON_ERROR_STOP=1` to `psql` `[provision_readonly_role.sh:L43]`.
- Non-destructive `.env` loading: Pre-existing shell-exported environment variables always take precedence over `.env` values (`[[ -z "${!key+x}" ]]`); parser skips blank/comment lines, strips optional `export ` prefixes, strips trailing `\r` (CRLF), and unquotes matching single or double quotes `[provision_readonly_role.sh:L8-L30]`.
- Required secret guard: Aborts with exit code `1` and an error on `stderr` if `DASHBOARD_DB_PASSWORD` is unset or empty `[provision_readonly_role.sh:L35-L38]`.
- Idempotent role creation/update: Queries `pg_roles` for `rolname = 'sro_readonly'` and executes `ALTER ROLE` when present or `CREATE ROLE` when absent via `\gexec` `[provision_readonly_role.sh:L47-L57]`.
- Safe dynamic SQL formatting: Uses PostgreSQL `format()` with `%L` for `readonly_pass` and `%I` for `db_name` and `owner_role` before `\gexec` `[provision_readonly_role.sh:L49-L67]`.
- Strict least-privilege & resource caps on `sro_readonly`:
  - Role attributes: `NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN CONNECTION LIMIT 5` `[provision_readonly_role.sh:L50,L54]`.
  - Grants & revocations: `GRANT CONNECT ON DATABASE`, `GRANT USAGE ON SCHEMA public`, `GRANT SELECT ON ALL TABLES IN SCHEMA public`, `ALTER DEFAULT PRIVILEGES FOR ROLE <owner_role> IN SCHEMA public GRANT SELECT ON TABLES`, and `REVOKE CREATE ON SCHEMA public` `[provision_readonly_role.sh:L59-L69]`.
  - Role-level session defaults: `default_transaction_read_only = on`, `statement_timeout = '15000'`, `lock_timeout = '3000'`, and `work_mem = '16MB'` `[provision_readonly_role.sh:L71-L74]`.

## Side Effects
- Reads `./.env` from the current working directory (when present) and exports unset variables into the script process `[provision_readonly_role.sh:L8-L30]`.
- Invokes `docker compose exec -T postgres psql` against the running `postgres` container `[provision_readonly_role.sh:L40-L46]`.
- Mutates PostgreSQL cluster and database state: creates or alters role `sro_readonly` (updating its password and session parameters), grants `CONNECT`/`USAGE`/`SELECT` and default `SELECT` privileges for `POSTGRES_USER`, and revokes `CREATE` on schema `public` `[provision_readonly_role.sh:L47-L75]`.
- Writes a confirmation message to `stdout` on success `[provision_readonly_role.sh:L77]` or an error message to `stderr` when `DASHBOARD_DB_PASSWORD` is missing `[provision_readonly_role.sh:L36]`.

## Verification
- Run: `uv run --no-sync pytest`
