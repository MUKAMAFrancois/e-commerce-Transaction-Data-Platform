#!/bin/bash
# Runs once, on first boot of an empty postgres data volume.
# Creates a second database so Airflow's ~40 metadata tables live apart from
# the analytics schema that Metabase browses.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE ${AIRFLOW_DB};
    GRANT ALL PRIVILEGES ON DATABASE ${AIRFLOW_DB} TO ${POSTGRES_USER};
EOSQL
