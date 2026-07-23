#!/bin/bash
# Exécuté automatiquement par l'image postgres UNE SEULE FOIS, au tout premier
# démarrage (volume postgres-data vide). Crée :
#   - la base air_quality (le warehouse)
#   - un utilisateur de chargement (droits complets, utilisé par load_dwh.py)
#   - un utilisateur en lecture seule (à donner à IA1)
# puis applique le schéma en étoile (sql/create_star_schema.sql).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE ${DWH_DB_NAME};
    CREATE USER ${DWH_DB_USER} WITH PASSWORD '${DWH_DB_PASSWORD}';
    GRANT ALL PRIVILEGES ON DATABASE ${DWH_DB_NAME} TO ${DWH_DB_USER};

    CREATE USER ${READONLY_DB_USER} WITH PASSWORD '${READONLY_DB_PASSWORD}';
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${DWH_DB_NAME}" <<-EOSQL
    GRANT CONNECT ON DATABASE ${DWH_DB_NAME} TO ${READONLY_DB_USER};
    GRANT USAGE ON SCHEMA public TO ${READONLY_DB_USER};
    ALTER DEFAULT PRIVILEGES FOR ROLE ${DWH_DB_USER} IN SCHEMA public
        GRANT SELECT ON TABLES TO ${READONLY_DB_USER};
EOSQL

echo ">>> Application du schéma en étoile (sql/create_star_schema.sql)..."
psql -v ON_ERROR_STOP=1 --username "${DWH_DB_USER}" --dbname "${DWH_DB_NAME}" \
     -f /schema/create_star_schema.sql

# Le futur utilisateur en lecture seule doit aussi voir les tables déjà créées
# ci-dessus (ALTER DEFAULT PRIVILEGES ne couvre que les tables créées APRÈS).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${DWH_DB_NAME}" <<-EOSQL
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${READONLY_DB_USER};
EOSQL

echo ">>> Initialisation air_quality terminée."
