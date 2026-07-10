-- sql/create_star_schema.sql
--
-- Schéma en étoile pour l'analyse de la qualité de l'air.
-- Compatible PostgreSQL local (sans Docker).
--
-- Créer la base d'abord :
--   sudo -u postgres psql -c "CREATE DATABASE air_quality;"
--   sudo -u postgres psql -c "CREATE USER air_quality_user WITH PASSWORD 'air_quality_pass';"
--   sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE air_quality TO air_quality_user;"
--
-- Puis appliquer ce fichier :
--   sudo -u postgres psql -d air_quality -f sql/create_star_schema.sql

CREATE TABLE IF NOT EXISTS dim_ville (
    ville_id     SERIAL PRIMARY KEY,
    ville        VARCHAR(100) NOT NULL,
    pays         VARCHAR(10)  NOT NULL,
    lat          NUMERIC(9,6) NOT NULL,
    lon          NUMERIC(9,6) NOT NULL,
    UNIQUE (ville, pays)
);

CREATE TABLE IF NOT EXISTS dim_temps (
    temps_id     SERIAL PRIMARY KEY,
    date_valeur  DATE     NOT NULL,
    heure        SMALLINT NOT NULL CHECK (heure BETWEEN 0 AND 23),
    jour_semaine SMALLINT NOT NULL,   -- 1=lundi ... 7=dimanche
    mois         SMALLINT NOT NULL,
    annee        SMALLINT NOT NULL,
    UNIQUE (date_valeur, heure)
);

CREATE TABLE IF NOT EXISTS fact_qualite_air (
    fact_id   BIGSERIAL PRIMARY KEY,
    ville_id  INTEGER NOT NULL REFERENCES dim_ville(ville_id),
    temps_id  INTEGER NOT NULL REFERENCES dim_temps(temps_id),
    aqi       SMALLINT,           -- indice OpenWeather 1 (bon) à 5 (très mauvais)
    co        NUMERIC(10,3),      -- µg/m³
    no        NUMERIC(10,3),
    no2       NUMERIC(10,3),
    o3        NUMERIC(10,3),
    so2       NUMERIC(10,3),
    pm2_5     NUMERIC(10,3),
    pm10      NUMERIC(10,3),
    nh3       NUMERIC(10,3),
    charge_le TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (ville_id, temps_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_qualite_air_temps ON fact_qualite_air(temps_id);
CREATE INDEX IF NOT EXISTS idx_fact_qualite_air_ville ON fact_qualite_air(ville_id);