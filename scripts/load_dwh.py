"""
scripts/load_dwh.py

Chargement du CSV clean dans le schéma en étoile PostgreSQL local
(sans Docker — connexion directe via variables d'environnement ou
Connection Airflow "air_quality_dwh").

Schéma : dim_ville, dim_temps, fact_qualite_air
(voir sql/create_star_schema.sql)
"""

import os
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def _get_connection():
    """
    Ouvre une connexion Postgres.
    Essaie d'abord une Connection Airflow "air_quality_dwh",
    puis retombe sur les variables d'environnement locales.
    Configuration sans Docker :
        PG_HOST     = localhost
        PG_PORT     = 5432
        PG_DB       = air_quality
        PG_USER     = air_quality_user
        PG_PASSWORD = air_quality_pass
    """
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("air_quality_dwh")
        return psycopg2.connect(
            host=conn.host,
            port=conn.port or 5432,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
        )
    except Exception:
        logger.info("Pas de Connection Airflow, repli sur les variables d'environnement.")
        return psycopg2.connect(
            host=os.environ.get("PG_HOST", "localhost"),
            port=os.environ.get("PG_PORT", "5432"),
            dbname=os.environ.get("PG_DB", "air_quality"),
            user=os.environ.get("PG_USER", "air_quality_user"),
            password=os.environ.get("PG_PASSWORD", "air_quality_pass"),
        )


def _upsert_dim_ville(cur, df: pd.DataFrame) -> dict:
    """Insère les villes inconnues, retourne mapping (ville, pays) -> ville_id."""
    villes = df[["ville", "pays", "lat", "lon"]].drop_duplicates()
    rows = list(villes.itertuples(index=False, name=None))

    execute_values(
        cur,
        """
        INSERT INTO dim_ville (ville, pays, lat, lon)
        VALUES %s
        ON CONFLICT (ville, pays) DO NOTHING
        """,
        rows,
    )

    cur.execute("SELECT ville_id, ville, pays FROM dim_ville")
    return {(ville, pays): ville_id for ville_id, ville, pays in cur.fetchall()}


def _upsert_dim_temps(cur, df: pd.DataFrame) -> dict:
    """Insère les couples (date, heure) inconnus, retourne mapping -> temps_id."""
    temps_df = df[["date_extraction", "heure_extraction"]].drop_duplicates()

    rows = []
    for date_str, heure_str in temps_df.itertuples(index=False, name=None):
        d = datetime.strptime(date_str, "%Y-%m-%d")
        jour_semaine = d.isoweekday()
        est_weekend = jour_semaine in (6, 7)
        rows.append((d.date(), int(heure_str), jour_semaine, est_weekend, d.month, d.year))

    execute_values(
        cur,
        """
        INSERT INTO dim_temps (date_valeur, heure, jour_semaine, est_weekend, mois, annee)
        VALUES %s
        ON CONFLICT (date_valeur, heure) DO NOTHING
        """,
        rows,
    )

    cur.execute("SELECT temps_id, date_valeur, heure FROM dim_temps")
    return {(str(date_valeur), heure): temps_id for temps_id, date_valeur, heure in cur.fetchall()}


def load_clean_csv_to_dwh(clean_csv_path: str) -> int:
    """
    Charge un fichier CSV clean dans le schéma en étoile.

    Args:
        clean_csv_path: chemin vers air_quality_clean_{date}_{heure}.csv

    Returns:
        Nombre de lignes de faits insérées/mises à jour.
    """
    df = pd.read_csv(clean_csv_path)
    if df.empty:
        logger.warning("Fichier clean vide, rien à charger : %s", clean_csv_path)
        return 0

    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                ville_map = _upsert_dim_ville(cur, df)
                temps_map = _upsert_dim_temps(cur, df)

                fact_rows = []
                for record in df.to_dict(orient="records"):
                    ville_id = ville_map.get((record["ville"], record["pays"]))
                    temps_id = temps_map.get((record["date_extraction"], int(record["heure_extraction"])))
                    if ville_id is None or temps_id is None:
                        logger.warning("Clé de dimension manquante pour %s, ligne ignorée.", record)
                        continue

                    fact_rows.append((
                        ville_id, temps_id,
                        record.get("aqi"),
                        record.get("co"),   record.get("no"),
                        record.get("no2"),  record.get("o3"),
                        record.get("so2"),  record.get("pm2_5"),
                        record.get("pm10"), record.get("nh3"),
                    ))

                if fact_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO fact_qualite_air
                            (ville_id, temps_id, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
                        VALUES %s
                        ON CONFLICT (ville_id, temps_id) DO UPDATE SET
                            aqi   = EXCLUDED.aqi,   co    = EXCLUDED.co,
                            no    = EXCLUDED.no,    no2   = EXCLUDED.no2,
                            o3    = EXCLUDED.o3,    so2   = EXCLUDED.so2,
                            pm2_5 = EXCLUDED.pm2_5, pm10  = EXCLUDED.pm10,
                            nh3   = EXCLUDED.nh3
                        """,
                        fact_rows,
                    )

        logger.info("Chargement DWH réussi : %s lignes pour %s", len(fact_rows), clean_csv_path)
        return len(fact_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    n = load_clean_csv_to_dwh("./data/clean/2026-07-10/air_quality_clean_2026-07-10_14.csv")
    print("Lignes chargées :", n)