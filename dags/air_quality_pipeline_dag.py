"""
dags/air_quality_pipeline_dag.py

DAG Airflow : pipeline qualité de l'air en production (Bloc 1 - groupe).

Même structure que weather_pipeline_dag.py du repo Pipeline_Donn-e2 :
  - une tâche d'extraction par ville (appel API OpenWeather Air Pollution)
  - une tâche de transformation raw -> clean
  - une tâche de chargement dans le schéma en étoile PostgreSQL

Étapes :
    Extraction (par ville, toutes les heures)
        -> data/raw/{date}/{heure}/   (JSON brut, un fichier par ville)
        -> Transformation             (nettoyage, aplatissement)
        -> data/clean/{date}/         (CSV consolidé par heure)
        -> Chargement DWH             (schéma en étoile : dim_ville, dim_temps, fact_qualite_air)

Configuration :
    - Variable Airflow "OPENWEATHER_API_KEY" (Admin > Variables)
      repli sur variable d'environnement du même nom.
    - Variables d'environnement PG_HOST / PG_PORT / PG_DB / PG_USER / PG_PASSWORD
      pour la connexion PostgreSQL (pas de Docker, connexion directe).

Installation (sans Docker) :
    1. Copier dags/ et scripts/ dans ~/airflow/ (même niveau)
    2. pip install -r requirements.txt  (dans le venv airflow)
    3. airflow variables set OPENWEATHER_API_KEY "votre_cle"
    4. Créer la DB : psql -d air_quality -f sql/create_star_schema.sql
    5. airflow standalone
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_FOLDER)
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")

if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)

from extract_air_quality import extract_air_quality, CITY_COORDS  # noqa: E402
from transform_air_quality import transform_files                   # noqa: E402
from load_dwh import load_clean_csv_to_dwh                         # noqa: E402

logger = logging.getLogger(__name__)

BASE_DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DIR  = os.path.join(BASE_DATA_DIR, "raw")
CLEAN_DIR = os.path.join(BASE_DATA_DIR, "clean")

CITIES = list(CITY_COORDS.keys())

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _get_api_key() -> str:
    """Récupère la clé API OpenWeather depuis une Variable Airflow, sinon l'env."""
    try:
        return Variable.get("OPENWEATHER_API_KEY")
    except Exception:
        return os.environ.get("OPENWEATHER_API_KEY", "")


def _extract_task(city_name: str, **context):
    """Extraction (raw) pour une ville, à l'heure d'exécution du DAG."""
    execution_date = context["ds"]
    execution_hour = context["ts"][11:13]
    output_dir = os.path.join(RAW_DIR, execution_date, execution_hour)
    api_key = _get_api_key()

    success = extract_air_quality(
        city_name=city_name,
        output_dir=output_dir,
        api_key=api_key,
        execution_date=execution_date,
        execution_hour=execution_hour,
    )
    if not success:
        logger.warning(
            "Extraction échouée pour %s, elle sera absente du fichier clean.", city_name
        )
    return success


def _transform_task(**context):
    """Transformation raw -> clean pour l'heure d'exécution du DAG."""
    execution_date = context["ds"]
    execution_hour = context["ts"][11:13]

    raw_dir   = os.path.join(RAW_DIR, execution_date, execution_hour)
    clean_dir = os.path.join(CLEAN_DIR, execution_date)

    clean_path = transform_files(
        date=execution_date,
        hour=execution_hour,
        raw_dir=raw_dir,
        clean_dir=clean_dir,
    )
    logger.info("Fichier clean produit : %s", clean_path)
    return clean_path


def _load_task(**context):
    """Chargement du fichier clean dans le schéma en étoile PostgreSQL."""
    ti = context["ti"]
    clean_path = ti.xcom_pull(task_ids="transform_air_quality")
    n_rows = load_clean_csv_to_dwh(clean_path)
    logger.info("Chargement DWH terminé : %s lignes.", n_rows)
    return n_rows


with DAG(
    dag_id="air_quality_pipeline_dag",
    description="Extraction horaire de la qualité de l'air multi-villes → clean → DWH étoile",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["qualite-air", "openweather", "etl", "bloc1"],
) as dag:

    extract_tasks = []
    for city_name in CITIES:
        task_id = f"extract_{city_name.lower().replace(' ', '_')}"
        task = PythonOperator(
            task_id=task_id,
            python_callable=_extract_task,
            op_kwargs={"city_name": city_name},
        )
        extract_tasks.append(task)

    transform_task = PythonOperator(
        task_id="transform_air_quality",
        python_callable=_transform_task,
    )

    load_task = PythonOperator(
        task_id="load_dwh",
        python_callable=_load_task,
    )

    # Toutes les extractions (gérées en interne) avant transformation, puis chargement
    extract_tasks >> transform_task >> load_task