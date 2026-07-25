"""
dags/air_quality_pipeline_dag.py

DAG Airflow : pipeline qualité de l'air en production (Bloc 1 - groupe).

Étapes :
    Extraction (par ville, toutes les heures)
        -> data/raw/{date}/{heure}/   (JSON brut, un fichier par ville, jamais modifié)
        -> build_clean_dataset        (relit TOUT raw/, reconstruit intégralement
                                        data/clean/air_quality_clean.csv)
        -> validate_clean             (le run échoue si le fichier clean ne respecte
                                        pas le contrat de données : colonnes, doublons,
                                        tri, plages de valeurs, nombre de villes —
                                        aucun fichier invalide n'est chargé dans le DWH)
        -> load_dwh                   (schéma en étoile : dim_ville, dim_temps, fact_qualite_air)

Configuration :
    - Variable Airflow "OPENWEATHER_API_KEY" (Admin > Variables)
      repli sur variable d'environnement du même nom.
    - Variables d'environnement PG_HOST / PG_PORT / PG_DB / PG_USER / PG_PASSWORD
      pour la connexion PostgreSQL (PG_PASSWORD obligatoire, aucun défaut —
      voir scripts/load_dwh.py).

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
from airflow.exceptions import AirflowFailException

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_FOLDER)
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")

if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)

from extract_air_quality import extract_air_quality, CITY_COORDS  # noqa: E402
from build_clean_dataset import build_clean_dataset                # noqa: E402
from validate_clean import validate as validate_clean_file         # noqa: E402
from load_dwh import load_clean_csv_to_dwh                         # noqa: E402

logger = logging.getLogger(__name__)

BASE_DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DIR = os.path.join(BASE_DATA_DIR, "raw")
CLEAN_DIR = os.path.join(BASE_DATA_DIR, "clean")
CLEAN_FILE_PATH = os.path.join(CLEAN_DIR, "air_quality_clean.csv")

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


def _build_clean_task(**context):
    """Reconstruit le fichier clean unique à partir de TOUT data/raw/."""
    clean_path = build_clean_dataset(raw_dir=RAW_DIR, out_path=CLEAN_FILE_PATH)
    logger.info("Fichier clean (unique) reconstruit : %s", clean_path)
    return clean_path


def _validate_clean_task(**context):
    """Valide le fichier clean produit ; fait échouer le DAG si non conforme.

    On ne veut jamais charger un fichier invalide dans le DWH : mieux vaut un
    run en échec (visible dans l'historique) qu'un warehouse silencieusement
    corrompu.
    """
    ti = context["ti"]
    clean_path = ti.xcom_pull(task_ids="build_clean_dataset")

    errors = validate_clean_file(clean_path)
    if errors:
        for e in errors:
            logger.error("Validation clean/ échouée : %s", e)
        raise AirflowFailException(
            f"Fichier clean non conforme au contrat de données ({len(errors)} erreur(s)), "
            f"chargement DWH annulé : {errors}"
        )

    logger.info("Fichier clean validé avec succès : %s", clean_path)
    return clean_path


def _load_task(**context):
    """Chargement du fichier clean validé dans le schéma en étoile PostgreSQL."""
    ti = context["ti"]
    clean_path = ti.xcom_pull(task_ids="validate_clean")
    n_rows = load_clean_csv_to_dwh(clean_path)
    logger.info("Chargement DWH terminé : %s lignes.", n_rows)
    return n_rows


with DAG(
    dag_id="air_quality_pipeline_dag",
    description="Extraction horaire de la qualité de l'air multi-villes -> "
                 "reconstruction clean/ unique -> validation -> DWH étoile",
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

    build_clean_task = PythonOperator(
        task_id="build_clean_dataset",
        python_callable=_build_clean_task,
    )

    validate_task = PythonOperator(
        task_id="validate_clean",
        python_callable=_validate_clean_task,
    )

    load_task = PythonOperator(
        task_id="load_dwh",
        python_callable=_load_task,
    )

    # Toutes les extractions (gérées en interne, jamais bloquantes) avant de
    # reconstruire clean/, puis validation obligatoire avant le chargement DWH.
    extract_tasks >> build_clean_task >> validate_task >> load_task