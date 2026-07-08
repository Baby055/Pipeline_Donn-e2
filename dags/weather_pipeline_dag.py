"""
dags/weather_pipeline_dag.py

DAG Airflow : pipeline météo global.

Étapes :
    1. Une tâche d'extraction par ville (appel API OpenWeather + sauvegarde CSV).
       Chaque tâche gère ses propres erreurs (try/except dans extract_meteo)
       et ne fait donc jamais échouer le DAG, même si l'API est indisponible.
    2. Une tâche de fusion qui lit tous les CSV du jour et produit
       meteo_global_{date}.csv, une fois que toutes les extractions sont terminées.

Configuration :
    - La clé API OpenWeather est lue depuis la Variable Airflow "OPENWEATHER_API_KEY"
      (Admin > Variables), avec repli sur la variable d'environnement du même nom.
    - La liste des villes est définie ci-dessous (ville, code pays).
    - Les fichiers sont écrits dans {AIRFLOW_HOME ou BASE_DATA_DIR}/data/{date}/
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_FOLDER)
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")

if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)

from extract_meteo import extract_meteo  # noqa: E402
from merge_meteo import merge_files  # noqa: E402

logger = logging.getLogger(__name__)


CITIES = [
    ("Paris", "FR"),
    ("Tokyo", "JP"),
    ("New York", "US"),
    ("Antananarivo", "MG"),
    ("Sydney", "AU"),
    ("Cairo", "EG"),
]

BASE_DATA_DIR = os.path.join(PROJECT_ROOT, "data")

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


def _extract_task(city_name: str, country_code: str, **context):
    """Wrapper appelé par PythonOperator pour une ville donnée."""
    execution_date = context["ds"]  # format YYYY-MM-DD
    output_dir = os.path.join(BASE_DATA_DIR, execution_date)
    api_key = _get_api_key()

    success = extract_meteo(
        city_name=city_name,
        output_dir=output_dir,
        api_key=api_key,
        execution_date=execution_date,
        country_code=country_code,
    )

    if not success:
        logger.warning(
            "L'extraction pour %s (%s) a échoué et sera absente du fichier global.",
            city_name,
            country_code,
        )
    return success


def _merge_task(**context):
    """Wrapper appelé par PythonOperator pour fusionner les fichiers du jour."""
    execution_date = context["ds"]
    data_dir = os.path.join(BASE_DATA_DIR, execution_date)
    merged_path = merge_files(date=execution_date, data_dir=data_dir)
    logger.info("Fichier global produit : %s", merged_path)
    return merged_path


with DAG(
    dag_id="weather_pipeline_dag",
    description="Extraction météo multi-villes (OpenWeather) puis fusion en un fichier global quotidien",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["meteo", "openweather", "etl"],
) as dag:

    extract_tasks = []
    for city_name, country_code in CITIES:
        # Identifiant de tâche stable et sans caractères spéciaux
        task_id = f"extract_{city_name.lower().replace(' ', '_')}"

        task = PythonOperator(
            task_id=task_id,
            python_callable=_extract_task,
            op_kwargs={"city_name": city_name, "country_code": country_code},
        )
        extract_tasks.append(task)

    merge_task = PythonOperator(
        task_id="merge_meteo_files",
        python_callable=_merge_task,
    )

    # Toutes les extractions doivent être terminées (avec succès ou échec géré)
    # avant de lancer la fusion.
    extract_tasks >> merge_task
