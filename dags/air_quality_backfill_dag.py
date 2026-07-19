"""
dags/air_quality_backfill_dag.py

DAG de backfill historique : charge l'historique de qualité de l'air
(12 mois idéal, 3 mois minimum accepté par le sujet) avant que le pipeline
horaire (air_quality_pipeline_dag) ne prenne le relai.

Ce DAG n'est PAS planifié automatiquement (schedule=None) :
il se déclenche manuellement une seule fois depuis l'UI Airflow.

Utilise l'endpoint OpenWeather "Air Pollution History" (plan gratuit inclus).
Écrit uniquement dans raw/ (un JSON par ville/heure) — la reconstruction de
clean/, sa validation et le chargement DWH sont ensuite délégués aux mêmes
scripts que le pipeline horaire (build_clean_dataset.py, validate_clean.py,
load_dwh.py), pour garantir que backfill et flux courant produisent des
données identiques en forme.
"""

import os
import sys
import json
import logging
import calendar
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_FOLDER)
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")

if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)

from extract_air_quality import CITY_COORDS, _slugify       # noqa: E402
from build_clean_dataset import build_clean_dataset            # noqa: E402
from validate_clean import validate as validate_clean_file     # noqa: E402
from load_dwh import load_clean_csv_to_dwh                   # noqa: E402

logger = logging.getLogger(__name__)

BASE_DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DIR   = os.path.join(BASE_DATA_DIR, "raw")
CLEAN_DIR = os.path.join(BASE_DATA_DIR, "clean")
CLEAN_FILE_PATH = os.path.join(CLEAN_DIR, "air_quality_clean.csv")

AIR_POLLUTION_HISTORY_URL = "https://api.openweathermap.org/data/2.5/air_pollution/history"

# 12 mois idéal (imposé par le sujet comme cible ; 3 mois minimum accepté si
# le quota API gratuit ou le temps disponible ne permet pas d'aller plus loin).
MONTHS_OF_HISTORY = 12

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _get_api_key() -> str:
    try:
        return Variable.get("OPENWEATHER_API_KEY")
    except Exception:
        return os.environ.get("OPENWEATHER_API_KEY", "")


def _month_ranges(n_months: int):
    """Génère les bornes (start_ts, end_ts, label) des n derniers mois complets."""
    today = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    ranges = []
    cursor = today
    for _ in range(n_months):
        last_day_prev = cursor - timedelta(days=1)
        month_start = last_day_prev.replace(day=1)
        days_in_month = calendar.monthrange(month_start.year, month_start.month)[1]
        month_end = month_start.replace(day=days_in_month, hour=23, minute=59, second=59)
        ranges.append((
            int(month_start.timestamp()),
            int(month_end.timestamp()),
            month_start.strftime("%Y-%m"),
        ))
        cursor = month_start
    return list(reversed(ranges))


def _backfill_city_month(city_name: str, start_ts: int, end_ts: int, label: str, **context):
    """Récupère l'historique d'un mois pour une ville, écrit un JSON par heure dans raw/."""
    api_key = _get_api_key()
    coords = CITY_COORDS[city_name]

    if not api_key:
        logger.error("Pas de clé API, backfill annulé pour %s / %s", city_name, label)
        return False

    params = {
        "lat": coords["lat"], "lon": coords["lon"],
        "start": start_ts, "end": end_ts, "appid": api_key,
    }
    try:
        response = requests.get(AIR_POLLUTION_HISTORY_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.error("Erreur API historique pour %s / %s : %s", city_name, label, exc)
        return False

    entries = payload.get("list", [])
    if not entries:
        logger.warning("Aucune donnée historique pour %s / %s", city_name, label)
        return False

    written = 0
    for entry in entries:
        dt_utc = datetime.fromtimestamp(entry.get("dt", 0), tz=timezone.utc)
        date_str = dt_utc.strftime("%Y-%m-%d")
        hour_str = dt_utc.strftime("%H")

        raw_record = {
            "ville": city_name,
            "pays": coords["country"],
            "lat": coords["lat"],
            "lon": coords["lon"],
            "date_extraction": date_str,
            "heure_extraction": hour_str,
            "timestamp_utc": dt_utc.isoformat(timespec="seconds"),
            "source": "openweather_air_pollution_history_backfill",
            "raw_response": {"list": [entry]},
        }

        out_dir = os.path.join(RAW_DIR, date_str, hour_str)
        os.makedirs(out_dir, exist_ok=True)
        file_path = os.path.join(
            out_dir,
            f"air_quality_{_slugify(city_name)}_{date_str}_{hour_str}.json"
        )
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(raw_record, f, ensure_ascii=False, indent=2)
        written += 1

    logger.info("Backfill %s / %s : %s enregistrements écrits.", city_name, label, written)
    return True


def _rebuild_validate_load(**context):
    """Rejoue build_clean_dataset + validate_clean + load_dwh sur TOUT raw/,
    une seule fois à la fin du backfill (le fichier clean est de toute façon
    reconstruit en entier à chaque appel, inutile de le faire mois par mois).

    Fait échouer le DAG si le fichier clean reconstruit n'est pas conforme,
    plutôt que de charger silencieusement des données invalides dans le DWH.
    """
    clean_path = build_clean_dataset(raw_dir=RAW_DIR, out_path=CLEAN_FILE_PATH)

    errors = validate_clean_file(clean_path)
    if errors:
        for e in errors:
            logger.error("Validation clean/ échouée après backfill : %s", e)
        raise AirflowFailException(
            f"Fichier clean non conforme après backfill ({len(errors)} erreur(s)) : {errors}"
        )

    n_loaded = load_clean_csv_to_dwh(clean_path)
    logger.info("Backfill terminé : clean validé, %s lignes chargées au total.", n_loaded)
    return n_loaded


with DAG(
    dag_id="air_quality_backfill_dag",
    description="Backfill ponctuel d'historique qualité de l'air (12 mois idéal, "
                 "3 mois minimum) — à déclencher manuellement une seule fois",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["qualite-air", "backfill", "historique", "bloc1"],
) as dag:

    all_city_month_tasks = []
    for start_ts, end_ts, label in _month_ranges(MONTHS_OF_HISTORY):
        for city_name in CITY_COORDS:
            t = PythonOperator(
                task_id=f"backfill_{city_name.lower().replace(' ', '_')}_{label}",
                python_callable=_backfill_city_month,
                op_kwargs={
                    "city_name": city_name,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "label": label,
                },
            )
            all_city_month_tasks.append(t)

    rebuild_task = PythonOperator(
        task_id="rebuild_validate_load",
        python_callable=_rebuild_validate_load,
    )

    all_city_month_tasks >> rebuild_task