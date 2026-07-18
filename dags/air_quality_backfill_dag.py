"""
dags/air_quality_backfill_dag.py

DAG de backfill historique : charge 12 mois d'historique de qualité de l'air
avant que le pipeline horaire (air_quality_pipeline_dag) ne prenne le relai.

Ce DAG n'est PAS planifié automatiquement (schedule=None) :
il se déclenche manuellement une seule fois depuis l'UI Airflow.

Utilise l'endpoint OpenWeather "Air Pollution History" (plan gratuit inclus).
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

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_FOLDER)
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")

if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)

from extract_air_quality import CITY_COORDS, _slugify  # noqa: E402
from build_clean_dataset import build_clean_dataset       # noqa: E402
from load_dwh import load_clean_csv_to_dwh              # noqa: E402

logger = logging.getLogger(__name__)

BASE_DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DIR   = os.path.join(BASE_DATA_DIR, "raw")
CLEAN_DIR = os.path.join(BASE_DATA_DIR, "clean")
CLEAN_FILE_PATH = os.path.join(CLEAN_DIR, "air_quality_clean.csv")

AIR_POLLUTION_HISTORY_URL = "https://api.openweathermap.org/data/2.5/air_pollution/history"
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
    """Récupère l'historique d'un mois pour une ville, écrit un JSON par heure."""
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


def _transform_and_load_month(label: str, **context):
    """
    Après le backfill brut d'un mois pour toutes les villes, reconstruit le
    fichier clean UNIQUE depuis tout raw/ (pas seulement ce mois) et recharge
    le DWH. Le rechargement est idempotent (UPSERT), donc rejouer sur tout
    raw/ à chaque mois ne duplique rien.
    """
    clean_path = build_clean_dataset(raw_dir=RAW_DIR, out_path=CLEAN_FILE_PATH)
    n_loaded = load_clean_csv_to_dwh(clean_path)
    logger.info("Backfill %s : clean reconstruit, %s lignes chargées au total.", label, n_loaded)
    return n_loaded


with DAG(
    dag_id="air_quality_backfill_dag",
    description="Backfill ponctuel de 12 mois d'historique qualité de l'air (déclencher manuellement une seule fois)",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["qualite-air", "backfill", "historique", "bloc1"],
) as dag:

    previous_load = None
    for start_ts, end_ts, label in _month_ranges(MONTHS_OF_HISTORY):
        city_tasks = []
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
            city_tasks.append(t)

        load_t = PythonOperator(
            task_id=f"transform_load_{label}",
            python_callable=_transform_and_load_month,
            op_kwargs={"label": label},
        )

        city_tasks >> load_t

        if previous_load is not None:
            previous_load >> city_tasks[0]
        previous_load = load_t