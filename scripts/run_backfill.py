import os
import sys
import json
import argparse
import logging
import calendar
from datetime import datetime, timedelta, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from extract_air_quality import CITY_COORDS, _slugify           # noqa: E402
from build_clean_dataset import build_clean_dataset              # noqa: E402
from validate_clean import validate as validate_clean_file       # noqa: E402
from load_dwh import load_clean_csv_to_dwh                       # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
CLEAN_DIR = os.path.join(PROJECT_ROOT, "data", "clean")
CLEAN_FILE_PATH = os.path.join(CLEAN_DIR, "air_quality_clean.csv")

AIR_POLLUTION_HISTORY_URL = "https://api.openweathermap.org/data/2.5/air_pollution/history"


def _month_ranges(n_months: int):
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


def _backfill_city_month(city_name: str, start_ts: int, end_ts: int, label: str, api_key: str) -> bool:
    coords = CITY_COORDS[city_name]
    params = {"lat": coords["lat"], "lon": coords["lon"], "start": start_ts, "end": end_ts, "appid": api_key}

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
        file_path = os.path.join(out_dir, f"air_quality_{_slugify(city_name)}_{date_str}_{hour_str}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(raw_record, f, ensure_ascii=False, indent=2)
        written += 1

    logger.info("Backfill %s / %s : %s enregistrements écrits.", city_name, label, written)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", type=int, default=12, help="Nombre de mois d'historique à récupérer")
    args = parser.parse_args()

    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key:
        logger.error("OPENWEATHER_API_KEY manquant. Backfill annulé.")
        return 1

    for start_ts, end_ts, label in _month_ranges(args.months):
        for city_name in CITY_COORDS:
            _backfill_city_month(city_name, start_ts, end_ts, label, api_key)

    logger.info("Backfill terminé, reconstruction de clean/...")
    clean_path = build_clean_dataset(raw_dir=RAW_DIR, out_path=CLEAN_FILE_PATH)

    errors = validate_clean_file(clean_path)
    if errors:
        for e in errors:
            logger.error("Validation clean/ échouée après backfill : %s", e)
        return 1

    n_loaded = load_clean_csv_to_dwh(clean_path)
    logger.info("Backfill terminé : clean validé, %s lignes chargées dans le warehouse.", n_loaded)
    return 0


if __name__ == "__main__":
    sys.exit(main())