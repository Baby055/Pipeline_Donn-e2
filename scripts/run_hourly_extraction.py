import os
import sys
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from extract_air_quality import extract_air_quality, CITY_COORDS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")


def main() -> int:
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key:
        logger.error("OPENWEATHER_API_KEY manquant dans l'environnement. Extraction annulée.")
        return 1

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    output_dir = os.path.join(RAW_DIR, date_str, hour_str)

    logger.info("Extraction horaire pour %s villes, %s %sh UTC.", len(CITY_COORDS), date_str, hour_str)

    n_success = 0
    for city_name in CITY_COORDS:
        success = extract_air_quality(
            city_name=city_name,
            output_dir=output_dir,
            api_key=api_key,
            execution_date=date_str,
            execution_hour=hour_str,
        )
        if success:
            n_success += 1

    logger.info("Extraction terminée : %s/%s villes réussies.", n_success, len(CITY_COORDS))

    if n_success == 0:
        logger.error("Aucune ville extraite avec succès, le run est marqué en échec.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())