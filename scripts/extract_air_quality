"""
scripts/extract_air_quality.py

Fonction d'extraction de la qualité de l'air (courante) pour une ville donnée,
via l'API OpenWeather Air Pollution.

Chaque appel :
  - interroge l'API OpenWeather (endpoint "air_pollution")
  - construit un enregistrement brut normalisé (même structure que celle
    utilisée par le DAG de backfill : ville, pays, lat, lon, date/heure
    d'extraction, timestamp, raw_response)
  - sauvegarde ce JSON dans output_dir/air_quality_{ville}_{date}_{heure}.json
  - ne lève JAMAIS d'exception vers l'appelant : toute erreur est loguée et
    la fonction renvoie simplement False, pour ne pas faire planter le DAG.
"""

import os
import json
import logging
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

AIR_POLLUTION_URL = "https://api.openweathermap.org/data/2.5/air_pollution"

REQUEST_TIMEOUT = 10
MAX_RETRIES = 2

# Coordonnées des villes suivies (mêmes villes que weather_pipeline_dag.py).
CITY_COORDS = {
    "Paris":        {"lat": 48.8566, "lon": 2.3522,   "country": "FR"},
    "Tokyo":        {"lat": 35.6762, "lon": 139.6503, "country": "JP"},
    "New York":     {"lat": 40.7128, "lon": -74.0060, "country": "US"},
    "Antananarivo": {"lat": -18.8792, "lon": 47.5079, "country": "MG"},
    "Sydney":       {"lat": -33.8688, "lon": 151.2093, "country": "AU"},
    "Cairo":        {"lat": 30.0444, "lon": 31.2357,  "country": "EG"},
}


def _slugify(name: str) -> str:
    """Transforme 'São Paulo' -> 'sao_paulo' pour un nom de fichier propre."""
    import unicodedata

    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return normalized.strip().lower().replace(" ", "_").replace(",", "")


def _call_openweather_air_pollution(lat: float, lon: float, api_key: str) -> dict:
    """
    Appelle l'API OpenWeather Air Pollution pour des coordonnées données.
    Peut lever une exception (requests.RequestException...).
    L'appelant (extract_air_quality) est responsable de capturer ces erreurs.
    """
    params = {"lat": lat, "lon": lon, "appid": api_key}

    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(AIR_POLLUTION_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_exception = exc
            logger.warning(
                "Tentative %s/%s échouée pour lat=%s lon=%s : %s", attempt, MAX_RETRIES, lat, lon, exc
            )

    raise last_exception


def extract_air_quality(city_name: str, output_dir: str, api_key: str = None,
                         execution_date: str = None, execution_hour: str = None) -> bool:
    """
    Extrait la qualité de l'air actuelle pour `city_name` et la sauvegarde en JSON.

    Args:
        city_name: nom de la ville, doit être une clé de CITY_COORDS.
        output_dir: dossier où écrire le fichier JSON (ex: data/raw/2026-07-10/14)
        api_key: clé API OpenWeather. Si None, lue depuis la variable
                 d'environnement OPENWEATHER_API_KEY.
        execution_date: date au format YYYY-MM-DD utilisée dans le nom de fichier.
                        Si None, la date du jour (UTC) est utilisée.
        execution_hour: heure au format HH utilisée dans le nom de fichier.
                        Si None, l'heure actuelle (UTC) est utilisée.

    Returns:
        True si l'extraction et la sauvegarde ont réussi, False sinon.
        Cette fonction ne lève jamais d'exception : toute erreur est loguée.
    """
    now = datetime.utcnow()
    date_str = execution_date or now.strftime("%Y-%m-%d")
    hour_str = execution_hour or now.strftime("%H")
    api_key = api_key or os.environ.get("OPENWEATHER_API_KEY")

    if not api_key:
        logger.error("Aucune clé API OpenWeather fournie pour '%s'. Extraction annulée.", city_name)
        return False

    coords = CITY_COORDS.get(city_name)
    if coords is None:
        logger.error("Ville inconnue dans CITY_COORDS : '%s'. Extraction annulée.", city_name)
        return False

    try:
        payload = _call_openweather_air_pollution(coords["lat"], coords["lon"], api_key)

        entries = payload.get("list", [])
        if not entries:
            logger.warning("Réponse API vide pour '%s'.", city_name)
            return False

        raw_record = {
            "ville": city_name,
            "pays": coords["country"],
            "lat": coords["lat"],
            "lon": coords["lon"],
            "date_extraction": date_str,
            "heure_extraction": hour_str,
            "timestamp_utc": now.isoformat(timespec="seconds"),
            "source": "openweather_air_pollution",
            "raw_response": {"list": [entries[0]]},
        }

        os.makedirs(output_dir, exist_ok=True)
        file_slug = _slugify(city_name)
        file_path = os.path.join(
            output_dir, f"air_quality_{file_slug}_{date_str}_{hour_str}.json"
        )

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(raw_record, f, ensure_ascii=False, indent=2)

        logger.info("Extraction réussie pour '%s' -> %s", city_name, file_path)
        return True

    except requests.RequestException as exc:
        logger.error("Erreur API OpenWeather pour '%s' : %s", city_name, exc)
        return False
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Erreur de parsing des données pour '%s' : %s", city_name, exc)
        return False
    except OSError as exc:
        logger.error("Erreur d'écriture du fichier JSON pour '%s' : %s", city_name, exc)
        return False
    except Exception as exc:  # filet de sécurité final : le DAG ne doit jamais planter ici
        logger.error("Erreur inattendue pour '%s' : %s", city_name, exc)
        return False


if __name__ == "__main__":
    # Petit test manuel en local (nécessite OPENWEATHER_API_KEY dans l'environnement)
    logging.basicConfig(level=logging.INFO)
    success = extract_air_quality("Paris", output_dir="./data/test")
    print("Succès :", success)
