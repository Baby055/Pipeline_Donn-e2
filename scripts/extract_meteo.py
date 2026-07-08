"""
scripts/extract_meteo.py

Fonction d'extraction des données météo pour une ville donnée via l'API OpenWeather.
Chaque appel :
  - interroge l'API OpenWeather (endpoint "current weather")
  - extrait les champs principaux (température, humidité, pression, description)
  - sauvegarde le résultat dans un fichier CSV individuel : meteo_{ville}_{date}.csv
  - ne lève JAMAIS d'exception vers l'appelant : toute erreur est loguée et
    la fonction renvoie simplement False, pour ne pas faire planter le DAG.
"""

import os
import csv
import logging
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"

# Nombre de tentatives et timeout HTTP (secondes)
REQUEST_TIMEOUT = 10
MAX_RETRIES = 2


def _slugify(name: str) -> str:
    """Transforme 'São Paulo' -> 'sao_paulo' pour un nom de fichier propre."""
    import unicodedata

    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return normalized.strip().lower().replace(" ", "_").replace(",", "")


def _call_openweather_api(city_query: str, api_key: str, units: str = "metric") -> dict:
    """
    Appelle l'API OpenWeather pour une requête de ville donnée (ex: "Paris,FR").
    Peut lever une exception (requests.RequestException, ValueError, KeyError...).
    L'appelant (extract_meteo) est responsable de capturer ces erreurs.
    """
    params = {
        "q": city_query,
        "appid": api_key,
        "units": units,
        "lang": "fr",
    }

    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(OPENWEATHER_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_exception = exc
            logger.warning(
                "Tentative %s/%s échouée pour '%s' : %s", attempt, MAX_RETRIES, city_query, exc
            )

    # Si toutes les tentatives ont échoué, on relève la dernière exception
    raise last_exception


def extract_meteo(city_name: str, output_dir: str, api_key: str = None,
                   execution_date: str = None, country_code: str = None) -> bool:
    """
    Extrait les données météo actuelles pour `city_name` et les sauvegarde en CSV.

    Args:
        city_name: nom de la ville, ex "Paris". Peut aussi être "Paris,FR".
        output_dir: dossier où écrire le fichier CSV (ex: data/2025-05-18)
        api_key: clé API OpenWeather. Si None, lue depuis la variable
                 d'environnement OPENWEATHER_API_KEY (ou variable Airflow en amont).
        execution_date: date au format YYYY-MM-DD utilisée dans le nom de fichier.
                        Si None, la date du jour est utilisée.
        country_code: code pays ISO (ex "FR", "JP") optionnel, concaténé à city_name
                      pour lever toute ambiguïté auprès de l'API.

    Returns:
        True si l'extraction et la sauvegarde ont réussi, False sinon.
        Cette fonction ne lève jamais d'exception : toute erreur est loguée.
    """
    date_str = execution_date or datetime.utcnow().strftime("%Y-%m-%d")
    api_key = api_key or os.environ.get("OPENWEATHER_API_KEY")

    if not api_key:
        logger.error("Aucune clé API OpenWeather fournie pour '%s'. Extraction annulée.", city_name)
        return False

    city_query = f"{city_name},{country_code}" if country_code else city_name

    try:
        data = _call_openweather_api(city_query, api_key)

        # Extraction des champs principaux avec gestion défensive des clés manquantes
        main = data.get("main", {})
        weather_list = data.get("weather", [])
        weather_desc = weather_list[0].get("description") if weather_list else None

        row = {
            "ville": city_name,
            "pays": data.get("sys", {}).get("country", country_code or ""),
            "date_extraction": date_str,
            "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds"),
            "temperature_c": main.get("temp"),
            "temperature_ressentie_c": main.get("feels_like"),
            "humidite_pct": main.get("humidity"),
            "pression_hpa": main.get("pressure"),
            "description": weather_desc,
        }

        os.makedirs(output_dir, exist_ok=True)
        file_slug = _slugify(city_name)
        file_path = os.path.join(output_dir, f"meteo_{file_slug}_{date_str}.csv")

        with open(file_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row.keys()))
            writer.writeheader()
            writer.writerow(row)

        logger.info("Extraction réussie pour '%s' -> %s", city_name, file_path)
        return True

    except requests.RequestException as exc:
        logger.error("Erreur API OpenWeather pour '%s' : %s", city_name, exc)
        return False
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Erreur de parsing des données pour '%s' : %s", city_name, exc)
        return False
    except OSError as exc:
        logger.error("Erreur d'écriture du fichier CSV pour '%s' : %s", city_name, exc)
        return False
    except Exception as exc:  # filet de sécurité final : le DAG ne doit jamais planter ici
        logger.error("Erreur inattendue pour '%s' : %s", city_name, exc)
        return False


if __name__ == "__main__":
    # Petit test manuel en local (nécessite OPENWEATHER_API_KEY dans l'environnement)
    logging.basicConfig(level=logging.INFO)
    success = extract_meteo("Paris", output_dir="./data/test", country_code="FR")
    print("Succès :", success)
