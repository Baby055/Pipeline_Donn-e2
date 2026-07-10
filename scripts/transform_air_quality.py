"""
scripts/transform_air_quality.py

Transformation raw -> clean (même rôle que merge_meteo.py dans Pipeline_Donn-e2,
mais avec nettoyage/filtrage en plus pour la qualité de l'air).

Comportement :
  - lit tous les JSON bruts d'un dossier data/raw/{date}/{heure}/
  - aplatit chaque réponse OpenWeather Air Pollution en une ligne tabulaire
  - filtre les valeurs aberrantes (AQI hors 1-5, polluants négatifs)
  - écrit un CSV consolidé dans data/clean/{date}/air_quality_clean_{date}_{heure}.csv
"""

import os
import glob
import json
import logging

import pandas as pd

logger = logging.getLogger(__name__)

AQI_MIN, AQI_MAX = 1, 5
POLLUTANT_MAX_UGM3 = 2000


def _flatten_record(raw_record: dict) -> dict | None:
    """Transforme un enregistrement brut OpenWeather en une ligne plate."""
    try:
        payload_list = raw_record.get("raw_response", {}).get("list", [])
        if not payload_list:
            logger.warning("Réponse API vide pour '%s', ligne ignorée.", raw_record.get("ville"))
            return None

        entry = payload_list[0]
        components = entry.get("components", {})
        aqi = entry.get("main", {}).get("aqi")

        return {
            "ville":              raw_record.get("ville"),
            "pays":               raw_record.get("pays"),
            "lat":                raw_record.get("lat"),
            "lon":                raw_record.get("lon"),
            "date_extraction":    raw_record.get("date_extraction"),
            "heure_extraction":   raw_record.get("heure_extraction"),
            "timestamp_utc":      raw_record.get("timestamp_utc"),
            "aqi":                aqi,
            "co":                 components.get("co"),
            "no":                 components.get("no"),
            "no2":                components.get("no2"),
            "o3":                 components.get("o3"),
            "so2":                components.get("so2"),
            "pm2_5":              components.get("pm2_5"),
            "pm10":               components.get("pm10"),
            "nh3":                components.get("nh3"),
        }
    except (AttributeError, IndexError, TypeError) as exc:
        logger.warning("Impossible d'aplatir un enregistrement brut : %s", exc)
        return None


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Dédoublonnage + filtrage des valeurs aberrantes."""
    before = len(df)

    df = df.drop_duplicates(
        subset=["ville", "date_extraction", "heure_extraction"], keep="last"
    )
    df = df[df["aqi"].isna() | df["aqi"].between(AQI_MIN, AQI_MAX)]

    pollutant_cols = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
    for col in pollutant_cols:
        if col in df.columns:
            invalid = (df[col] < 0) | (df[col] > POLLUTANT_MAX_UGM3)
            df.loc[invalid, col] = pd.NA

    after = len(df)
    if after < before:
        logger.info("Nettoyage : %s ligne(s) supprimée(s) sur %s.", before - after, before)

    return df.reset_index(drop=True)


def transform_files(date: str, hour: str, raw_dir: str, clean_dir: str) -> str:
    """
    Transforme les JSON bruts d'une date/heure en un CSV nettoyé.

    Args:
        date:      YYYY-MM-DD
        hour:      HH
        raw_dir:   dossier contenant les JSON (data/raw/{date}/{hour})
        clean_dir: dossier de sortie (data/clean/{date})

    Returns:
        Chemin du CSV clean produit.

    Raises:
        FileNotFoundError: si aucun fichier brut trouvé.
    """
    pattern = os.path.join(raw_dir, f"air_quality_*_{date}_{hour}.json")
    raw_files = sorted(glob.glob(pattern))

    if not raw_files:
        msg = f"Aucun fichier brut trouvé pour {date} {hour}h dans {raw_dir}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    logger.info("Fichiers bruts trouvés pour %s %sh : %s", date, hour, raw_files)

    rows = []
    for file_path in raw_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                raw_record = json.load(f)
            row = _flatten_record(raw_record)
            if row:
                rows.append(row)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Impossible de lire %s : %s", file_path, exc)

    if not rows:
        msg = f"Aucune ligne exploitable pour {date} {hour}h"
        logger.error(msg)
        raise FileNotFoundError(msg)

    df = pd.DataFrame(rows)
    df = _clean_dataframe(df)
    df = df.sort_values(by="ville").reset_index(drop=True)

    os.makedirs(clean_dir, exist_ok=True)
    clean_path = os.path.join(clean_dir, f"air_quality_clean_{date}_{hour}.csv")
    df.to_csv(clean_path, index=False, encoding="utf-8")

    logger.info("Fichier clean produit : %s (%s lignes)", clean_path, len(df))
    return clean_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    transform_files(
        date="2026-07-10", hour="14",
        raw_dir="./data/raw/2026-07-10/14",
        clean_dir="./data/clean/2026-07-10",
    )