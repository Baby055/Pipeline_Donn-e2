"""
scripts/build_clean_dataset.py

Reconstruit LE fichier clean/ unique exigé par le contrat de données du sujet :

    "clean/ : UN fichier CSV unique, reconstruit à chaque run depuis raw/"

Contrairement à transform_air_quality.py (qui ne traite qu'une date/heure à la
fois pour alimenter le chargement incrémental du warehouse), ce script relit
TOUT data/raw/ à chaque exécution et régénère intégralement
data/clean/air_quality_clean.csv :

  - une ligne par (ville, date, heure)
  - triée chronologiquement (date, heure) puis par ville
  - dédoublonnée (même ville + même heure ne doit apparaître qu'une fois,
    on garde la dernière extraction en cas de doublon)
  - filtrée des valeurs aberrantes (AQI hors 1-5, polluants négatifs ou
    absurdement élevés), exactement comme transform_air_quality.py

Il ne modifie JAMAIS les fichiers de raw/ (lecture seule) et peut être rejoué
autant de fois que nécessaire : le fichier de sortie est toujours réécrit en
entier, jamais complété en append.

Usage :
    python scripts/build_clean_dataset.py \
        --raw-dir data/raw --out data/clean/air_quality_clean.csv
"""

import os
import sys
import glob
import json
import argparse
import logging

import pandas as pd

logger = logging.getLogger(__name__)

AQI_MIN, AQI_MAX = 1, 5
POLLUTANT_MAX_UGM3 = 2000

COLUMN_ORDER = [
    "ville", "pays", "lat", "lon",
    "date_extraction", "heure_extraction", "timestamp_utc",
    "aqi", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3",
]


def _flatten_record(raw_record: dict) -> dict | None:
    try:
        payload_list = raw_record.get("raw_response", {}).get("list", [])
        if not payload_list:
            return None
        entry = payload_list[0]
        components = entry.get("components", {})
        aqi = entry.get("main", {}).get("aqi")
        return {
            "ville":            raw_record.get("ville"),
            "pays":             raw_record.get("pays"),
            "lat":              raw_record.get("lat"),
            "lon":              raw_record.get("lon"),
            "date_extraction":  raw_record.get("date_extraction"),
            "heure_extraction": raw_record.get("heure_extraction"),
            "timestamp_utc":    raw_record.get("timestamp_utc"),
            "aqi":              aqi,
            "co":               components.get("co"),
            "no":               components.get("no"),
            "no2":              components.get("no2"),
            "o3":               components.get("o3"),
            "so2":              components.get("so2"),
            "pm2_5":            components.get("pm2_5"),
            "pm10":             components.get("pm10"),
            "nh3":              components.get("nh3"),
        }
    except (AttributeError, IndexError, TypeError) as exc:
        logger.warning("Impossible d'aplatir un enregistrement brut : %s", exc)
        return None


def _load_all_raw_records(raw_dir: str) -> list[dict]:
    """Lit récursivement tous les JSON de raw/ (data/raw/{date}/{heure}/*.json)."""
    pattern = os.path.join(raw_dir, "**", "air_quality_*.json")
    files = sorted(glob.glob(pattern, recursive=True))
    logger.info("%s fichier(s) brut(s) trouvé(s) sous %s", len(files), raw_dir)

    rows = []
    for file_path in files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                raw_record = json.load(f)
            row = _flatten_record(raw_record)
            if row:
                rows.append(row)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Impossible de lire %s : %s", file_path, exc)
    return rows


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    # Une seule ligne par ville + heure : on garde la dernière extraction connue.
    df = df.sort_values(by=["ville", "date_extraction", "heure_extraction", "timestamp_utc"])
    df = df.drop_duplicates(subset=["ville", "date_extraction", "heure_extraction"], keep="last")

    df = df[df["aqi"].isna() | df["aqi"].between(AQI_MIN, AQI_MAX)]

    pollutant_cols = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
    for col in pollutant_cols:
        if col in df.columns:
            invalid = (df[col] < 0) | (df[col] > POLLUTANT_MAX_UGM3)
            df.loc[invalid, col] = pd.NA

    after = len(df)
    if after < before:
        logger.info("Nettoyage global : %s ligne(s) supprimée(s) sur %s.", before - after, before)

    return df.reset_index(drop=True)


def build_clean_dataset(raw_dir: str, out_path: str) -> str:
    """Reconstruit intégralement le fichier clean unique depuis raw_dir."""
    rows = _load_all_raw_records(raw_dir)
    if not rows:
        msg = f"Aucune donnée brute exploitable trouvée sous {raw_dir}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    df = pd.DataFrame(rows)
    df = _clean_dataframe(df)

    # Tri chronologique global (date, heure) puis ville, comme exigé par le contrat.
    df = df.sort_values(by=["date_extraction", "heure_extraction", "ville"]).reset_index(drop=True)
    df = df[[c for c in COLUMN_ORDER if c in df.columns]]

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")

    logger.info("Fichier clean unique reconstruit : %s (%s lignes, %s villes)",
                out_path, len(df), df["ville"].nunique())
    return out_path


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", default="data/raw", help="Dossier raw/ à relire")
    parser.add_argument("--out", default="data/clean/air_quality_clean.csv",
                         help="Chemin du fichier clean unique à (re)générer")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    try:
        build_clean_dataset(raw_dir=args.raw_dir, out_path=args.out)
    except FileNotFoundError as exc:
        logger.error(str(exc))
        sys.exit(1)