"""
  - colonnes attendues toutes présentes
  - une seule ligne par (ville, date_extraction, heure_extraction) : pas de doublon
  - trié chronologiquement (date_extraction, heure_extraction croissants)
  - AQI dans [1, 5] quand renseigné
  - polluants >= 0 quand renseignés
  - lat/lon non nuls et plausibles (-90/90, -180/180)
  - au moins 5 villes distinctes

Usage :
    python scripts/validate_clean.py --file data/clean/air_quality_clean.csv

Sortie : code retour 0 si tout est conforme, 1 sinon, avec le détail des
erreurs sur stdout. Prévu pour être branché en CI ou lancé manuellement
avant de livrer le fichier clean/.
"""

import sys
import argparse

import pandas as pd

REQUIRED_COLUMNS = [
    "ville", "pays", "lat", "lon",
    "date_extraction", "heure_extraction", "timestamp_utc",
    "aqi", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3",
]

POLLUTANT_COLS = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
MIN_CITIES = 5


def validate(file_path: str) -> list[str]:
    """Retourne la liste des erreurs trouvées (liste vide = fichier conforme)."""
    errors = []

    try:
        df = pd.read_csv(file_path)
    except Exception as exc:
        return [f"Impossible de lire le fichier : {exc}"]

    if df.empty:
        return ["Le fichier clean est vide."]

    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        errors.append(f"Colonnes manquantes : {missing_cols}")
        return errors  # inutile de continuer sans les colonnes de base

    # Doublons ville + date + heure
    dup_mask = df.duplicated(subset=["ville", "date_extraction", "heure_extraction"], keep=False)
    if dup_mask.any():
        n_dup = int(dup_mask.sum())
        errors.append(f"{n_dup} ligne(s) en doublon sur (ville, date_extraction, heure_extraction).")

    # Tri chronologique
    sort_key = df["date_extraction"].astype(str) + " " + df["heure_extraction"].astype(str).str.zfill(2)
    if not sort_key.is_monotonic_increasing:
        errors.append("Le fichier n'est pas trié chronologiquement (date_extraction, heure_extraction).")

    # AQI dans [1, 5]
    aqi_valid = df["aqi"].isna() | df["aqi"].between(1, 5)
    if not aqi_valid.all():
        errors.append(f"{(~aqi_valid).sum()} valeur(s) d'AQI hors de l'intervalle [1, 5].")

    # Polluants >= 0
    for col in POLLUTANT_COLS:
        if col in df.columns:
            neg_mask = df[col].notna() & (df[col] < 0)
            if neg_mask.any():
                errors.append(f"{int(neg_mask.sum())} valeur(s) négative(s) sur la colonne '{col}'.")

    # Coordonnées plausibles
    if df["lat"].isna().any() or df["lon"].isna().any():
        errors.append("Des latitudes/longitudes sont manquantes.")
    else:
        if not df["lat"].between(-90, 90).all():
            errors.append("Des latitudes sont hors de [-90, 90].")
        if not df["lon"].between(-180, 180).all():
            errors.append("Des longitudes sont hors de [-180, 180].")

    # Nombre de villes
    n_cities = df["ville"].nunique()
    if n_cities < MIN_CITIES:
        errors.append(f"Seulement {n_cities} ville(s) distincte(s), minimum attendu : {MIN_CITIES}.")

    return errors


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default="data/clean/air_quality_clean.csv",
                         help="Chemin du fichier clean unique à valider")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    found_errors = validate(args.file)

    if not found_errors:
        print(f"OK : {args.file} est conforme au contrat de données.")
        sys.exit(0)
    else:
        print(f"NON CONFORME : {args.file}")
        for e in found_errors:
            print(f"  - {e}")
        sys.exit(1)