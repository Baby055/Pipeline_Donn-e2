"""
scripts/merge_meteo.py

Fonction de fusion des fichiers CSV individuels (un par ville) en un seul
fichier consolidé meteo_global_{date}.csv.

Comportement :
  - lit tous les fichiers `meteo_*_{date}.csv` présents dans le dossier du jour
  - ignore silencieusement un éventuel fichier meteo_global_*.csv déjà présent
    (pour éviter de se fusionner avec lui-même en cas de relance du DAG)
  - concatène le tout dans un seul DataFrame
  - écrit meteo_global_{date}.csv SANS toucher aux fichiers individuels
"""

import os
import glob
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def merge_files(date: str, data_dir: str) -> str:
    """
    Fusionne tous les fichiers météo individuels d'un dossier en un seul CSV global.

    Args:
        date: date au format YYYY-MM-DD (utilisée pour retrouver et nommer les fichiers)
        data_dir: dossier contenant les fichiers meteo_{ville}_{date}.csv
                  (typiquement data/{date})

    Returns:
        Le chemin du fichier global créé.

    Raises:
        FileNotFoundError: si aucun fichier individuel n'a été trouvé (ce qui
                           signifie que toutes les extractions ont échoué).
    """
    global_filename = f"meteo_global_{date}.csv"
    global_path = os.path.join(data_dir, global_filename)

    pattern = os.path.join(data_dir, f"meteo_*_{date}.csv")
    all_files = sorted(glob.glob(pattern))

    # On exclut explicitement un fichier global déjà existant, pour ne pas
    # le réintégrer dans la fusion (ex: relance manuelle de la tâche merge)
    individual_files = [f for f in all_files if os.path.basename(f) != global_filename]

    if not individual_files:
        msg = f"Aucun fichier météo individuel trouvé pour la date {date} dans {data_dir}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    logger.info("Fichiers trouvés pour la fusion (%s) : %s", date, individual_files)

    dataframes = []
    for file_path in individual_files:
        try:
            df = pd.read_csv(file_path)
            dataframes.append(df)
        except Exception as exc:
            logger.warning("Impossible de lire %s, il sera ignoré : %s", file_path, exc)

    if not dataframes:
        msg = f"Aucun fichier lisible parmi les fichiers trouvés pour la date {date}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    merged_df = pd.concat(dataframes, ignore_index=True)

    # Tri par ville pour un fichier final propre et déterministe
    if "ville" in merged_df.columns:
        merged_df = merged_df.sort_values(by="ville").reset_index(drop=True)

    merged_df.to_csv(global_path, index=False, encoding="utf-8")
    logger.info(
        "Fusion réussie : %s lignes écrites dans %s", len(merged_df), global_path
    )

    return global_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    merge_files(date="2025-05-18", data_dir="./data/2025-05-18")
