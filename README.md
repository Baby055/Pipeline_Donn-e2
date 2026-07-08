# Pipeline_Donn-e2
A repository for Exam Donneé 2
STD24063
STD24095
STD24056
STD24111
STD24163

##Structure

Pipeline_Donn-e2/
├── dags/
│   └── weather_pipeline_dag.py    # DAG (6 tâches d'extraction + 1 fusion)
├── scripts/
│   ├── extract_meteo.py           # extract_meteo(city_name, ...)
│   └── merge_meteo.py             # merge_files(date, data_dir)
├── data/
│   └── {YYYY-MM-DD}/
│       ├── meteo_paris_{date}.csv
│       ├── meteo_tokyo_{date}.csv
│       ├── ...
│       └── meteo_global_{date}.csv
├── requirements.txt
└── README.md
 
## ## Installation

1. Copier `dags/` et `scripts/` dans votre `AIRFLOW_HOME` (ou pointer
   `AIRFLOW_HOME/dags` vers ce dossier `dags/`, en gardant `scripts/` au
   même niveau que `dags/`, comme dans cette arborescence).

2. Installer les dépendances :
   ```bash
   pip install -r requirements.txt
   ```

3. Créer une clé API gratuite sur https://openweathermap.org/api puis
   l'enregistrer comme **Variable Airflow** :
   ```bash

