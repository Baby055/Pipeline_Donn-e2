# Pipeline Qualité de l'Air — README stockage

Pipeline de données en production pour la qualité de l'air de 6 villes,
collectée toutes les heures, nettoyée en un fichier unique et chargée dans
un entrepôt de données en étoile (Bloc 1, projet de groupe).

> Voir `ARCHITECTURE.md` pour la stack complète et sa justification, et
> le Rapport de projet pour la répartition des tâches et les difficultés
> rencontrées.

## Villes suivies

| Ville | Pays | Latitude | Longitude |
|---|---|---|---|
| Paris | FR | 48.8566 | 2.3522 |
| Tokyo | JP | 35.6762 | 139.6503 |
| New York | US | 40.7128 | -74.0060 |
| Antananarivo | MG | -18.8792 | 47.5079 |
| Sydney | AU | -33.8688 | 151.2093 |
| Cairo | EG | 30.0444 | 31.2357 |

(6 villes ≥ 5 minimum demandé par le sujet. Coordonnées définies dans
`scripts/extract_air_quality.py::CITY_COORDS`.)

## Structure du dépôt

```
Pipeline_Donn-e2/
├── ARCHITECTURE.md
├── README.md                          # ce fichier
├── dags/
│   ├── air_quality_pipeline_dag.py    # DAG horaire : extraction -> clean -> validation -> DWH
│   └── air_quality_backfill_dag.py    # DAG manuel : backfill historique
├── scripts/
│   ├── extract_air_quality.py         # extraction API -> raw/ (JSON, jamais modifié)
│   ├── build_clean_dataset.py         # reconstruit clean/air_quality_clean.csv depuis raw/
│   ├── validate_clean.py              # valide le contrat de données sur clean/
│   └── load_dwh.py                    # charge clean/ dans le schéma en étoile
├── sql/
│   └── create_star_schema.sql         # DDL : dim_ville, dim_temps, fact_qualite_air
├── data/
│   ├── raw/{date}/{heure}/            # zone intouchable, un JSON par ville et par appel
│   └── clean/air_quality_clean.csv    # fichier unique, reconstruit à chaque run
└── requirements.txt
```

## Zone raw/

Un fichier JSON par ville et par appel : `air_quality_{ville}_{date}_{heure}.json`.
Contient la réponse brute de l'API OpenWeather Air Pollution, enveloppée avec
les métadonnées d'extraction (`ville`, `pays`, `lat`, `lon`, `date_extraction`,
`heure_extraction`, `timestamp_utc`, `source`). **Jamais modifié après écriture**
— c'est la source de vérité à partir de laquelle `clean/` est intégralement
reconstruit à chaque run.

## Zone clean/ — contrat de données

Fichier unique : `data/clean/air_quality_clean.csv`. Une ligne par (ville, date,
heure), triée chronologiquement, sans doublon (en cas de double extraction pour
la même heure, la plus récente est conservée). Reconstruit en entier à chaque
exécution de `build_clean_dataset.py` — jamais d'append.

| Colonne | Type | Unité / plage | Description |
|---|---|---|---|
| `ville` | texte | — | Nom de la ville (voir tableau ci-dessus) |
| `pays` | texte | code ISO 2 lettres | Code pays |
| `lat` | numérique | degrés décimaux, [-90, 90] | Latitude |
| `lon` | numérique | degrés décimaux, [-180, 180] | Longitude |
| `date_extraction` | date | AAAA-MM-JJ | Date UTC de la mesure |
| `heure_extraction` | entier | 00-23 | Heure UTC de la mesure |
| `timestamp_utc` | texte ISO 8601 | — | Horodatage précis de l'extraction |
| `aqi` | entier | 1 (bon) à 5 (très mauvais) | Indice de qualité de l'air OpenWeather |
| `co` | numérique | µg/m³ | Monoxyde de carbone |
| `no` | numérique | µg/m³ | Monoxyde d'azote |
| `no2` | numérique | µg/m³ | Dioxyde d'azote |
| `o3` | numérique | µg/m³ | Ozone |
| `so2` | numérique | µg/m³ | Dioxyde de soufre |
| `pm2_5` | numérique | µg/m³ | Particules fines ≤ 2.5 µm |
| `pm10` | numérique | µg/m³ | Particules fines ≤ 10 µm |
| `nh3` | numérique | µg/m³ | Ammoniac |

Validation : `python scripts/validate_clean.py --file data/clean/air_quality_clean.csv`
vérifie colonnes, doublons, tri chronologique, plages de valeurs et nombre
minimum de villes avant toute livraison.

## Data Warehouse

PostgreSQL, modélisation en étoile (voir `ARCHITECTURE.md` pour la
justification étoile vs flocon).

**`dim_ville`** — `ville_id` (PK), `ville`, `pays`, `lat`, `lon`
**`dim_temps`** — `temps_id` (PK), `date_valeur`, `heure`, `jour_semaine` (1=lundi..7=dimanche, ISO), `est_weekend` (booléen), `mois`, `annee`
**`fact_qualite_air`** — `fact_id` (PK), `ville_id` (FK), `temps_id` (FK), `aqi`, `co`, `no`, `no2`, `o3`, `so2`, `pm2_5`, `pm10`, `nh3`, `charge_le`

Aucune mesure dans les dimensions, aucune colonne descriptive dans la table de
faits (conforme aux règles de modélisation du cours).

Chargement : `python scripts/load_dwh.py` (rejouable — upsert sur les clés
naturelles, ne duplique jamais une ligne pour un même (ville, date, heure)).

**Cohérence attendue** : nombre de lignes de `fact_qualite_air` ≈ nombre de
villes (6) × nombre d'heures couvertes par la période de collecte. Les écarts
proviennent des extractions horaires échouées (panne API ponctuelle, quota
dépassé) — chaque échec est loggé par `extract_air_quality.py` et n'interrompt
jamais le DAG pour les autres villes.

## Période couverte et trous connus

> À compléter par le groupe une fois le pipeline déployé et le backfill lancé :
> - Période du backfill historique effectivement obtenue (12 mois idéal / 3 mois minimum)
> - Date de démarrage de la collecte horaire en continu
> - Heures ou jours avec des données manquantes identifiées (ex : panne serveur, quota API dépassé) et pourquoi

## Connexion au Data Warehouse

> À compléter par le groupe avec les identifiants de connexion réels du
> serveur de déploiement (host, port, nom de base, utilisateur en lecture
> seule pour IA1) — ne jamais commiter le mot de passe, le donner par un
> canal séparé (ex : formulaire de rendu, message privé au correcteur).

```
Host     : <à compléter>
Port     : <à compléter, 5432 par défaut>
Database : air_quality
Utilisateur (lecture seule recommandé pour IA1) : <à compléter>
```

## Installation locale (test avant déploiement)

```bash
export AIRFLOW_HOME=~/Pipeline_Donn-e2   # dags/ et scripts/ au même niveau
pip install -r requirements.txt

export OPENWEATHER_API_KEY="votre_cle"
export PG_HOST=localhost PG_PORT=5432 PG_DB=air_quality \
       PG_USER=air_quality_user PG_PASSWORD="votre_mot_de_passe"

psql -h localhost -U air_quality_user -d air_quality -f sql/create_star_schema.sql

airflow standalone
```

Dans l'UI (`localhost:8080`), activer `air_quality_pipeline_dag`, déclencher
un run manuel, vérifier que `data/clean/air_quality_clean.csv` se met à jour
et que `SELECT * FROM fact_qualite_air LIMIT 10;` renvoie des lignes.

Puis déclencher une fois `air_quality_backfill_dag` pour charger l'historique.

**Rappel** : `airflow standalone` est réservé aux tests locaux (SQLite,
un seul process). Pour la collecte 24h/24 exigée par le sujet, Airflow doit
tourner en service persistant sur le serveur de déploiement, pas en standalone.