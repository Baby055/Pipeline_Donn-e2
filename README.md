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
├── .github/
│   └── workflows/
│       ├── pipeline_hourly.yml        # workflow horaire : extraction -> clean -> validation -> DWH
│       └── backfill.yml               # workflow manuel : backfill historique
├── scripts/
│   ├── extract_air_quality.py         # fonction d'extraction API -> raw/ (JSON, jamais modifié)
│   ├── run_hourly_extraction.py       # runner horaire, appelé par pipeline_hourly.yml
│   ├── run_backfill.py                # runner de backfill, appelé par backfill.yml
│   ├── build_clean_dataset.py         # reconstruit clean/air_quality_clean.csv depuis raw/
│   ├── validate_clean.py              # valide le contrat de données sur clean/
│   └── load_dwh.py                    # charge clean/ dans le schéma en étoile (Neon)
├── sql/
│   └── create_star_schema.sql         # DDL : dim_ville, dim_temps, fact_qualite_air
├── data/
│   ├── raw/{date}/{heure}/            # zone intouchable, un JSON par ville et par appel
│   └── clean/air_quality_clean.csv    # fichier unique, reconstruit à chaque run
├── requirements.txt                   # dépendances pour tests locaux
└── requirements-actions.txt           # dépendances minimales utilisées par les workflows
```

## Zone raw/

Un fichier JSON par ville et par appel : `air_quality_{ville}_{date}_{heure}.json`.
Contient la réponse brute de l'API OpenWeather Air Pollution, enveloppée avec
les métadonnées d'extraction (`ville`, `pays`, `lat`, `lon`, `date_extraction`,
`heure_extraction`, `timestamp_utc`, `source`). **Jamais modifié après écriture**
— c'est la source de vérité à partir de laquelle `clean/` est intégralement
reconstruit à chaque run. Comme les runners GitHub Actions s'exécutent sur des
machines éphémères, `raw/` est commité dans le dépôt Git à la fin de chaque run
pour rester disponible au run suivant.

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
minimum de villes avant toute livraison. Cette étape est intégrée et bloquante
dans les deux workflows GitHub Actions.

## Data Warehouse

PostgreSQL managé (Neon), modélisation en étoile (voir `ARCHITECTURE.md` pour la
justification étoile vs flocon).

**`dim_ville`** — `ville_id` (PK), `ville`, `pays`, `lat`, `lon`
**`dim_temps`** — `temps_id` (PK), `date_valeur`, `heure`, `jour_semaine` (1=lundi..7=dimanche, ISO), `est_weekend` (booléen), `mois`, `annee`
**`fact_qualite_air`** — `fact_id` (PK), `ville_id` (FK), `temps_id` (FK), `aqi`, `co`, `no`, `no2`, `o3`, `so2`, `pm2_5`, `pm10`, `nh3`, `charge_le`

Aucune mesure dans les dimensions, aucune colonne descriptive dans la table de
faits (conforme aux règles de modélisation du cours).

Chargement : `python scripts/load_dwh.py` (rejouable — upsert sur les clés
naturelles, ne duplique jamais une ligne pour un même (ville, date, heure)).
Exécuté automatiquement à la fin de chaque run des workflows GitHub Actions.

**Cohérence attendue** : nombre de lignes de `fact_qualite_air` ≈ nombre de
villes (6) × nombre d'heures couvertes par la période de collecte. Les écarts
proviennent des extractions horaires échouées (panne API ponctuelle, quota
dépassé) — chaque échec est loggé par `extract_air_quality.py` et n'interrompt
jamais le run pour les autres villes.

## Période couverte et trous connus

> À compléter par le groupe une fois le backfill lancé et le pipeline horaire
> stabilisé sur plusieurs jours :
> - Période du backfill historique effectivement obtenue (12 mois idéal / 3 mois minimum)
> - Date de démarrage de la collecte horaire en continu
> - Heures ou jours avec des données manquantes identifiées (ex : panne API, quota dépassé) et pourquoi

## Connexion au Data Warehouse

> À compléter par le groupe avec les identifiants de connexion réels de
> l'instance Neon (host exact, utilisateur en lecture seule pour IA1) — ne
> jamais commiter le mot de passe, le donner par un canal séparé (ex :
> formulaire de rendu, message privé au correcteur).

```
Host     : <à compléter, ex: ep-xxxx-pooler.c-10.us-east-1.aws.neon.tech>
Port     : 5432
Database : neondb
SSL mode : require
Utilisateur (lecture seule recommandé pour IA1) : air_quality_readonly
```

## Installation locale (test avant déploiement)

```bash
pip install -r requirements.txt

export OPENWEATHER_API_KEY="votre_cle"
export PG_HOST="ep-xxxx-pooler.c-10.us-east-1.aws.neon.tech"
export PG_PORT=5432
export PG_DB="neondb"
export PG_USER="neondb_owner"
export PG_PASSWORD="votre_mot_de_passe"
export PG_SSLMODE="require"

# Une seule fois : créer le schéma sur Neon
psql "postgresql://$PG_USER:$PG_PASSWORD@$PG_HOST/$PG_DB?sslmode=require" \
  -f sql/create_star_schema.sql

# Tester l'extraction horaire en local
python scripts/run_hourly_extraction.py
python scripts/build_clean_dataset.py
python scripts/validate_clean.py --file data/clean/air_quality_clean.csv
python scripts/load_dwh.py

# Tester le backfill en local (ex: 1 mois pour un test rapide)
python scripts/run_backfill.py --months 1
```

## Déploiement en production (GitHub Actions)

1. Ajouter les secrets du dépôt : Settings > Secrets and variables > Actions
   — `OPENWEATHER_API_KEY`, `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`
2. Activer les permissions d'écriture : Settings > Actions > General >
   Workflow permissions > **Read and write permissions**
3. Le workflow `.github/workflows/pipeline_hourly.yml` se déclenche
   automatiquement toutes les heures dès qu'il est présent sur la branche
   par défaut — rien d'autre à activer
4. Déclencher une fois `.github/workflows/backfill.yml` manuellement
   (onglet Actions > Backfill historique Qualité de l'Air > Run workflow)
   pour charger l'historique

**Preuve d'exécution automatique** : onglet **Actions** du dépôt GitHub,
historique des runs de `pipeline_hourly.yml` sur plusieurs jours.