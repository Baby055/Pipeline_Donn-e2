# ARCHITECTURE.md

## Stack Technique Choisie

**Orchestrateur : Apache Airflow**
Justification : Airflow permet un scheduling fiable 24h/24, un monitoring intégré (historique de runs visible dans l'UI) et une gestion native des backfills, avec une tâche indépendante par ville qui n'interrompt jamais le DAG en cas d'échec ponctuel.

**Stockage : Système de fichiers local (raw/ et clean/)**
Justification : Le stockage local garantit la simplicité de mise en œuvre et l'intégrité des données, avec des fichiers bruts immuables dans raw/ et un fichier clean unique reconstruit à chaque run.

**Base de données : PostgreSQL**
Justification : PostgreSQL supporte efficacement le schéma en étoile et offre une interface SQL standard pour IA1.

## Modélisation Dimensionnelle

**Schéma choisi : Étoile**

- `dim_temps` — dimension temporelle (`date_valeur`, `heure`, `jour_semaine`, `est_weekend`, `mois`, `annee`)
  Justification : permet d'analyser les tendances sur différentes périodes et de distinguer semaine/weekend.
- `dim_ville` — dimension géographique (`ville`, `pays`, `lat`, `lon`)
  Justification : permet de comparer la qualité de l'air entre différentes localisations.
- `fact_qualite_air` — table de faits (`aqi`, `co`, `no`, `no2`, `o3`, `so2`, `pm2_5`, `pm10`, `nh3`) avec clés étrangères `ville_id` et `temps_id` vers les dimensions.
  Justification : stocke uniquement les mesures, aucune colonne descriptive, conforme aux règles de modélisation du cours (pas de mesures dans les dimensions, pas de texte dans les faits).

Justification du schéma en étoile : privilégié pour sa simplicité d'utilisation par IA1, avec des requêtes SQL intuitives et un minimum de jointures. Un schéma en flocon n'apporterait pas de bénéfice réel ici : `dim_ville` reste petite (6 lignes) et stable, normaliser pays/ville en tables séparées ajouterait des jointures sans gain.

DDL complet : `sql/create_star_schema.sql`.

## Stockage et Gestion des Données

Organisation des fichiers :

```
data/
├── raw/                                    # Fichiers JSON bruts, jamais modifiés
│   └── {date}/{heure}/
│       ├── air_quality_paris_2026-07-19_14.json
│       ├── air_quality_tokyo_2026-07-19_14.json
│       └── ...  (6 villes x 24h)
│
└── clean/
    └── air_quality_clean.csv               # Fichier unique, reconstruit à chaque run
```

Un fichier par ville et par heure dans `raw/`, nommé
`air_quality_{ville}_{date}_{heure}.json` (voir `scripts/extract_air_quality.py`).

Justification du stockage raw/clean : la séparation stricte entre raw (sauvegarde immuable) et clean (reconstruit depuis raw à chaque run par `scripts/build_clean_dataset.py`) garantit l'intégrité des données historiques et permet de rejouer tout le processus de transformation à tout moment, sans jamais toucher aux fichiers bruts.

## Flux de Données

```
API OpenWeather Air Pollution (6 villes, horaire)
        │
        ▼  EXTRACT (une tâche Airflow par ville)
   data/raw/{date}/{heure}/air_quality_{ville}_{date}_{heure}.json
        │
        ▼  BUILD CLEAN DATASET (relit TOUT raw/, reconstruit intégralement :
        │   déduplication, filtrage des valeurs aberrantes, tri chronologique)
   data/clean/air_quality_clean.csv
        │
        ▼  VALIDATE (tâche séparée et bloquante : colonnes attendues, pas de
        │   doublons, tri correct, AQI dans [1,5], polluants ≥ 0, ≥ 5 villes —
        │   le run échoue et rien n'est chargé si le fichier n'est pas conforme)
        │
        ▼  LOAD (upsert sur les clés naturelles)
   PostgreSQL — dim_ville, dim_temps, fact_qualite_air
```

Orchestration : Apache Airflow — `air_quality_pipeline_dag` (horaire, `@hourly`) +
`air_quality_backfill_dag` (déclenché manuellement une seule fois, historique de
12 mois — 3 mois minimum accepté par le sujet si le quota API gratuit ou le temps
disponible ne permet pas d'aller plus loin).

## Sécurité

Aucun secret en dur dans le code ni commité. Gestion via variables
d'environnement (ou Connection Airflow pour la base) :

```
OPENWEATHER_API_KEY=votre_cle_api_ici
PG_HOST=localhost
PG_PORT=5432
PG_DB=air_quality
PG_USER=air_quality_user
PG_PASSWORD=votre_mot_de_passe
```

Justification : les variables d'environnement sécurisent la clé API et les
identifiants de connexion en les excluant de l'historique Git. `scripts/load_dwh.py`
échoue explicitement si `PG_PASSWORD` n'est pas fourni (aucun mot de passe par
défaut dans le code), pour éviter qu'un secret ne soit accidentellement commité
comme valeur de repli.

## Cohérence des Données

- Lignes attendues en `clean/` : 6 villes × 24h = 144 lignes/jour de collecte continue.
- Écarts possibles : indisponibilité de l'API, erreurs réseau, rate limiting —
  chaque échec est loggé par `extract_air_quality.py` sans interrompre les
  autres villes ni le DAG.
- Lignes en `fact_qualite_air` : approximativement égal au nombre de lignes de
  `clean/`, chargées par upsert (`ville_id`, `temps_id`) — aucune ligne dupliquée
  en cas de rejeu.
- Backfill (12 mois idéal) : jusqu'à 6 × 24 × ~365 lignes historiques ; en cas de
  limite atteinte à 3 mois minimum, 6 × 24 × ~90 ≈ 12 960 lignes.
- Gestion des manques : logs Airflow détaillés par tâche, retries automatiques
  configurés dans `default_args` de chaque DAG.

## Résumé des Justifications

- **Apache Airflow** : scheduling fiable 24h/24, monitoring intégré, backfill manuel dédié.
- **Stockage local raw/clean** : simplicité, fichiers bruts immuables, clean reconstruit à chaque run.
- **PostgreSQL** : support natif du schéma en étoile, interface SQL standard pour IA1.
- **Schéma en étoile** : requêtes simples, jointures minimales, performances adaptées à l'échelle du projet (6 villes).
- **Validation bloquante** : aucune donnée non conforme n'atteint jamais le DWH.

Version : 1.1