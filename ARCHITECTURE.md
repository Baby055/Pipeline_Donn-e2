# ARCHITECTURE.md

## Stack Technique Choisie

**Orchestrateur : GitHub Actions**
Justification : GitHub Actions permet un scheduling fiable 24h/24 via `cron`, sans nécessiter de serveur dédié à gérer par le groupe (pas de carte bancaire requise, contrairement aux VM cloud classiques), avec un monitoring intégré (historique de runs visible directement dans l'onglet Actions du dépôt) et un déclenchement manuel natif (`workflow_dispatch`) pour le backfill. Chaque étape du pipeline (extraction, nettoyage, validation, chargement) reste du code Python versionné, exécuté par un workflow YAML lui-même versionné dans le dépôt — conforme à la règle du sujet sur les outils no-code/orchestrateurs externes.

**Stockage : Système de fichiers du dépôt Git (raw/ et clean/)**
Justification : Comme les exécutions GitHub Actions sont éphémères (une machine neuve à chaque run), `data/raw/` et `data/clean/` sont commités dans le dépôt à la fin de chaque run — le dépôt Git devient lui-même la zone de stockage persistante, avec un historique de versions natif (utile pour retracer l'évolution du fichier clean dans le temps).

**Base de données : PostgreSQL managé (Neon, serverless)**
Justification : Neon fournit un PostgreSQL toujours accessible depuis l'extérieur avec un hostname stable, sans carte bancaire ni serveur à administrer par le groupe. Cela résout à la fois l'hébergement du warehouse pour IA1 et l'accès depuis Power BI, sans dépendre de la disponibilité d'une VM personnelle.

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

## Scripts du Pipeline

| Script | Rôle |
|--------|------|
| `scripts/extract_air_quality.py` | Fonction d'extraction pour une ville (appelée par les runners ci-dessous) |
| `scripts/run_hourly_extraction.py` | Runner horaire : boucle sur toutes les villes, écrit dans `raw/` (appelé par le workflow `pipeline_hourly.yml`) |
| `scripts/run_backfill.py` | Runner de backfill : historique mois par mois, toutes villes (appelé par le workflow `backfill.yml`) |
| `scripts/build_clean_dataset.py` | Reconstruit `clean/air_quality_clean.csv` depuis l'intégralité de `raw/` |
| `scripts/validate_clean.py` | Valide le fichier clean (colonnes, doublons, valeurs, tri, nombre de villes) |
| `scripts/load_dwh.py` | Charge le fichier clean validé dans le schéma en étoile PostgreSQL (Neon) |

## Stockage et Gestion des Données

Organisation des fichiers dans le dépôt :

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

Justification du stockage raw/clean : la séparation stricte entre raw (sauvegarde immuable) et clean (reconstruit depuis raw à chaque run par `scripts/build_clean_dataset.py`) garantit l'intégrité des données historiques et permet de rejouer tout le processus de transformation à tout moment, sans jamais toucher aux fichiers bruts. Chaque run GitHub Actions récupère l'état actuel de `raw/` via `git checkout`, ajoute ses nouveaux fichiers, puis recommite `raw/` et `clean/` — garantissant la continuité entre deux exécutions malgré des machines éphémères.

## Flux de Données

```
API OpenWeather Air Pollution (6 villes, horaire)
        │
        ▼  EXTRACT (run_hourly_extraction.py, une ville après l'autre)
   data/raw/{date}/{heure}/air_quality_{ville}_{date}_{heure}.json
        │
        ▼  BUILD CLEAN DATASET (relit TOUT raw/, reconstruit intégralement :
        │   déduplication, filtrage des valeurs aberrantes, tri chronologique)
   data/clean/air_quality_clean.csv
        │
        ▼  VALIDATE (étape séparée et bloquante : colonnes attendues, pas de
        │   doublons, tri correct, AQI dans [1,5], polluants ≥ 0, ≥ 5 villes —
        │   le workflow échoue et rien n'est chargé si le fichier n'est pas conforme)
        │
        ▼  LOAD (upsert sur les clés naturelles)
   PostgreSQL (Neon) — dim_ville, dim_temps, fact_qualite_air
        │
        ▼  COMMIT
   git add data/raw data/clean && git commit && git push
```

Orchestration : GitHub Actions — `.github/workflows/pipeline_hourly.yml` (`cron: '0 * * * *'`, toutes les heures) +
`.github/workflows/backfill.yml` (déclenché manuellement une seule fois via `workflow_dispatch`, historique de
12 mois — 3 mois minimum accepté par le sujet si le quota API gratuit ou le temps disponible ne permet pas d'aller plus loin).

## Sécurité

Aucun secret en dur dans le code ni commité. Gestion via les **Secrets** du dépôt GitHub
(Settings > Secrets and variables > Actions), injectés comme variables d'environnement
au moment de l'exécution du workflow :

```
OPENWEATHER_API_KEY   (clé API OpenWeather)
PG_HOST                (host du pooler Neon)
PG_PORT                (5432)
PG_DB                  (neondb)
PG_USER                (neondb_owner)
PG_PASSWORD            (mot de passe Neon)
```

Justification : les secrets GitHub Actions sont chiffrés au repos, jamais visibles dans les logs
de run (masqués automatiquement), et exclus de l'historique Git. `scripts/load_dwh.py`
échoue explicitement si `PG_PASSWORD` n'est pas fourni (aucun mot de passe par
défaut dans le code), pour éviter qu'un secret ne soit accidentellement commité
comme valeur de repli. La connexion à Neon utilise SSL obligatoire (`sslmode=require`).

Un utilisateur PostgreSQL séparé en lecture seule (`air_quality_readonly`) est créé
manuellement sur Neon, distinct de `neondb_owner` (droits complets), pour l'accès
donné à IA1 et à Power BI.

## Cohérence des Données

- Lignes attendues en `clean/` : 6 villes × 24h = 144 lignes/jour de collecte continue.
- Écarts possibles : indisponibilité de l'API, erreurs réseau, rate limiting —
  chaque échec est loggé par `extract_air_quality.py` sans interrompre les
  autres villes ni le run.
- Lignes en `fact_qualite_air` : approximativement égal au nombre de lignes de
  `clean/`, chargées par upsert (`ville_id`, `temps_id`) — aucune ligne dupliquée
  en cas de rejeu.
- Backfill (12 mois idéal) : jusqu'à 6 × 24 × ~365 lignes historiques ; en cas de
  limite atteinte à 3 mois minimum, 6 × 24 × ~90 ≈ 12 960 lignes.
- Gestion des manques : logs détaillés par étape dans chaque run GitHub Actions,
  consultables dans l'onglet **Actions** du dépôt.

## Résumé des Justifications

- **GitHub Actions** : scheduling fiable 24h/24 par cron, monitoring intégré (onglet Actions),
  déclenchement manuel dédié pour le backfill, aucun serveur ni carte bancaire à gérer par le groupe.
- **Stockage Git (raw/clean)** : simplicité, fichiers bruts immuables, historique de versions natif,
  clean reconstruit à chaque run.
- **Neon (PostgreSQL managé)** : support natif du schéma en étoile, interface SQL standard pour IA1,
  hostname stable accessible sans configuration réseau côté groupe.
- **Schéma en étoile** : requêtes simples, jointures minimales, performances adaptées à l'échelle du projet (6 villes).
- **Validation bloquante** : aucune donnée non conforme n'atteint jamais le DWH.

Version : 2.0 (migration Airflow/Docker/VM → GitHub Actions/Neon)