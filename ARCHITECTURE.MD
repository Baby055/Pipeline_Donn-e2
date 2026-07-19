
Stack Technique Choisie

Orchestrateur : Apache Airflow
Justification : Airflow permet un scheduling fiable 24h/24, un monitoring intégré et une gestion native des backfills de 3 mois.

Stockage : Système de fichiers local (raw/ et clean/)
Justification : Le stockage local garantit la simplicité de mise en œuvre et l'intégrité des données avec des fichiers bruts immuables et un clean reconstruit à chaque run.

Base de données : PostgreSQL
Justification : PostgreSQL supporte efficacement le schéma en étoile et offre une interface SQL standard pour IA1.

Modélisation Dimensionnelle

Schéma choisi : Étoile

dim_time : Dimension temporelle (date, heure, jour de semaine, weekend)
Justification : Permet d'analyser les tendances sur différentes périodes.

dim_city : Dimension géographique (ville, pays, coordonnées GPS)
Justification : Permet de comparer la qualité de l'air entre différentes localisations.

fact_air_quality : Table de faits (AQI, CO, NO, NO2, O3, SO2, PM2.5, PM10, NH3)
Justification : Stocke les mesures avec clés étrangères vers les dimensions.

Justification du schéma en étoile : Ce schéma est privilégié pour sa simplicité d'utilisation par IA1, car il permet des requêtes SQL intuitives avec un minimum de jointures.

Stockage et Gestion des Données

Organisation des fichiers :

data/
├── raw/                          # Fichiers JSON bruts, jamais modifiés
│   ├── Paris_20260119_120000.json
│   ├── London_20260119_120000.json
│   └── ... (6 villes x 24h)
│
└── clean/
    └── air_quality_clean.csv     # Unique fichier, reconstruit à chaque run

Justification du stockage raw/clean : La séparation stricte entre raw (sauvegarde immuable) et clean (reconstruit depuis raw) garantit l'intégrité des données historiques et permet de rejouer tout le processus de transformation.

Flux de Données

API OpenWeatherMap (6 villes, horaire) -> EXTRACT -> raw/
raw/ -> TRANSFORM (déduplication, tri, validation) -> clean/ -> LOAD -> PostgreSQL (dim_time, dim_city, fact_air_quality)

Orchestration : Apache Airflow (DAG horaire + backfill 90 jours)

Sécurité

Gestion des secrets via fichier .env (ignoré par Git) :
OPENWEATHER_API_KEY=votre_cle_api_ici
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/air_quality

Justification : Les variables d'environnement sécurisent les clés API en les excluant de l'historique Git.

Cohérence des Données

Lignes en clean/ : 6 villes x 24h = 144 lignes/jour
Écarts possibles : Indisponibilité de l'API, erreurs réseau, rate limiting.

Lignes en fact_air_quality : environ égal au nombre de lignes en clean/
Écarts gérés par : Déduplication (même ville + même heure), logs Airflow, retry automatique (3 tentatives).

Backfill 3 mois : 6 x 24 x 90 = 12 960 lignes
Gestion des manques : Logs et retry automatique.

Résumé des Justifications

Apache Airflow : Scheduling fiable 24h/24, monitoring intégré, backfills natifs.
Stockage local : Simplicité, fichiers bruts immuables, clean reconstruit à chaque run.
PostgreSQL : Support du schéma en étoile, interface SQL standard.
Schéma en étoile : Requêtes simples, jointures minimales, performances optimales.

Version : 1.0