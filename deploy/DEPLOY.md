# DEPLOY.md — Déploiement sur Oracle Cloud Free Tier (Docker Compose)

Ce guide déploie Airflow (LocalExecutor) + PostgreSQL en continu sur une VM
"Always Free" d'Oracle Cloud Infrastructure (OCI), avec Docker Compose.
Adaptable tel quel à AWS/GCP free tier (seule l'étape 1 change).

## 0. Où placer ces fichiers dans le repo

Copiez le contenu de ce dossier `deploy/` à la racine de votre repo, à côté de
`dags/`, `scripts/`, `sql/` :

```
Pipeline_Donn-e2/
├── dags/
├── scripts/
├── sql/
├── data/                     # créé au premier run (raw/, clean/)
├── docker-compose.yml
├── Dockerfile
├── requirements-extra.txt
├── .env.example              # commité (sans valeurs)
├── .env                      # NE JAMAIS COMMITER — ajoutez-le au .gitignore
└── DEPLOY.md
```

Ajoutez `.env` et `data/` à votre `.gitignore` si `data/` devient volumineux
(le sujet exige le backfill dans `raw/`, mais un `.gitignore` trop strict qui
exclurait `raw/` serait un livrable invérifiable — vérifiez que `data/raw/`
reste bien versionné ou fourni autrement, seul `.env` doit être exclu).

## 1. Créer la VM Oracle Cloud Free Tier

1. Créer un compte OCI (carte bancaire demandée mais jamais débitée sur le tier gratuit).
2. **Compute > Instances > Create Instance**
   - Image : Ubuntu 22.04 (Canonical Ubuntu)
   - Forme (Shape) : `VM.Standard.A1.Flex` (Ampere ARM, jusqu'à 4 OCPU / 24 Go RAM gratuits) ou `VM.Standard.E2.1.Micro` (x86, plus limité mais suffisant ici)
   - Clé SSH : générer une paire ou fournir votre clé publique
3. **Réseau (VCN) > Security Lists** : ajouter des règles d'entrée (Ingress Rules) :
   - TCP port `22` (SSH) — source : votre IP si possible, sinon `0.0.0.0/0`
   - TCP port `8080` (UI Airflow) — source : votre IP ou celle de l'équipe/du correcteur
   - TCP port `5432` (PostgreSQL) — **restreindre à l'IP d'IA1/du correcteur**, ne pas ouvrir à `0.0.0.0/0` si évitable
4. Noter l'IP publique de la VM.

## 2. Installer Docker sur la VM

```bash
ssh ubuntu@<IP_PUBLIQUE>

# Ubuntu pare-feu interne (iptables) : autoriser les mêmes ports que la Security List
sudo iptables -I INPUT -p tcp --dport 8080 -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 5432 -j ACCEPT
sudo netfilter-persistent save 2>/dev/null || true

# Docker + plugin compose
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
newgrp docker

sudo systemctl enable docker      # Docker démarre automatiquement après reboot
sudo systemctl start docker
docker compose version            # doit afficher une version (plugin v2)
```

## 3. Cloner le repo et configurer les secrets

```bash
git clone <url_de_votre_repo>.git
cd Pipeline_Donn-e2

cp deploy/.env.example .env   # adapter le chemin si deploy/ a été fusionné à la racine
nano .env
```

Renseigner dans `.env` :
- `OPENWEATHER_API_KEY` (votre clé, jamais commitée)
- Tous les mots de passe (génération : `openssl rand -base64 24` pour chacun)
- `AIRFLOW_UID` : résultat de `id -u` sur la VM (évite les soucis de permissions sur `data/`)

## 4. Démarrer la stack

```bash
docker compose up -d --build
docker compose ps        # tous les services doivent être "Up" / "healthy"
docker compose logs -f airflow-init   # vérifier que la migration + la Variable API key sont OK
```

Ouvrir `http://<IP_PUBLIQUE>:8080` dans un navigateur, se connecter avec
`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`.

## 5. Activer le pipeline

Dans l'UI Airflow :
1. Activer (toggle) `air_quality_pipeline_dag` — il tourne alors `@hourly`, 24h/24, tant que la VM et Docker tournent.
2. Déclencher manuellement `air_quality_backfill_dag` une seule fois (bouton ▶) pour charger l'historique (12 mois idéal / 3 mois minimum).
3. Vérifier dans **Runs** que les tâches passent au vert, et que `data/clean/air_quality_clean.csv` est bien mis à jour (`docker compose exec airflow-scheduler ls -la /opt/airflow/data/clean/`).

## 6. Vérifier le warehouse

```bash
docker compose exec postgres psql -U air_quality_loader -d air_quality \
  -c "SELECT COUNT(*) FROM fact_qualite_air;"
```

Depuis votre machine locale (une fois le port 5432 ouvert dans la Security List) :

```bash
psql "host=<IP_PUBLIQUE> port=5432 dbname=air_quality user=air_quality_readonly password=<mot_de_passe>" \
  -c "SELECT v.ville, COUNT(*) FROM fact_qualite_air f JOIN dim_ville v USING(ville_id) GROUP BY v.ville;"
```

## 7. Résilience après redémarrage de la VM

`restart: unless-stopped` dans `docker-compose.yml` + `docker` activé au boot
(étape 2) suffisent : si la VM redémarre (maintenance OCI, coupure), Docker
redémarre automatiquement et relance tous les conteneurs sans intervention.
Aucun service systemd supplémentaire n'est nécessaire.

Pour vérifier que ça survit vraiment à un reboot :

```bash
sudo reboot
# attendre ~1 min, puis :
ssh ubuntu@<IP_PUBLIQUE> "cd Pipeline_Donn-e2 && docker compose ps"
```

## 8. Preuve d'exécution automatique (livrable demandé)

Une fois le pipeline stable plusieurs jours : dans l'UI Airflow, onglet
**Runs** du DAG `air_quality_pipeline_dag`, faire une capture d'écran montrant
plusieurs runs verts sur au moins 5 jours différents, à des heures creuses
(nuit) — c'est la preuve exigée par le sujet.

## 9. Informations à reporter dans README.md (section "Connexion au DWH")

```
Host     : <IP_PUBLIQUE>
Port     : 5432
Database : air_quality
Utilisateur (lecture seule) : air_quality_readonly
```

Le mot de passe de `air_quality_readonly` doit être transmis à IA1/au
correcteur par un canal séparé (formulaire de rendu, message privé) —
jamais commité, jamais dans le README.

## Dépannage rapide

| Symptôme | Cause probable | Solution |
|---|---|---|
| `airflow-webserver` redémarre en boucle | `AIRFLOW_ADMIN_PASSWORD` vide ou DB pas migrée | `docker compose logs airflow-init` |
| Connexion refusée sur :8080 depuis l'extérieur | Security List OCI ou iptables ne laisse pas passer le port | Revoir étape 1 (Security List) et étape 2 (iptables) |
| `psql` externe échoue en connexion sur :5432 | Port fermé côté OCI, ou `pg_hba.conf` par défaut de l'image postgres refuse les IP externes | Ouvrir le port dans la Security List ; l'image officielle postgres autorise déjà `0.0.0.0/0` par défaut dans `pg_hba.conf`, donc le blocage vient presque toujours du pare-feu réseau |
| Tâche `extract_*` échoue systématiquement | Clé API absente/invalide, quota dépassé | `docker compose exec airflow-scheduler airflow variables get OPENWEATHER_API_KEY` |
