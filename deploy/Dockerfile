# Image Airflow officielle + dépendances du pipeline (requests, pandas).
# psycopg2-binary est déjà fourni par apache-airflow-providers-postgres,
# préinstallé dans l'image de base.
FROM apache/airflow:2.10.4-python3.11

COPY requirements-extra.txt /requirements-extra.txt
RUN pip install --no-cache-dir -r /requirements-extra.txt
