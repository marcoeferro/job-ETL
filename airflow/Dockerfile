# airflow/Dockerfile
FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalar dependencias del proyecto
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Instalar src como paquete editable (permite importar desde src.)
COPY --chown=airflow:root . /opt/airflow/project
RUN pip install --no-cache-dir -e /opt/airflow/project

# Variables de entorno por defecto
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow/src:$PYTHONPATH