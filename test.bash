# 1. Crea una carpeta vacía para el proyecto
mkdir airflow-getonbrd && cd airflow-getonbrd

# 2. Descarga el docker-compose oficial de Airflow (SIEMPRE usa este link)
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# 3. Crea las carpetas que Airflow necesita
mkdir -p ./dags ./logs ./plugins ./config

# 4. Crea un archivo de variables de entorno (para que no pida contraseña)
echo -e "AIRFLOW_UID=$(id -u)" > .env

# 5. Levanta Airflow (la primera vez tarda ~3-5 min en descargar todo)
sudo docker compose up airflow-init   # ← solo la primera vez

sudo docker compose up -d             # ← levanta todo (webserver, scheduler, etc.)


