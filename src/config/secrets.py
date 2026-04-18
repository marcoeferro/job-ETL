# src/config/secrets.py
"""
Manejo de secretos y credenciales.
Nunca commitear este archivo con valores reales.
Usar variables de entorno (.env)
"""
import os
from dotenv import load_dotenv

load_dotenv()  # carga .env si existe

# Ejemplo: proxies o API keys si las agregás después
PROXY_LIST = os.getenv("PROXY_LIST", "").split(",") if os.getenv("PROXY_LIST") else None

# User-Agent adicional si querés configurarlo
CUSTOM_USER_AGENT = os.getenv("CUSTOM_USER_AGENT")

# Para Airflow / Streamlit (si usás variables sensibles)
AIRFLOW__CORE__SQL_ALCHEMY_CONN = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")