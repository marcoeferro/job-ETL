# src/config/settings.py
from pathlib import Path

# Paths del proyecto
BASE_DIR = Path(__file__).parent.parent.parent

DATA_RAW_DIR = BASE_DIR / "data" / "raw"
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"
DATA_INTERIM_DIR = BASE_DIR / "data" / "interim"

# Configuración de scraping
DEFAULT_SEARCH_QUERY = "data analyst OR data scientist OR ingeniero de datos"
DEFAULT_LOCATION = "Argentina"
DEFAULT_MAX_PAGES = 5

# Configuración Airflow / ejecución
DEFAULT_DS_FORMAT = "%Y-%m-%d"

# NLP config (para más adelante)
SPACY_MODEL = "es_core_news_md"   # o "es_core_news_lg"
HF_MODEL_SENTIMENT = "nlptown/bert-base-multilingual-uncased-sentiment"

# Otras configuraciones
LOG_LEVEL = "INFO"