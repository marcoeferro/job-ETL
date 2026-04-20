# src/config/settings.py
from pathlib import Path
from typing import Dict

# ==================== PATHS DEL PROYECTO ====================
BASE_DIR = Path(__file__).parent.parent.parent

DATA_RAW_DIR = BASE_DIR / "data" / "raw"
DATA_INTERIM_DIR = BASE_DIR / "data" / "interim"
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"

# ==================== CONFIGURACIÓN GENERAL ====================
DEFAULT_SEARCH_QUERY = "data analyst OR data scientist OR ingeniero de datos OR 'data engineer'"
DEFAULT_LOCATION = "Argentina"
DEFAULT_MAX_PAGES = 5

# Configuración Airflow / ejecución
DEFAULT_DS_FORMAT = "%Y-%m-%d"

# NLP (para más adelante)
SPACY_MODEL = "es_core_news_md"
HF_MODEL_SENTIMENT = "nlptown/bert-base-multilingual-uncased-sentiment"

# Logging
LOG_LEVEL = "INFO"

# ==================== URLs BASE DE LOS SCRAPERS ====================
SCRAPER_BASE_URLS: Dict[str, str] = {
    "computrabajo": "https://www.computrabajo.com.ar",
    "indeed": "https://ar.indeed.com",
    "portal_empleo": "https://portalempleo.gob.ar",        # Portal Empleo Nacional
    "zonajobs": "https://www.zonajobs.com.ar",
    "getonboard": "https://www.getonbrd.com/",
}

# ==================== CONFIGURACIÓN ESPECÍFICA POR SITIO ====================
SCRAPER_CONFIG: Dict[str, Dict] = {
    "computrabajo": {
        "rate_limit": 1.2,
        "max_pages": 8,
    },
    "indeed": {
        "rate_limit": 1.5,
        "max_pages": 10,
    },
    "portal_empleo": {
        "rate_limit": 2.0,
        "max_pages": 5,
    },
    "zonajobs": {
        "rate_limit": 1.3,
        "max_pages": 6,
    },
    "getonboard": {
        "rate_limit": 1.0,
        "max_pages": 15,      # GetOnBoard suele tener menos paginación
    },
}

# Helper para obtener settings fácilmente
def get_scraper_config(site_name: str) -> Dict:
    site_key = site_name.lower().replace(" ", "_")
    return SCRAPER_CONFIG.get(site_key, {"rate_limit": 1.5, "max_pages": DEFAULT_MAX_PAGES})