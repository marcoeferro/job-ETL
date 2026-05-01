# src/pipelines/transform.py
import pandas as pd
import re
from fuzzywuzzy import fuzz, process
from unidecode import unidecode
from datetime import datetime
from ..utils.logging import get_logger
from ..nlp.extractors import normalize_technologies, normalize_location, normalize_title


logger = get_logger(__name__)

# Diccionarios de estandarización (ejemplo)
TECH_STANDARD = {
    'py': 'Python', 'python': 'Python', 'js': 'JavaScript', 'javascript': 'JavaScript',
    'java': 'Java', 'sql': 'SQL', 'aws': 'AWS', 'azure': 'Azure', 'gcp': 'GCP',
    'airflow': 'Airflow', 'spark': 'Spark', 'pandas': 'Pandas'
}

LOCATION_STANDARD = {
    'bs as': 'Buenos Aires', 'capital federal': 'CABA', 'caba': 'CABA',
    'cordoba': 'Córdoba', 'rosario': 'Rosario', 'mendoza': 'Mendoza'
}

def normalize_text(text: str) -> str:
    if pd.isna(text):
        return ""
    text = unidecode(text.lower())
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def validate_row(row):
    """Validaciones básicas: título no nulo, ubicación no vacía, etc."""
    if pd.isna(row.get('title')) or row.get('title') == '':
        return False
    if pd.isna(row.get('location')):
        row['location'] = 'Desconocida'
    if pd.isna(row.get('company')):
        row['company'] = 'Desconocida'
    return True

def generate_job_id(row):
    """Hash simple basado en título + compañía + ubicación normalizada."""
    import hashlib
    unique_str = f"{row['title']}|{row['company']}|{row['location']}"
    return hashlib.md5(unique_str.encode()).hexdigest()

def deduplicate(df: pd.DataFrame, threshold=85):
    """Elimina duplicados usando fuzzy matching sobre título y compañía."""
    
    to_keep = []
    for i, row in df.iterrows():
        duplicate = False
        for j in to_keep:
            title_sim = fuzz.ratio(row['title'], df.loc[j, 'title'])
            company_sim = fuzz.ratio(row['company'], df.loc[j, 'company'])
            if title_sim > threshold and company_sim > threshold:
                duplicate = True
                break
        if not duplicate:
            to_keep.append(i)
    return df.loc[to_keep].drop(columns=['title', 'company'])

def transform_jobs(df: pd.DataFrame) -> pd.DataFrame:
    # Leer todos los raw parquets del día
    today = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Procesando {len(df.shape[0])} filas de Dataframe")
    
    df = df.copy()

    if not df:
        logger.warning("No hay datos para transformar")
        return
    
    # Aplicar transformaciones
    df['description'] = df['description'].fillna('')
    df['location'] = df['location'].apply(normalize_location).apply(lambda x: LOCATION_STANDARD.get(x.lower(), x))
    df['technologies'] = df['description'].apply(normalize_technologies)
    df['title'] = df['title'].apply(normalize_title).apply(normalize_text)
    df['company'] = df['company'].apply(normalize_text)
    
    # Validar y filtrar
    df['valid'] = df.apply(validate_row, axis=1)
    df = df[df['valid']].drop(columns=['valid'])
    
    # Deduplicar
    df_deduplicated = deduplicate(df)
    logger.info(f"Después de deduplicación: {len(df_deduplicated)}")
    
    # Generar job_id
    df_deduplicated['job_id'] = df_deduplicated.apply(generate_job_id, axis=1)
    
    # Deduplicación por job_key
    df = df.drop_duplicates(subset=['job_key'])

    logger.info(f"Datos procesados con exito")
    return df

