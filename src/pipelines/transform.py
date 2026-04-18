# src/pipelines/transform.py
import pandas as pd

def transform_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza y normalización (Silver layer)"""
    if df.empty:
        return df

    df = df.copy()

    # Limpieza básica
    df['title'] = df['title'].str.strip().str.title() if 'title' in df.columns else None
    df['company'] = df['company'].str.strip() if 'company' in df.columns else None
    df['location'] = df['location'].str.strip() if 'location' in df.columns else None
    
    # Deduplicación por job_key
    df = df.drop_duplicates(subset=['job_key'])

    # Columnas adicionales útiles para NLP
    df['title_lower'] = df['title'].str.lower()

    return df