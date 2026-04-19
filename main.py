# src/main.py
"""
Main entry point para ejecución local (sin Docker ni Airflow).
Ideal para testing rápido y debugging.
"""
from datetime import datetime
import logging

from src.config.settings import (
    DEFAULT_SEARCH_QUERY,
    DEFAULT_MAX_PAGES,
    DATA_RAW_DIR,
    DATA_PROCESSED_DIR,
)
from src.utils.logging import setup_logging
from src.pipelines.extract import extract_jobs
from src.pipelines.transform import transform_jobs
from src.pipelines.load import load_raw, load_processed

# Configurar logging
logger = setup_logging(__name__)

def run_pipeline(ds: str = None):
    """Ejecuta el pipeline completo de forma local"""
    
    if ds is None:
        ds = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"🚀 Iniciando Job Market Intelligence Pipeline para ds = {ds}")

    # 1. EXTRACT
    logger.info("📥 Etapa 1: Extract")
    extracted_data = extract_jobs(
        ds=ds,
        sources=["computrabajo"],           # ← agrega más fuentes cuando las tengas
        search_query="data analyst",
        max_pages=DEFAULT_MAX_PAGES
    )

    # 2. TRANSFORM
    logger.info("🔄 Etapa 2: Transform")
    transformed_data = {}
    for source, df in extracted_data.items():
        transformed_data[source] = transform_jobs(df)

    # 3. LOAD
    logger.info("💾 Etapa 3: Load (idempotente)")
    for source, df in transformed_data.items():
        load_raw(df, ds=ds, source=source, base_path=str(DATA_RAW_DIR))
        load_processed(df, ds=ds, base_path=str(DATA_PROCESSED_DIR))

    logger.info(f"✅ Pipeline completado exitosamente para ds={ds}")
    logger.info(f"   Archivos generados en:")
    logger.info(f"   → data/raw/")
    logger.info(f"   → data/processed/")

if __name__ == "__main__":
    # Puedes pasar una fecha específica para testing/backfill
    # run_pipeline("2026-04-17")
    run_pipeline()   # usa la fecha de hoy