# airflow/dags/job_market_intelligence_pipeline_dag.py
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import logging
import pandas as pd

logger = logging.getLogger(__name__)

@dag(
    dag_id="job_market_intelligence_pipeline",
    description="Pipeline completo: Scraping → Transform → Load (idempotente)",
    schedule="@daily",                    # o "0 2 * * *" (cada día a las 2 AM)
    start_date=days_ago(2),
    catchup=False,                        # importante para portafolio
    default_args={
        "owner": "marco",
        "retries": 2,
        "retry_delay": lambda: 60 * 5,    # 5 minutos
    },
    tags=["etl", "scraping", "job-market"],
)
def job_market_pipeline():

    @task
    def extract(ds: str = None) -> dict:
        """Extrae jobs de las fuentes configuradas"""
        from src.pipelines.extract import extract_jobs
        from src.config.settings import DEFAULT_SEARCH_QUERY, DEFAULT_MAX_PAGES

        logger.info(f"🚀 Extract - ds={ds}")

        return extract_jobs(
            ds=ds or datetime.now().strftime("%Y-%m-%d"),
            sources=["computrabajo"],           # ← agrega "zonajobs", "getonboard", etc. cuando los implementes
            search_query=DEFAULT_SEARCH_QUERY,
            max_pages=DEFAULT_MAX_PAGES
        )

    @task
    def transform(extracted_data: dict) -> dict:
        """Transforma cada DataFrame (Silver layer)"""
        from src.pipelines.transform import transform_jobs

        transformed = {}
        for source, df in extracted_data.items():
            logger.info(f"🔄 Transformando {source} - {len(df)} registros")
            transformed[source] = transform_jobs(df)
        
        return transformed

    @task
    def load(transformed_data: dict, ds: str = None):
        """Carga en Parquet (idempotente con overwrite)"""
        from src.pipelines.load import load_raw, load_processed
        from src.config.settings import DATA_RAW_DIR, DATA_PROCESSED_DIR

        ds = ds or datetime.now().strftime("%Y-%m-%d")

        for source, df in transformed_data.items():
            # Bronze (raw)
            load_raw(df, ds=ds, source=source, base_path=str(DATA_RAW_DIR))
            
            # Silver (processed)
            load_processed(df, ds=ds, base_path=str(DATA_PROCESSED_DIR))

        logger.info(f"✅ Pipeline finalizado para ds={ds}")

    # === ORQUESTACIÓN ===
    raw_data = extract()
    cleaned_data = transform(raw_data)
    load(cleaned_data)

# Instanciar el DAG
job_market_dag = job_market_pipeline()