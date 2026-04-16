# dags/getonbrd_etl_dag.py
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.trigger_rule import TriggerRule

# Importamos tus funciones (las mismas que usas en __main__)
from extractors.scraper_get_on_board import extract_get_on_board
from transformer.transformer_get_on_board import get_on_board_transformer
from utils.save_to_html import save_html
from utils.save_to_JSON import save_to_JSON


# =============================================
# CONFIGURACIÓN DEL DAG
# =============================================
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="getonbrd_full_etl",
    default_args=default_args,
    description="ETL completo de GetOnBrd: extracción + transformación",
    schedule_interval=timedelta(days=1),        # diario (ajusta según necesidad)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "jobs", "getonbrd"],
    max_active_runs=1,                          # evita solapamientos
) as dag:

    # =============================================
    # 1. TAREA DE EXTRACCIÓN (async → necesitamos correrla en un event loop)
    # =============================================
    @task
    def extract_getonbrd_job_offers(job_titles: list[str]) -> dict:
        """
        Ejecuta tu scraper async dentro de un nuevo event loop.
        Devuelve las estadísticas para logging/XCom.
        """
        import asyncio

        # Esta es la única forma segura de correr código async en Airflow hoy
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            stats = loop.run_until_complete(
                extract_get_on_board(
                    save_html_callback=save_html,
                    job_titles=job_titles,
                    workers=6,               # puedes parametrizar esto también
                )
            )
            return stats
        finally:
            loop.close()

    # =============================================
    # 2. TAREA DE TRANSFORMACIÓN (sync, pero pesada → paralelismo interno)
    # =============================================
    @task
    def transform_getonbrd_data() -> dict:
        """
        Ejecuta tu transformador paralelo.
        Devuelve cantidad de registros procesados.
        """
        results = get_on_board_transformer(save_func=save_to_JSON)
        return {"transformed_records": len(results)}

    # =============================================
    # 3. TAREA OPCIONAL: Limpieza de raw (solo si quieres idempotencia total)
    # =============================================
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def cleanup_raw_folder() -> None:
        raw_path = Path(__file__).resolve().parents[2] / "data" / "raw"
        if raw_path.exists():
            for file in raw_path.glob("get_on_board_*.html"):
                try:
                    file.unlink()
                    print(f"[CLEANUP] Borrado {file}")
                except Exception as e:
                    print(f"[CLEANUP] Error borrando {file}: {e}")

    # =============================================
    # DEFINICIÓN DE JOB_TITLES (puedes moverlo a Variable o Connection)
    # =============================================
    JOB_TITLES = [
        "python developer",
        "data engineer",
        "data scientist",
        "machine learning",
        "devops",
        "backend",
        "frontend",
        "fullstack",
        "product manager",
        "qa",
    ]

    # =============================================
    # FLUJO DEL DAG
    # =============================================
    extract_task = extract_getonbrd_job_offers.override(task_id="extract")(
        job_titles=JOB_TITLES
    )

    transform_task = transform_getonbrd_data.override(task_id="transform")()

    cleanup_task = cleanup_raw_folder.override(task_id="cleanup_raw")()

    # Orden de ejecución
    extract_task >> transform_task >> cleanup_task