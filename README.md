# Job Market Intelligence ETL

Pipeline para extraer, normalizar y cargar ofertas de trabajo desde múltiples portales, con deduplicación y consultas interactivas.

## Tecnologías
- Airflow (orquestación)
- PostgreSQL (almacenamiento)
- Python (scraping, transformación, NLP)
- Streamlit (dashboard)

## Uso rápido
1. Copiar `.env.example` a `.env` y ajustar
2. `docker-compose up -d`
3. Acceder a Airflow: http://localhost:8080
4. Activar el DAG `job_market_intelligence_pipeline`
5. Dashboard: `streamlit run streamlit_app/app.py`