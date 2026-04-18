# src/utils/logging.py
import logging
from pathlib import Path
from datetime import datetime

def setup_logging(name: str = "job_pipeline"):
    log_dir = Path("airflow/logs") if Path("airflow").exists() else Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True
    )
    return logging.getLogger(name)