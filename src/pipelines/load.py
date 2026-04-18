# src/pipelines/load.py
from pathlib import Path
import pandas as pd
import logging

from ..utils.helpers import ensure_path

logger = logging.getLogger(__name__)

def load_raw(df: pd.DataFrame, ds: str, source: str, base_path: str = "data/raw"):
    """Bronze / Raw layer - idempotente con partition overwrite"""
    if df.empty:
        return

    path = Path(base_path) / source / ds
    ensure_path(path)

    output_file = path / "jobs_raw.parquet"
    df.to_parquet(output_file, compression="snappy", index=False)
    
    logger.info(f"[{source}] {len(df)} jobs raw guardados en {output_file}")

def load_processed(df: pd.DataFrame, ds: str, base_path: str = "data/processed"):
    """Processed / Silver layer"""
    if df.empty:
        return

    path = Path(base_path) / ds
    ensure_path(path)

    output_file = path / "jobs_processed.parquet"
    df.to_parquet(output_file, compression="snappy", index=False)
    
    logger.info(f"Procesados {len(df)} jobs guardados en {output_file}")