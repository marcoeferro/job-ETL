# src/scrapers/base_scraper.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime
import logging

from ..utils.helpers import get_random_headers, random_delay, generate_job_key
from ..utils.logging import setup_logging

logger = setup_logging(__name__)

class BaseJobScraper(ABC):
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.headers = get_random_headers()

    @abstractmethod
    def scrape(self, search_query: str = "data analyst", location: str = "Argentina", max_pages: int = 5) -> List[Dict]:
        """Devuelve lista de dicts raw desde el sitio"""
        pass

    def scrape_to_dataframe(self, ds: str, **kwargs) -> pd.DataFrame:
        """Método principal: scrape + enrich con metadata (idempotente-friendly)"""
        logger.info(f"[{self.source_name}] Iniciando scrape para ds={ds}")
        
        jobs = self.scrape(**kwargs)
        
        if not jobs:
            logger.warning(f"[{self.source_name}] No se obtuvieron resultados")
            return pd.DataFrame()

        df = pd.DataFrame(jobs)
        df['source'] = self.source_name
        df['ds'] = ds
        df['scraped_at'] = datetime.utcnow().isoformat()
        df['job_key'] = df.apply(generate_job_key, axis=1)

        return df