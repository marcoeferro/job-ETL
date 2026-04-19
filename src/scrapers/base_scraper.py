# src/scrapers/base_scraper.py
from abc import ABC, abstractmethod
from typing import List, Dict
import pandas as pd
from datetime import datetime
import logging
import time
import random

from src.config.settings import (
    SCRAPER_BASE_URLS,
    get_scraper_config,
    DEFAULT_SEARCH_QUERY,
    DEFAULT_LOCATION,
    DEFAULT_MAX_PAGES
)
from src.utils.helpers import get_random_headers, generate_job_key
from ..utils.logging import setup_logging

logger = setup_logging(__name__)


class BaseJobScraper(ABC):
    def __init__(self, source_name: str):
        self.source_name = source_name.lower()
        self.base_url = SCRAPER_BASE_URLS.get(self.source_name)
        
        if not self.base_url:
            raise ValueError(f"No se encontró URL base para el scraper: {source_name}")

        config = get_scraper_config(self.source_name)
        self.rate_limit = config["rate_limit"]
        self.max_pages = config.get("max_pages", DEFAULT_MAX_PAGES)

        self.headers = get_random_headers()
        
        # === NUEVO: Logger con contexto específico del scraper ===
        self.logger = setup_logging(f"scrapers.{self.source_name}")
        logger.info(f"[{self.source_name}] Scraper inicializado | base_url={self.base_url} | rate_limit={self.rate_limit}s")

    def _respect_rate_limit(self):
        """Espera entre requests para evitar ser bloqueado"""
        delay = self.rate_limit + random.uniform(0.3, 1.2)
        time.sleep(delay)

    @abstractmethod
    def scrape(self, 
                search_query: str = DEFAULT_SEARCH_QUERY,
                location: str = DEFAULT_LOCATION,
                max_pages: int = None) -> List[Dict]:
        """
        Método que cada scraper debe implementar.
        Devuelve lista de diccionarios con los datos crudos.
        """
        pass

    def scrape_to_dataframe(self, ds: str, **kwargs) -> pd.DataFrame:
        """Método principal recomendado para usar desde los pipelines"""
        logger.info(f"[{self.source_name}] Iniciando scrape_to_dataframe | ds={ds}")

        max_pages = kwargs.pop("max_pages", self.max_pages)

        jobs = self.scrape(
            search_query=kwargs.get("search_query", DEFAULT_SEARCH_QUERY),
            location=kwargs.get("location", DEFAULT_LOCATION),
            max_pages=max_pages
        )

        if not jobs:
            logger.warning(f"[{self.source_name}] No se obtuvieron ofertas")
            return pd.DataFrame()

        df = pd.DataFrame(jobs)
        
        # Metadata estándar
        df['source'] = self.source_name
        df['ds'] = ds
        df['scraped_at'] = datetime.utcnow().isoformat()
        df['job_key'] = df.apply(generate_job_key, axis=1)

        logger.info(f"[{self.source_name}] Scrape finalizado → {len(df)} ofertas")
        return df