# src/pipelines/extract.py
from typing import Dict
import pandas as pd
from ..scrapers.job_sites.computrabajo import ComputrabajoScraper
from ..scrapers.job_sites.get_on_board import GetOnBoardScraper
from ..scrapers.job_sites.indeed import IndeedScraper
from ..scrapers.job_sites.portal_empleo import PortalEmpleoScraper
from ..scrapers.job_sites.zonajobs import ZonajobsScraper
# Importa otros cuando los implementes: GetOnBoardScraper, ZonajobsScraper, etc.

def extract_jobs(ds: str, sources: list = None, search_query: str = "data analyst", max_pages: int = 3) -> Dict[str, pd.DataFrame]:
    """Orquestador de extracción - devuelve dict con DataFrames por fuente"""
    if sources is None:
        sources = ["computrabajo"]  # extender fácilmente

    results = {}

    for source in sources:
        if source == "computrabajo":
            scraper = ComputrabajoScraper()
        elif source == "get_on_board":
            scraper = GetOnBoardScraper()
        elif source == "indeed":
            scraper = IndeedScraper()
        elif source == "portal_empleo":
            scraper = PortalEmpleoScraper()
        elif source == "zonajobs":
            scraper = ZonajobsScraper()
        else:
            continue

        df = scraper.scrape_to_dataframe(ds=ds, search_query=search_query, max_pages=max_pages)
        results[source] = df

    return results