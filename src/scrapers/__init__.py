from .job_sites.computrabajo import ComputrabajoScraper
from .job_sites.indeed import IndeedScraper
from .job_sites.portal_empleo import PortalEmpleoScraper
from .job_sites.zonajobs import ZonajobsScraper
from .job_sites.get_on_board import GetOnBoardScraper

SCRAPER_REGISTRY = {
    "computrabajo": ComputrabajoScraper,
    "indeed": IndeedScraper,
    "portal_empleo": PortalEmpleoScraper,
    "zonajobs": ZonajobsScraper,
    "getonboard": GetOnBoardScraper,
}

def get_scraper(site_name: str):
    site_key = site_name.lower().replace(" ", "_")
    scraper_class = SCRAPER_REGISTRY.get(site_key)
    if not scraper_class:
        raise ValueError(f"Scraper no registrado: {site_name}")
    return scraper_class()