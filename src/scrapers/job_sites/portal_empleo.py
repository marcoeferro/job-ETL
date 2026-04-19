# src/scrapers/job_sites/portal_empleo.py
from typing import List, Dict
import asyncio
from playwright.async_api import async_playwright

from src.scrapers.base_scraper import BaseJobScraper
from src.utils.helpers import normalize_url   # movido a utils

class PortalEmpleoScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("portal_empleo")

    def scrape(self, 
               search_query: str = None,
               location: str = None,
               max_pages: int = None) -> List[Dict]:
        search_query = search_query or "data analyst"

        all_jobs: List[Dict] = []

        async def run():
            nonlocal all_jobs
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                page = await browser.new_context(user_agent=self.headers["User-Agent"]).new_page()

                self._respect_rate_limit()
                search_url = f"{self.base_url}/OfertasLaborales?Q={search_query.replace(' ', '+')}"

                try:
                    await page.goto(search_url, timeout=45000)
                    # Scroll básico
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(2000)

                    anchors = await page.locator("a.btn.btn-success.comp-button-ciudadanos").evaluate_all(
                        "nodes => nodes.map(n => n.getAttribute('href'))"
                    )

                    for href in anchors:
                        url = normalize_url(href, "https://portalempleo.gob.ar")
                        all_jobs.append({
                            "title": None,
                            "company": None,
                            "location": None,
                            "url": url,
                            "description_raw": None,
                            "source_site": "portal_empleo"
                        })
                except Exception as e:
                    self.logger.warning(f"[{self.source_name}] Error: {e}")

                await browser.close()

        asyncio.run(run())
        return all_jobs