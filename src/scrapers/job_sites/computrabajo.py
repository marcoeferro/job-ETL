# src/scrapers/job_sites/computrabajo.py
from typing import List, Dict
import asyncio
from playwright.async_api import async_playwright
import re

from src.scrapers.base_scraper import BaseJobScraper
from src.utils.helpers import sanitize_filename

class ComputrabajoScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("computrabajo")

    async def _auto_scroll(self, page):
        previous_height = await page.evaluate("document.body.scrollHeight")
        while True:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1200)
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == previous_height:
                break
            previous_height = new_height

    async def _extract_links_from_page(self, page):
        await page.wait_for_timeout(1500)
        await self._auto_scroll(page)

        try:
            await page.wait_for_selector("a.js-o-link.fc_base", timeout=8000)
        except:
            return []

        links = await page.locator("a.js-o-link.fc_base").evaluate_all(
            "nodes => nodes.map(n => n.href)"
        )
        return list(set(links))

    def scrape(self, 
                search_query: str = None,
                location: str = None,
                max_pages: int = None) -> List[Dict]:
        """Implementación específica para Computrabajo usando Playwright."""
        search_query = search_query or "data analyst"
        max_pages = max_pages or self.max_pages

        all_jobs: List[Dict] = []

        async def run():
            page_number = 1
            nonlocal all_jobs
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(user_agent=self.headers["User-Agent"])
                page = await context.new_page()

                while page_number <= max_pages:
                    self._respect_rate_limit()
                    url = f"{self.base_url}/trabajo-de-{search_query.replace(' ', '-')}/?p={page_number}"

                    try:
                        await page.goto(url, timeout=30000)
                        links = await self._extract_links_from_page(page)

                        if not links:
                            break

                        for link in links:
                            all_jobs.append({
                                "title": None,           # se puede enriquecer en transform
                                "company": None,
                                "location": None,
                                "url": link,
                                "description_raw": None, # se llena en detail scrape si se quiere
                                "source_site": "computrabajo"
                            })

                        page_number += 1
                    except Exception as e:
                        self.logger.warning(f"[{self.source_name}] Error en página {page_number}: {e}")
                        break

                await browser.close()

        asyncio.run(run())
        return all_jobs