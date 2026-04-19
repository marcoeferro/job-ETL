# src/scrapers/job_sites/zonajobs.py
from typing import List, Dict
import json
import asyncio
from playwright.async_api import async_playwright

from src.scrapers.base_scraper import BaseJobScraper

class ZonajobsScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("zonajobs")

    def build_fetch_script(self, query: str, page: int = 0, page_size: int = 20):
        return f"""
            () => fetch("https://www.zonajobs.com.ar/api/avisos/searchV2?pageSize={page_size}&page={page}&sort=RELEVANTES", {{
                method: "POST",
                headers: {{
                    "accept": "application/json",
                    "content-type": "application/json",
                    "x-site-id": "ZJAR"
                }},
                body: JSON.stringify({{
                    filtros: [],
                    query: "{query}"
                }})
            }}).then(r => r.json())
        """

    def scrape(self, 
               search_query: str = None,
               location: str = None,
               max_pages: int = None) -> List[Dict]:
        search_query = search_query or "data analyst"
        max_pages = max_pages or self.max_pages

        all_jobs: List[Dict] = []

        async def run():
            nonlocal all_jobs
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                page = await browser.new_context(user_agent=self.headers["User-Agent"]).new_page()

                await page.goto("https://www.zonajobs.com.ar/", timeout=60000)

                for p in range(max_pages):
                    self._respect_rate_limit()
                    script = self.build_fetch_script(search_query, p)

                    raw_result = await page.evaluate(script)
                    content = raw_result.get("content") if raw_result else None

                    if not content or len(content) == 0:
                        break

                    for item in content:
                        all_jobs.append({
                            "title": item.get("titulo"),
                            "company": item.get("empresa"),
                            "location": item.get("ubicacion"),
                            "url": item.get("url"),
                            "description_raw": item.get("descripcion"),
                            "source_site": "zonajobs"
                        })

                await browser.close()

        asyncio.run(run())
        return all_jobs