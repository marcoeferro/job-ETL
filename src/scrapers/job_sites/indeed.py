# src/scrapers/job_sites/indeed.py
from typing import List, Dict
import asyncio
from playwright.async_api import async_playwright

from src.scrapers.base_scraper import BaseJobScraper

class IndeedScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("indeed")

    def scrape(self, 
                search_query: str = None,
                location: str = None,
                max_pages: int = None) -> List[Dict]:
        search_query = search_query or "data analyst"
        location = location or "Argentina"
        max_pages = max_pages or self.max_pages

        all_jobs: List[Dict] = []

        async def run():
            nonlocal all_jobs
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                page = await browser.new_context(user_agent=self.headers["User-Agent"]).new_page()

                for page_num in range(max_pages):
                    self._respect_rate_limit()
                    url = f"{self.base_url}/jobs?q={search_query.replace(' ', '+')}&l={location}&start={page_num*10}"

                    try:
                        await page.goto(url, timeout=30000)
                        # Selectores típicos de Indeed (pueden necesitar ajuste)
                        cards = await page.locator("div.job_seen_beacon").all()

                        for card in cards:
                            try:
                                title_elem = card.locator("h2 a")
                                title = await title_elem.inner_text() if await title_elem.count() > 0 else None
                                url = await title_elem.get_attribute("href") if await title_elem.count() > 0 else None
                                if url and not url.startswith("http"):
                                    url = f"https://ar.indeed.com{url}"

                                all_jobs.append({
                                    "title": title,
                                    "company": None,
                                    "location": None,
                                    "url": url,
                                    "description_raw": None,
                                    "source_site": "indeed"
                                })
                            except:
                                continue
                    except Exception as e:
                        break

                await browser.close()

        asyncio.run(run())
        return all_jobs