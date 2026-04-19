# src/scrapers/job_sites/getonboard.py
from typing import List, Dict
import asyncio
from playwright.async_api import async_playwright

from src.scrapers.base_scraper import BaseJobScraper
from src.utils.helpers import normalize_url   # movido a utils

class GetOnBoardScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("getonboard")

    async def _extract_links_for_job(self, page, job_title: str) -> List[str]:
        await page.goto(self.base_url, timeout=45000)
        try:
            await page.locator("#search_term").fill(job_title)
            await page.keyboard.press("Enter")
            await page.wait_for_selector("ul.gb-results-list", timeout=15000)
        except:
            return []

        await self._auto_scroll(page)   # reutilizamos helper si se mueve a utils

        hrefs = await page.locator("ul.gb-results-list a").evaluate_all(
            "nodes => nodes.map(n => n.getAttribute('href'))"
        )

        links = []
        for href in hrefs:
            normalized = normalize_url(href, self.base_url)
            if normalized:
                links.append(normalized)
        return links

    def scrape(self, 
                search_query: str = None,
                location: str = None,
                max_pages: int = None) -> List[Dict]:
        search_query = search_query or "data engineer"
        
        all_jobs: List[Dict] = []

        async def run():
            nonlocal all_jobs
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(user_agent=self.headers["User-Agent"])
                page = await context.new_page()

                self._respect_rate_limit()
                links = await self._extract_links_for_job(page, search_query)

                for link in links:
                    all_jobs.append({
                        "title": None,
                        "company": None,
                        "location": None,
                        "url": link,
                        "description_raw": None,
                        "source_site": "getonboard"
                    })

                await browser.close()

        asyncio.run(run())
        return all_jobs

    async def _auto_scroll(self, page):
        previous_height = await page.evaluate("document.body.scrollHeight")
        while True:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(350)
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == previous_height:
                break
            previous_height = new_height