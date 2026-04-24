# src/scrapers/job_sites/getonboard.py

from typing import List, Dict, Optional
import asyncio
import re

from playwright.async_api import async_playwright
import aiohttp
from bs4 import BeautifulSoup

from src.scrapers.base_scraper import BaseJobScraper
from src.utils.helpers import normalize_url, get_random_headers
from src.config.settings import get_scraper_config


class GetOnBoardScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("getonboard")
        self.config = get_scraper_config("getonboard")

    # ===================== PLAYWRIGHT → SOLO LINKS =====================
    def _get_url(self, job_title:str, all=False):
        if all:
            return f"{self.base_url}/empleos/"
        return f"{self.base_url}/empleos-{job_title.replace(' ', '-')}"


    async def _auto_scroll(self, page, max_scrolls: int = 6):
        """Scroll limitado (evita loops infinitos)"""
        for _ in range(max_scrolls):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(300)

    async def _extract_links_for_job(self, page, job_title: str) -> List[str]:
        all = True

        listado = "div.gb-results-list" if all else "ul.gb-results-list"

        url = self._get_url(job_title=job_title,all=all)

        await page.goto(url, timeout=45000)
            
        try:
            await page.wait_for_selector(f"{listado}", timeout=15000)
        except Exception as e:
            self.logger.warning(f"Error obteniendo links: {e}")
            return []

        await self._auto_scroll(page)

        hrefs = await page.locator(f"{listado} a").evaluate_all(
            "nodes => nodes.map(n => n.getAttribute('href'))"
        )

        return list({
            normalize_url(h, self.base_url)
            for h in hrefs if h
        })

    # ===================== REQUESTS + BS4 =====================

    async def _fetch(self, session, url: str, sem: asyncio.Semaphore) -> Optional[str]:
        """Fetch HTML con rate limit"""
        async with sem:
            try:
                async with session.get(url, timeout=20) as res:
                    if res.status == 200:
                        return await res.text()
                    return None
            except Exception as e:
                self.logger.debug(f"Error fetch {url}: {e}")
                return None
    
    def _parse_job(self, html: str, url: str) -> Dict:
        """Parse HTML → dict (sin awaits, rápido)"""
        soup = BeautifulSoup(html, "html.parser")

        def safe_text(selector):
            el = soup.select_one(selector)
            return el.get_text(strip=True) if el else None

        def safe_all(selector):
            return [e.get_text(strip=True) for e in soup.select(selector)]
        
        def extract_posted_date(soup):
            # 1. PRIORIDAD: dato estructurado
            tag = soup.select_one("time[itemprop='datePosted']")
            if tag and tag.get("datetime"):
                return tag["datetime"]

            # 2. fallback: texto visible
            tag = soup.select_one("div.color-hierarchy2 time")
            if tag:
                return tag.get_text(strip=True)

            return None
        
        def extract_location(soup):
            # 1. camino principal (estructura semántica)
            tag = soup.select_one("span[itemprop='jobLocation'] a")
            if tag:
                return tag.get_text(strip=True)

            # 2. fallback: cualquier link de ciudad
            tag = soup.select_one("a[href*='/empleos/ciudad/']")
            if tag:
                return tag.get_text(strip=True)

            return None

        def classify_modality(text):
            if text == None:
                return "Otros"
            text = text.lower()
            if "remoto" in text:
                return "Remoto"
            elif "híbrido" in text:
                return "Híbrido"
            elif "presencial" in text:
                return "Presencial"
            else:
                return "Otros"
        
        def extract_job_type(soup):
            h2 = soup.select_one("h2.size1")
            if not h2:
                return None

            texts = list(h2.stripped_strings)

            if "Full time" in texts:
                return "Full time"
            elif "Part time" in texts: 
                return "Part time"
            elif "Freelance" in texts:
                return "Freelance"

            return None
        
        # TITLE
        title = safe_text("h1.gb-landing-cover__title") or safe_text("h1")

        # COMPANY
        company = safe_text("a.tooltipster")

        # LOCATION
        location = extract_location(soup)

        # DESCRIPTION
        desc_blocks = soup.select("div.gb-rich-txt")
        description = "\n".join([d.get_text(strip=True) for d in desc_blocks]) if desc_blocks else None

        # TAGS
        tags = safe_all("a.gb-tags__item")

        # SKILLS
        skills = list(set(
            safe_all("span.tag.bg_brand_light") +
            safe_all("ul.disc.mbB li")
        ))

        # DATE
        posted_date = extract_posted_date(soup)

        # SALARY
        min_salary = soup.select_one("span[itemprop='minValue']")
        max_salary = soup.select_one("span[itemprop='maxValue']")
        currency = soup.select_one("span[itemprop='currency']")
        unit = soup.select_one("span[itemprop='unitText']")

        salary = None
        salary_currency = None

        if min_salary and max_salary:
            salary = f"{min_salary.get('content')} - {max_salary.get('content')}"
        if currency and unit:
            salary_currency = f"{currency.get('content')}/{unit.get('content')}"

        # APPLICANTS
        applicants = None
        app_text = safe_text("div.size0.mt1")
        if app_text:
            match = re.search(r"(\d+)", app_text)
            if match:
                applicants = int(match.group(1))

        # EXPERIENCE
        experience = safe_text("span[itemprop='qualifications']")

        # JOB TYPE
        job_type = extract_job_type(soup)

        # MODALITY
        modality_text = safe_text("span[itemprop='jobLocation']").lower()
        modality = classify_modality(modality_text)
        
        return {
            "title": title,
            "company": company,
            "location": location,
            "salary": salary,
            "salary_currency": salary_currency,
            "job_type": job_type,
            "modality": modality,
            "experience_level": experience,
            "posted_date": posted_date,
            "url": url,
            "description_raw": description,
            "tags": tags,
            "skills": skills,
            "source_site": "get_on_board",
            "applicants": applicants,
        }

    async def _process_url(self, session, url: str, sem: asyncio.Semaphore) -> Optional[Dict]:
        html = await self._fetch(session, url, sem)
        if not html:
            return None
        return self._parse_job(html, url)

    # ===================== MAIN =====================

    def scrape(
        self,
        search_query: str = None,
        location: str = None,
        max_pages: int = None
    ) -> List[Dict]:

        search_query = search_query or "data engineer"

        async def run():
            # 1. Playwright → links
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent=self.headers["User-Agent"]
                )
                page = await context.new_page()

                links = await self._extract_links_for_job(page, search_query)
                await browser.close()

            self.logger.info(f"Links encontrados: {len(links)}")

            # 2. aiohttp + paralelismo
            headers = get_random_headers()

            connector = aiohttp.TCPConnector(limit=20)
            timeout = aiohttp.ClientTimeout(total=30)

            sem = asyncio.Semaphore(8)  # 🔥 control de concurrencia

            async with aiohttp.ClientSession(
                headers=headers,
                connector=connector,
                timeout=timeout
            ) as session:

                tasks = [
                    self._process_url(session, url, sem)
                    for url in links
                ]

                results = await asyncio.gather(*tasks)

            # limpiar None
            return [r for r in results if r]

        results = asyncio.run(run())

        self.logger.info(
            f"[{self.source_name}] Scrape completado → {len(results)} ofertas"
        )

        return results