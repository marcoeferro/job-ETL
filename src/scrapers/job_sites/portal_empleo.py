# src/scrapers/job_sites/portal_empleo.py

from typing import List, Dict, Optional
import asyncio
import re

from playwright.async_api import async_playwright
import aiohttp
from bs4 import BeautifulSoup

from src.scrapers.base_scraper import BaseJobScraper
from src.utils.helpers import normalize_url, get_random_headers
from src.config.settings import get_scraper_config


class PortalEmpleoScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("portal_empleo")
        self.config = get_scraper_config("portal_empleo")
        self.base_url = getattr(self.config, "base_url", None) or "https://portalempleo.gob.ar"

    # ===================== PLAYWRIGHT → SOLO LINKS =====================

    async def _auto_scroll(self, page, max_scrolls: int = 6):
        """Scroll limitado (evita loops infinitos)"""
        for _ in range(max_scrolls):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(300)

    def _get_url(self,job_title,page_number, all=False):
        if all:
            return f"{self.base_url}/OfertasLaborales?page-number={page_number}"
        return f"{self.base_url}/OfertasLaborales?Q={job_title.replace(' ', '+')}?page-number={page_number}"


    async def _extract_links_for_job(self, page, job_title: str) -> List[str]:
        """Extrae links de detalle usando el selector original del scraper"""
        url = f"{self.base_url}/OfertasLaborales?Q={job_title.replace(' ', '+')}"

        hrefs = []

        all = True
        
        for page_number in range(1, self.max_pages+1):
            
            url = self._get_url(job_title=job_title,page_number=page_number,all=all)
            
            await page.goto(url, timeout=45000)

            try:
                await page.wait_for_selector("a.btn.btn-success.comp-button-ciudadanos", timeout=15000)
            except Exception as e:
                self.logger.warning(f"Error obteniendo links: {e}")
                return []

            await self._auto_scroll(page)

            current_hrefs = await page.locator("a.btn.btn-success.comp-button-ciudadanos").evaluate_all(
                "nodes => nodes.map(n => n.getAttribute('href'))"
            )
            hrefs.extend([h for h in current_hrefs if h])
        
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
        """Parse HTML → dict (CORREGIDO según HTML de referencia)"""
        soup = BeautifulSoup(html, "html.parser")

        def safe_text(selector):
            el = soup.select_one(selector)
            return el.get_text(strip=True) if el else None

        def safe_all(selector):
            return [e.get_text(strip=True) for e in soup.select(selector)]

        # ===================== TITLE =====================
        # Nombre del empleo (h3 con clase específica)
        title = safe_text("h3.text-capitalize.text-turqueza") or safe_text("h3")

        # ===================== COMPANY =====================
        company = safe_text("div > svg ~ text") or safe_text("strong")  # MARIA ALICIA MARTINELLI

        # ===================== LOCATION =====================
        location = safe_text("div.col-md-12 p span") or safe_text("svg.fa-map-marker-alt ~ span") or safe_text("svg.fa-map-marker-alt ~ text")

        # ===================== SALARY =====================
        salary_text = soup.get_text()
        salary = "A Convenir" if "A Convenir" in salary_text or "a convenir" in salary_text.lower() else None
        salary_currency = "$ARS"  # ← valor por defecto solicitado

        # ===================== JOB TYPE =====================
        def extract_job_type(soup):
            for label in soup.select("label.fw-600"):
                if "Disponibilidad" in label.get_text() or "Disponibilidad horaria" in label.get_text():
                    p = label.find_next("p")
                    if p:
                        text = p.get_text(strip=True).lower()
                        if "tiempo completo" in text:
                            return "Tiempo completo"
                        elif "tiempo parcial" in text:
                            return "Tiempo parcial"
            return None

        job_type = extract_job_type(soup)

        # ===================== EXPERIENCE LEVEL =====================
        # "Experiencia requerida" (campo específico solicitado)
        def extract_experience_level(soup):
            for label in soup.select("label.fw-600"):
                if "Experiencia Requerida" in label.get_text():
                    p = label.find_next("p")
                    if p:
                        return p.get_text(strip=True)  # Ej: "No"
            return None

        experience_level = extract_experience_level(soup)

        # ===================== SKILLS =====================
        # Todo lo de "Requisitos" EXCEPTO "Experiencia Requerida"
        def extract_skills(soup):
            skills = []
            in_requisitos = False
            for row in soup.select(".row"):
                h2 = row.select_one("h2")
                if h2 and "Requisitos" in h2.get_text():
                    in_requisitos = True
                    continue

                if in_requisitos:
                    for label in row.select("label.fw-600"):
                        label_text = label.get_text(strip=True)
                        if "Experiencia Requerida" in label_text:
                            continue  # se excluye (va a experience_level)
                        p = label.find_next("p")
                        if p:
                            value = p.get_text(strip=True)
                            if value and value.lower() not in ["no", ""]:
                                skills.append(value)
            return list(set(skills))  # deduplicar

        skills = extract_skills(soup)

        # ===================== DESCRIPTION =====================
        desc_parts = []
        for header in soup.find_all(["h2", "label.fw-600"]):
            text = header.get_text(strip=True).lower()
            if any(k in text for k in ["resumen", "tareas", "principales tareas"]):
                for sibling in header.find_next_siblings():
                    if sibling.name in ["h2", "label"] and sibling.get_text(strip=True):
                        break
                    if sibling.name == "p" and sibling.get_text(strip=True):
                        desc_parts.append(sibling.get_text(strip=True))
        description = "\n".join(desc_parts) if desc_parts else safe_text("p")

        # ===================== DATE =====================
        posted_date = safe_text(".float-right.mt-2") or re.search(r'\b(\d{1,2}/\d{1,2}/\d{4})\b', soup.get_text()).group(1) if re.search(r'\b(\d{1,2}/\d{1,2}/\d{4})\b', soup.get_text()) else None

        # ===================== MODALITY =====================
        def classify_modality(text):
            if not text:
                return "Otros"
            text = text.lower()
            if "remoto" in text:
                return "Remoto"
            elif "híbrido" in text:
                return "Híbrido"
            elif "presencial" in text:
                return "Presencial"
            return "Otros"

        modality_text = location or ""
        modality = classify_modality(modality_text)

        # ===================== TAGS =====================
        tags = safe_all("label.fw-600")

        # ===================== APPLICANTS =====================
        applicants = None  # no existe en este sitio

        return {
            "title": title,
            "company": company,
            "location": location,
            "salary": salary,
            "salary_currency": salary_currency,          # ← corregido: siempre $ARS
            "job_type": job_type,
            "modality": modality,
            "experience_level": experience_level,        # ← corregido: "Experiencia Requerida"
            "posted_date": posted_date,
            "url": url,
            "description_raw": description,
            "tags": tags,
            "skills": skills,                            # ← corregido: solo requisitos sin experiencia
            "source_site": "portal_empleo",
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

        search_query = search_query or "data analyst"

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

            sem = asyncio.Semaphore(8)

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

            return [r for r in results if r]

        results = asyncio.run(run())

        self.logger.info(
            f"[{self.source_name}] Scrape completado → {len(results)} ofertas"
        )

        return results