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
        # Aseguramos base_url (el config o fallback al valor hardcodeado original)
        self.base_url = getattr(self.config, "base_url", None) or "https://portalempleo.gob.ar"

    # ===================== PLAYWRIGHT → SOLO LINKS =====================

    async def _auto_scroll(self, page, max_scrolls: int = 6):
        """Scroll limitado (evita loops infinitos)"""
        for _ in range(max_scrolls):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(300)

    async def _extract_links_for_job(self, page, job_title: str) -> List[str]:
        """Extrae links de detalle usando el selector original del scraper"""
        url = f"{self.base_url}/OfertasLaborales?Q={job_title.replace(' ', '+')}"
        await page.goto(url, timeout=45000)

        try:
            # Esperamos los botones "Ver Oferta" / "comp-button-ciudadanos"
            await page.wait_for_selector("a.btn.btn-success.comp-button-ciudadanos", timeout=15000)
        except Exception as e:
            self.logger.warning(f"Error obteniendo links: {e}")
            return []

        await self._auto_scroll(page)

        hrefs = await page.locator("a.btn.btn-success.comp-button-ciudadanos").evaluate_all(
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
        """Parse HTML → dict (adaptado a la estructura de PortalEmpleo.gob.ar)"""
        soup = BeautifulSoup(html, "html.parser")

        def safe_text(selector):
            el = soup.select_one(selector)
            return el.get_text(strip=True) if el else None

        def safe_all(selector):
            return [e.get_text(strip=True) for e in soup.select(selector)]

        def extract_location(soup):
            # Patrón típico: "ROSARIO , SANTA FE" o "RIO CUARTO , CORDOBA"
            match = re.search(r'([A-ZÁÉÍÓÚÑ\s]+)\s*,\s*([A-ZÁÉÍÓÚÑ\s]+)', soup.get_text())
            if match:
                return f"{match.group(1).strip()} , {match.group(2).strip()}"
            return None

        def extract_posted_date(soup):
            # Fecha visible tipo "25/11/2024"
            match = re.search(r'\b(\d{1,2}/\d{1,2}/\d{4})\b', soup.get_text())
            return match.group(1) if match else None

        def extract_job_type(soup):
            text = soup.get_text().lower()
            if "tiempo completo" in text:
                return "Tiempo completo"
            elif "tiempo parcial" in text:
                return "Tiempo parcial"
            return None

        def classify_modality(text):
            # Reutilizamos la lógica de GetOnBoard
            if text is None:
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

        # TITLE
        title = safe_text("h3") or safe_text("h1") or safe_text("h2")

        # COMPANY (el primer <strong> suele ser el nombre de la empresa)
        company = safe_text("strong")

        # LOCATION
        location = extract_location(soup)

        # DESCRIPTION (sección "Tareas a Realizar")
        description = None
        for header in soup.find_all(["h4", "h3", "strong"]):
            if "tareas a realizar" in header.get_text().lower():
                desc_parts = []
                for sibling in header.find_next_siblings():
                    if sibling.name in ["h4", "h3", "strong"] and sibling.get_text(strip=True):
                        break
                    if sibling.get_text(strip=True):
                        desc_parts.append(sibling.get_text(strip=True))
                description = "\n".join(desc_parts) if desc_parts else None
                break
        # fallback si no se encontró la sección
        if not description:
            desc_blocks = [p.get_text(strip=True) for p in soup.select("p") if p.get_text(strip=True)]
            description = "\n".join(desc_blocks)

        # TAGS / SKILLS (categorías y requisitos)
        tags = safe_all("strong")
        skills = list(set(
            safe_all("li") +
            safe_all("strong") +
            [safe_text("span")]
        ))

        # DATE
        posted_date = extract_posted_date(soup)

        # SALARY (casi siempre "A Convenir")
        salary_text = soup.get_text()
        salary = "A Convenir" if "A Convenir" in salary_text or "a convenir" in salary_text.lower() else None
        salary_currency = None

        # APPLICANTS (no existe en PortalEmpleo)
        applicants = None

        # EXPERIENCE / REQUISITOS
        experience = None
        for header in soup.find_all(["h4", "h3", "strong"]):
            if "requisitos" in header.get_text().lower():
                req_parts = []
                for sibling in header.find_next_siblings():
                    if sibling.name in ["h4", "h3", "strong"] and sibling.get_text(strip=True):
                        break
                    if sibling.get_text(strip=True):
                        req_parts.append(sibling.get_text(strip=True))
                experience = "; ".join(req_parts) if req_parts else None
                break

        # JOB TYPE
        job_type = extract_job_type(soup)

        # MODALITY
        modality_text = location or ""
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