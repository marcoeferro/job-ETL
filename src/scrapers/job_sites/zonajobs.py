# src/scrapers/job_sites/zonajobs.py

from typing import List, Dict, Optional
import asyncio

from playwright.async_api import async_playwright
import aiohttp
from lxml import html as lxml_html

from src.scrapers.base_scraper import BaseJobScraper
from src.utils.helpers import normalize_url, get_random_headers
from src.config.settings import get_scraper_config


class ZonajobsScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("zonajobs")
        self.config = get_scraper_config("zonajobs")
        self.base_url = getattr(self.config, "base_url", None) or "https://www.zonajobs.com.ar"

    # ===================== PLAYWRIGHT → LINKS (PAGINACIÓN) =====================

    async def _auto_scroll(self, page, max_scrolls: int = 8):
        for _ in range(max_scrolls):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(400)

    async def _extract_links_for_job(self, page, job_title: str) -> List[str]:
        """Extrae links usando XPath estructural (sin clases)."""
        hrefs = []
        slug = job_title.replace(" ", "-").lower()

        for p in range(1, self.max_pages + 1):
            url = f"{self.base_url}/empleos-busqueda-{slug}.html?page={p}"

            await page.goto(url, timeout=45000)

            try:
                # Espera explícita: al menos un <a> de oferta dentro de <main>
                await page.wait_for_selector("xpath=//main[@id='listado-avisos']//a[contains(@href, '/empleos/')]", timeout=20000)
            except Exception as e:
                self.logger.warning(f"Página {p} sin resultados: {e}")
                break

            await self._auto_scroll(page)

            # Extraer todos los href de los avisos
            current_hrefs = await page.locator(
                "xpath=//main[@id='listado-avisos']//a[contains(@href, '/empleos/')]"
            ).evaluate_all("nodes => nodes.map(n => n.getAttribute('href'))")

            hrefs.extend([h for h in current_hrefs if h])

        return list({normalize_url(h, self.base_url) for h in hrefs if h})

    # ===================== FETCH =====================

    async def _fetch(self, session, url: str, sem: asyncio.Semaphore) -> Optional[str]:
        async with sem:
            try:
                async with session.get(url, timeout=20) as res:
                    print("CLOUDFLARE Y LPMQTP ", await res.status, await res.text())
                    return await res.text() if res.status == 200 else None
            except Exception as e:
                self.logger.debug(f"Error fetch {url}: {e}")
                return None

    # ===================== PARSE CON XPATH ESTRUCTURAL =====================

    def _parse_job(self, html: str, url: str) -> Dict:
        tree = lxml_html.fromstring(html)

        def safe_text(xpath: str) -> Optional[str]:
            try:
                result = tree.xpath(xpath)
                if result:
                    val = result[0] if isinstance(result[0], str) else " ".join(str(x) for x in result)
                    return val.strip() if val else None
                return None
            except:
                return None

        def safe_all(xpath: str) -> List[str]:
            try:
                return [t.strip() for t in tree.xpath(xpath) if isinstance(t, str) and t.strip()]
            except:
                return []

        # TITLE: primer <h1> en la página de detalle
        title = safe_text("//h1[1]")

        # COMPANY: texto dentro del primer <span> o <div> que contiene enlace a perfil de empresa (posición jerárquica)
        company = safe_text("(//a[contains(@href, '/perfiles/')]//text() | //span[contains(@data-url, '/perfiles/')]//text())[1]")

        # LOCATION: <h2> que sigue al primer icono de ubicación (jerarquía padre-hijo)
        location = safe_text("(//i[contains(@name, 'location')]/following::h2)[3]")

        # MODALITY: <p> que sigue al primer icono de oficina
        modality_text = safe_text("(//i[contains(@name, 'office')]/following::p[1])[2]")
        modality = self._classify_modality(modality_text)

        # POSTED DATE: segundo <h2> en la sección superior (posición relativa)
        posted_date = safe_text("(//i[contains(@name, 'location')]/following::h2)[2]")

        # DESCRIPTION: primer <p> grande después del <h3> de descripción (jerarquía)
        description = safe_text("//h3[contains(., 'Descripción')]/following::p[1]") or \
                        "\n".join(safe_all("//div[@id='ficha-detalle']//p"))

        # JOB TYPE: <p> que sigue al icono de reloj
        job_type = safe_text("//i[contains(@name, 'clock')]/following::p[1]")

        # EXPERIENCE LEVEL: <p> que sigue al icono de award/medalla
        experience_level = safe_text("//i[contains(@name, 'award')]/following::p[1]")

        # SKILLS: todos los <li> dentro de la sección de descripción/requisitos
        skills = (safe_all("//*[@id='ficha-detalle']/div[2]/div/div/p/ul[4]") or 
                    safe_all("//*[@id='ficha-detalle']/div[2]/div/div/p/p[5]"))

        # TAGS: todos los <span> o <p> pequeños en la sección de metadata
        tags = safe_all("(//i[contains(@name, 'location')]/following::h2)[1]")

        salary = None
        salary_currency = "$ARS"
        applicants = None

        return {
            "title": title,
            "company": company,
            "location": location,
            "salary": salary,
            "salary_currency": salary_currency,
            "job_type": job_type,
            "modality": modality,
            "experience_level": experience_level,
            "posted_date": posted_date,
            "url": url,
            "description_raw": description,
            "tags": tags,
            "skills": skills,
            "source_site": "zonajobs",
            "applicants": applicants,
        }

    def _classify_modality(self, text: str) -> str:
        if not text:
            return "Otros"
        t = text.lower()
        if "remoto" in t:
            return "Remoto"
        elif "híbrido" in t or "hybrid" in t:
            return "Híbrido"
        elif "presencial" in t:
            return "Presencial"
        return "Otros"

    async def _process_url(self, session, url: str, sem: asyncio.Semaphore) -> Optional[Dict]:
        html = await self._fetch(session, url, sem)
        print("HTML",html)
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
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(user_agent=self.headers["User-Agent"])
                page = await context.new_page()

                links = await self._extract_links_for_job(page, search_query)
                await browser.close()

            self.logger.info(f"Links encontrados: {len(links)}")

            headers = get_random_headers()
            connector = aiohttp.TCPConnector(limit=20)
            timeout = aiohttp.ClientTimeout(total=30)
            sem = asyncio.Semaphore(8)

            async with aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout) as session:
                tasks = [self._process_url(session, url, sem) for url in links]
                results = await asyncio.gather(*tasks)

            return [r for r in results if r]

        results = asyncio.run(run())

        self.logger.info(f"[{self.source_name}] Scrape completado → {len(results)} ofertas")
        return results