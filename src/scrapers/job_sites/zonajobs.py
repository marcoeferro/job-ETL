# src/scrapers/job_sites/zonajobs.py

from typing import List, Dict, Optional
import asyncio

from playwright.async_api import async_playwright
from lxml import html as lxml_html

from src.scrapers.base_scraper import BaseJobScraper
from src.utils.helpers import normalize_url
from src.config.settings import get_scraper_config


class ZonajobsScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("zonajobs")
        self.config = get_scraper_config("zonajobs")
        self.base_url = getattr(self.config, "base_url", None) or "https://www.zonajobs.com.ar"

    # ===================== PLAYWRIGHT → LINKS =====================
    def _get_url(self, slug:str, p:str, all=False):
        if all:
            return f"{self.base_url}/empleos.html?page={p}"
        return f"{self.base_url}/empleos-busqueda-{slug}.html?page={p}"

    async def _auto_scroll(self, page, max_scrolls: int = 8):
        for _ in range(max_scrolls):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(400)

    async def _extract_links_for_job(self, page, job_title: str) -> List[str]:
        hrefs = []
        slug = job_title.replace(" ", "-").lower()
        
        for p in range(1, self.max_pages + 1):
            url = self._get_url(slug,p,all=True)
            
            print(url)
            
            await page.goto(url, timeout=45000)

            try:
                await page.wait_for_selector(
                    "xpath=//main[@id='listado-avisos']//a[contains(@href, '/empleos/')]",
                    timeout=20000
                )
            except Exception as e:
                self.logger.warning(f"Página {p} sin resultados: {e}")
                break

            await self._auto_scroll(page)

            current_hrefs = await page.locator(
                "xpath=//main[@id='listado-avisos']//a[contains(@href, '/empleos/')]"
            ).evaluate_all("nodes => nodes.map(n => n.getAttribute('href'))")

            hrefs.extend([h for h in current_hrefs if h])

        return list({normalize_url(h, self.base_url) for h in hrefs if h})

    # ===================== FETCH CON PLAYWRIGHT =====================

    async def _fetch_page(self, context, url: str, sem: asyncio.Semaphore) -> Optional[str]:
        async with sem:
            page = await context.new_page()
            try:
                await page.goto(url, timeout=45000)

                # Esperar contenido real (clave anti-bot)
                await page.wait_for_selector("//h1", timeout=15000)

                content = await page.content()
                return content

            except Exception as e:
                self.logger.debug(f"Error fetch {url}: {e}")
                return None
            finally:
                await page.close()

    async def _process_url(self, context, url: str, sem: asyncio.Semaphore) -> Optional[Dict]:
        html = await self._fetch_page(context, url, sem)

        if not html:
            return None

        return self._parse_job(html, url)

    # ===================== PARSE =====================

    def _parse_job(self, html: str, url: str) -> Dict:
        tree = lxml_html.fromstring(html)

        def safe_text(xpath: str) -> Optional[str]:
            try:
                result = tree.xpath(xpath)
                if result:
                    # Si es string, lo devolvemos directo
                    if isinstance(result[0], str):
                        val = result[0]
                    else:
                        # Si es Element, usamos .text_content()
                        val = result[0].text_content()
                    return val.strip() if val else None
                return None
            except Exception:
                return None


        def safe_all(xpath: str) -> List[str]:
            try:
                return [t.strip() for t in tree.xpath(xpath) if isinstance(t, str) and t.strip()]
            except:
                return []

        title = safe_text("//h1[1]")

        company = safe_text(
            "(//a[contains(@href, '/perfiles/')]//text() | //span[contains(@data-url, '/perfiles/')]//text())[1]"
        )

        location = safe_text("(//i[contains(@name, 'location')]/following::h2)[3]")

        modality_text = safe_text("(//i[contains(@name, 'office')]/following::p[1])[2]")
        modality = self._classify_modality(modality_text)

        posted_date = safe_text("(//i[contains(@name, 'location')]/following::h2)[2]")

        description = safe_text("//*[@id='ficha-detalle']/div[2]/div/div")

        job_type = safe_text("//i[contains(@name, 'clock')]/following::p[1]")

        experience_level = safe_text("//i[contains(@name, 'award')]/following::p[1]")

        val1 = safe_text("//*[@id='ficha-detalle']/div[2]/div/div/p/ul[4]")
        val2 = safe_text("//*[@id='ficha-detalle']/div[2]/div/div/p/p[5]")
        skills = val1 if val1 else val2

        tags = safe_text("(//i[contains(@name, 'location')]/following::h2)[1]")

        return {
            "title": title,
            "company": company,
            "location": location,
            "salary": None,
            "salary_currency": "$ARS",
            "job_type": job_type,
            "modality": modality,
            "experience_level": experience_level,
            "posted_date": posted_date,
            "url": url,
            "description_raw": description,
            "tags": tags,
            "skills": skills,
            "source_site": "zonajobs",
            "applicants": None,
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

                context = await browser.new_context(
                    user_agent=self.headers["User-Agent"]
                )

                page = await context.new_page()

                links = await self._extract_links_for_job(page, search_query)
                self.logger.info(f"Links encontrados: {len(links)}")

                # 🔥 Control de concurrencia (clave)
                sem = asyncio.Semaphore(5)

                tasks = [
                    self._process_url(context, url, sem)
                    for url in links
                ]

                results = await asyncio.gather(*tasks)

                await browser.close()

                return [r for r in results if r]

        results = asyncio.run(run())

        self.logger.info(f"[{self.source_name}] Scrape completado → {len(results)} ofertas")
        return results