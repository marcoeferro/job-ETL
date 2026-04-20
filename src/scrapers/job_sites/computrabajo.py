# src/scrapers/job_sites/computrabajo.py
from typing import List, Dict, Optional
import asyncio
import re
from playwright.async_api import async_playwright

from src.scrapers.base_scraper import BaseJobScraper


class ComputrabajoScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("computrabajo")

    async def _auto_scroll(self, page):
        """Scroll hasta el final de la página para cargar todos los elementos dinámicos."""
        previous_height = await page.evaluate("document.body.scrollHeight")
        while True:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1200)
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == previous_height:
                break
            previous_height = new_height

    async def _extract_links_from_page(self, page) -> List[str]:
        """Extrae los enlaces únicos a las ofertas desde la página de listado."""
        await page.wait_for_timeout(1500)
        await self._auto_scroll(page)

        try:
            await page.wait_for_selector("a.js-o-link.fc_base", timeout=8000)
        except Exception:
            self.logger.warning("No se encontraron enlaces de ofertas en la página de listado")
            return []

        links = await page.locator("a.js-o-link.fc_base").evaluate_all(
            "nodes => nodes.map(n => n.href)"
        )
        return list(set(links))

    # ====================== FUNCIONES PRIVADAS PARA PÁGINA DE DETALLE ======================

    async def _get_title(self, page) -> Optional[str]:
        """Extrae el título de la oferta desde el HTML de detalle."""
        try:
            # Selector principal en el HTML proporcionado
            title = await page.locator("h1.fwB.fs24.mb5.box_detail.w100_m").first.text_content()
            return title.strip() if title else None
        except Exception:
            try:
                # Fallback más genérico
                title = await page.locator("h1").first.text_content()
                return title.strip() if title else None
            except Exception:
                self.logger.debug("No se pudo extraer título")
                return None

    async def _get_company(self, page) -> Optional[str]:
        """Extrae el nombre de la empresa."""
        try:
            company_text = await page.locator("div.container").nth(1).locator("p.fs16").first.text_content()
            if company_text:
                # Formato típico: "Grupo Metropol - San Nicolás, Capital Federal"
                parts = company_text.split(" - ")
                if len(parts) > 1:
                    # Une todas las partes menos la última
                    return " - ".join(parts[:-1]).strip()
                return company_text.strip()
            return None

        except Exception as e:
            self.logger.debug(f"No se pudo extraer empresa: {e}")
            return None

    async def _get_location(self, page) -> Optional[str]:
        """Extrae la ubicación de la oferta."""
        try:
            # Del párrafo debajo del título o del side panel
            loc_text = await page.locator("div.container").nth(1).locator("p.fs16").first.text_content()
            if loc_text and " - " in loc_text:
                return loc_text.split(" - ")[-1].strip()
            
            # Fallback en panel lateral
            side_loc = await page.locator("p.fs16:has-text('Capital Federal')").first.text_content()
            return side_loc.strip() if side_loc else None
        except Exception:
            self.logger.debug("No se pudo extraer ubicación")
            return None

    async def _get_description(self, page) -> Optional[str]:
        """Extrae la descripción completa de la oferta (incluye responsabilidades, requisitos, etc.)."""
        try:
            # Selector del contenedor principal de descripción
            desc_elem = await page.locator('div[div-link="oferta"] p').all_text_contents()
            if desc_elem:
                return "\n".join([text.strip() for text in desc_elem if text.strip()])
            
            # Fallback más amplio
            desc = await page.locator("div.mb40.pb40.bb1[div-link='oferta']").text_content()
            return desc.strip() if desc else None
        except Exception:
            self.logger.debug("No se pudo extraer descripción")
            return None

    async def _get_tags(self, page) -> List[str]:
        """Extrae tags como tipo de contrato, jornada, modalidad, salario, etc."""
        tags = []
        try:
            tag_elements = await page.locator("span.tag.base.mb10").all_text_contents()
            tags = [tag.strip() for tag in tag_elements if tag.strip()]
        except Exception:
            pass
        return tags

    async def _get_skills(self, page) -> List[str]:
        """Extrae skills/aptitudes asociadas (ej: Mysql, Power BI, SQL)."""
        skills = []
        try:
            # Skills principales visibles
            skill_tags = await page.locator("span.tag.bg_brand_light.fc_base").all_text_contents()
            skills.extend([s.strip() for s in skill_tags if s.strip()])
            
            # Requerimientos en lista
            req_items = await page.locator("ul.disc.mbB li").all_text_contents()
            for item in req_items:
                if item.strip():
                    skills.append(item.strip())
        except Exception:
            pass
        return list(set(skills))  # eliminar duplicados

    async def _get_posted_date(self, page) -> Optional[str]:
        """Extrae la fecha de publicación/actualización."""
        try:
            date_text = await page.locator("p.fc_aux.fs13").filter(has_text="días").first.text_content()
            return date_text.strip() if date_text else None
        except Exception:
            try:
                date_text = await page.locator("p.fc_aux").filter(has_text="Hace").first.text_content()
                return date_text.strip()
            except Exception:
                self.logger.debug("No se pudo extraer fecha de publicación")
                return None

    async def _get_salary_info(self, page) -> Dict[str, Optional[str]]:
        """Extrae información de salario (actualmente suele ser 'A convenir')."""
        salary_info = {"salary": None, "currency": "$ARS"}
        try:
            salary_tag = await page.locator("span.tag.base.mb10").filter(has_text="convenir").first.text_content()
            if salary_tag:
                salary_info["salary"] = salary_tag.strip()
        except Exception:
            pass
        return salary_info

    # ====================== SCRAPE DETALLE ======================

    async def _scrape_job_detail(self, page, url: str) -> Dict:
        """Scrape completo de una página de detalle usando funciones privadas."""
        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)  # Espera para que cargue contenido dinámico

            title = await self._get_title(page)
            company = await self._get_company(page)
            location = await self._get_location(page)
            description_raw = await self._get_description(page)
            tags = await self._get_tags(page)
            skills = await self._get_skills(page)
            posted_date = await self._get_posted_date(page)
            salary_info = await self._get_salary_info(page)

            # Parseo básico de tags (ejemplos del HTML)
            job_type = next((t for t in tags if "jornada" in t.lower()), None)
            modality = next((t for t in tags if any(m in t.lower() for m in ["presencial", "remoto", "híbrido"])), None)

            return {
                "title": title,
                "company": company,
                "location": location,
                "salary": salary_info.get("salary"),
                "salary_currency": salary_info.get("currency"),
                "job_type": job_type,
                "modality": modality,
                "experience_level": None,  # No presente de forma clara en el HTML de ejemplo
                "posted_date": posted_date,
                "url": url,
                "description_raw": description_raw,
                "tags": tags,
                "skills": skills,
                "source_site": "computrabajo",
                "applicants": None,  # No visible en el HTML proporcionado
            }
        except Exception as e:
            self.logger.warning(f"Error al scrapear detalle de {url}: {e}")
            return {
                "title": None,
                "company": None,
                "location": None,
                "salary": None,
                "salary_currency": None,
                "job_type": None,
                "modality": None,
                "experience_level": None,
                "posted_date": None,
                "url": url,
                "description_raw": None,
                "tags": [],
                "skills": [],
                "source_site": "computrabajo",
                "applicants": None,
            }

    # ====================== MÉTODO PRINCIPAL ======================

    def scrape(self, 
                search_query: str = None,
                location: str = None,
                max_pages: int = None) -> List[Dict]:
        """Implementación completa para Computrabajo: listado + detalle con funciones privadas."""
        search_query = search_query or "data analyst"
        max_pages = max_pages or self.max_pages

        all_jobs: List[Dict] = []

        async def run():
            nonlocal all_jobs
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(user_agent=self.headers["User-Agent"])
                page = await context.new_page()

                page_number = 1
                while page_number <= max_pages:
                    self._respect_rate_limit()

                    # URL de listado (ajustada al formato real de Computrabajo AR)
                    url_list = f"{self.base_url}/trabajo-de-{search_query.replace(' ', '-')}/?p={page_number}"
                    if location:
                        url_list += f"&q={location.replace(' ', '+')}"

                    try:
                        await page.goto(url_list, timeout=30000)
                        links = await self._extract_links_from_page(page)

                        if not links:
                            self.logger.info(f"No hay más ofertas en página {page_number}")
                            break

                        self.logger.info(f"[{self.source_name}] Página {page_number}: {len(links)} enlaces encontrados")

                        for link in links:
                            detail_data = await self._scrape_job_detail(page, link)
                            all_jobs.append(detail_data)

                        page_number += 1

                    except Exception as e:
                        self.logger.warning(f"[{self.source_name}] Error en página {page_number}: {e}")
                        break

                await browser.close()

        asyncio.run(run())
        self.logger.info(f"[{self.source_name}] Scrape completado → {len(all_jobs)} ofertas")
        return all_jobs