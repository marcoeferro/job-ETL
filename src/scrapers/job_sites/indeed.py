# src/scrapers/job_sites/indeed.py

from typing import List, Dict, Optional
import asyncio
import json
import re
import random

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

from src.scrapers.base_scraper import BaseJobScraper


class IndeedScraper(BaseJobScraper):
    def __init__(self):
        super().__init__("indeed")
        # Selectores ACTUALIZADOS 2026 (confirmados)
        self.job_card_selector = "div.job_seen_beacon"
        self.job_link_selector = 'a[href*="/rc/clk?jk="], a[href*="/viewjob?jk="]'

    def _log(self, msg: str):
        self.logger.info(f"[INDEED DEBUG] {msg}")

    def _normalize_url(self, url: str) -> str:
        match = re.search(r"jk=([a-zA-Z0-9]+)", url)
        if match:
            return f"{self.base_url}/viewjob?jk={match.group(1)}"
        return url

    def _is_blocked(self, html: str) -> bool:
        if not html:
            return True
        blocked_phrases = [
            "Solicitud bloqueada", "Te bloquearon", "Ray ID", "Cloudflare",
            "Blocked", "Access Denied", "captcha", "robot", "verificación de seguridad"
        ]
        html_lower = html.lower()
        return any(phrase.lower() in html_lower for phrase in blocked_phrases)

    # ===================== PARSER ACTUALIZADO =====================
    def _parse_job(self, html: str, url: str) -> Optional[Dict]:
        if not html or self._is_blocked(html):
            self.logger.warning(f"BLOCKED → {url}")
            return None

        soup = BeautifulSoup(html, "html.parser")
        data = self._extract_json_ld(soup, url)
        desc_elem = soup.select_one("#jobDescriptionText")

        if not data and not desc_elem:
            return None

        return {
            "title": data.get("title") or self._safe_text(
                soup, "h1[data-testid='jobsearch-JobInfoHeader-title']"
            ),
            "company": data.get("hiringOrganization", {}).get("name") or self._safe_text(
                soup, "a[data-testid='company-name']"
            ),
            "location": data.get("jobLocation", {}).get("address", {}).get("addressLocality") or self._safe_text(
                soup, "div[data-testid='jobsearch-JobInfoHeader-location']"
            ),
            "salary": None,
            "salary_currency": None,
            "job_type": data.get("employmentType"),
            "modality": None,
            "experience_level": [],
            "posted_date": data.get("datePosted"),
            "url": url,
            "description_raw": data.get("description") or (
                desc_elem.get_text("\n", strip=True) if desc_elem else None
            ),
            "tags": None,
            "skills": [],
            "source_site": "indeed",
            "applicants": None,
        }

    def _extract_json_ld(self, soup, url: str) -> Dict:
        try:
            script = soup.find("script", {"type": "application/ld+json"})
            return json.loads(script.text) if script else {}
        except:
            return {}

    def _safe_text(self, soup, selector: str) -> Optional[str]:
        try:
            el = soup.select_one(selector)
            return el.get_text(strip=True) if el else None
        except:
            return None

    # ===================== STEALTH + HUMAN BEHAVIOR =====================
    async def _human_behavior(self, page):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.3)")
            await asyncio.sleep(random.uniform(0.8, 1.8))
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.7)")
            await asyncio.sleep(random.uniform(1.0, 2.2))
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(random.uniform(1.2, 2.5))
        except:
            pass

    async def _safe_goto(self, page, url: str) -> bool:
        try:
            await page.goto(url, timeout=45000, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(2, 4))
            await self._human_behavior(page)
            return True
        except Exception as e:
            self.logger.error(f"Error goto {url}: {e}")
            return False

    # ===================== MAIN =====================
    def scrape(self, search_query=None, location=None, max_pages=None) -> List[Dict]:
        search_query = search_query or "data analyst"
        max_pages = max_pages or self.max_pages or 3
        location = location or "Argentina"

        async def run():
            stats = {"links": 0, "processed": 0, "blocked": 0, "parsed": 0}

            async with async_playwright() as pw:
                # ================== PROXY (OBLIGATORIO) ==================
                # Descomenta y pone tu proxy residencial argentino aquí:
                # proxy = {"server": "http://user:pass@ip:port"}
                proxy = {"server": "socks4://181.15.154.156:5203"}   # ← probá este primero

                browser = await pw.chromium.launch(headless=False)

                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
                    viewport={"width": 1366, "height": 768},
                    locale="es-AR",
                    timezone_id="America/Argentina/Buenos_Aires",
                    proxy=proxy,                    # ← activado
                    bypass_csp=True,
                    ignore_https_errors=True
                )

                page = await context.new_page()

                all_links = []

                # 1. EXTRACCIÓN DE LINKS (selector correcto 2026)
                for p in range(max_pages):
                    start = p * 10
                    url = f"{self.base_url}/jobs?q={search_query.replace(' ', '+')}&l={location.replace(' ', '+')}&start={start}"

                    self._log(f"Buscando página {p+1} → {url}")
                    await self._safe_goto(page, url)

                    try:
                        await page.wait_for_selector(self.job_card_selector, timeout=15000)
                        links = await page.locator(self.job_link_selector).evaluate_all(
                            "nodes => nodes.map(n => n.href)"
                        )
                        all_links.extend(links)
                        self._log(f"→ Encontrados {len(links)} links en página {p+1}")
                    except Exception as e:
                        self.logger.warning(f"No se encontraron jobs en página {p+1}: {e}")
                        break

                    await asyncio.sleep(random.uniform(4, 7))

                all_links = list(set([self._normalize_url(u) for u in all_links]))
                stats["links"] = len(all_links)
                self._log(f"TOTAL LINKS: {len(all_links)}")

                # 2. DETALLES (secuencial + lento para no ser baneado más)
                results = []
                for url in all_links:
                    try:
                        stats["processed"] += 1
                        self._log(f"Procesando → {url}")
                        await self._safe_goto(page, url)
                        await page.wait_for_timeout(random.randint(4000, 7000))

                        html = await page.content()

                        if self._is_blocked(html):
                            stats["blocked"] += 1
                            self.logger.warning(f"BLOQUEADO (IP aún banneada) → {url}")
                            continue

                        job = self._parse_job(html, url)
                        if job:
                            stats["parsed"] += 1
                            results.append(job)

                    except Exception as e:
                        self.logger.error(f"Error detalle {url}: {e}")

                await browser.close()
                self._log(f"STATS FINALES: {stats}")
                return results

        results = asyncio.run(run())
        self.logger.info(f"[indeed] RESULTADOS FINALES: {len(results)}")
        return results