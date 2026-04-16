# getonboard_scraper.py
"""
Scraper para GetOnBoard con:
 - múltiples BrowserContext (por búsqueda / por worker)
 - multiproceso real (ProcessPoolExecutor) para paralelismo
 - TigerStyle programming: asserts, stops, retries, semáforos
 - type hints y docstrings
 - dependencia: async def save_html(content: str, path: str) -> None
   ubicada en utils/save_to_html.py (importada dentro del worker)
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import Callable, List, Set, Coroutine, Optional
from playwright.async_api import async_playwright, Browser, Page
import math
import os
import sys
import time

# ---------- CONFIG ----------
BASE_URL = "https://www.getonbrd.com"
DEFAULT_CONCURRENCY = 6  # concurrency por proceso (cantidad de tabs/contextos concurrentes)
DEFAULT_WORKERS = 4      # procesos en paralelo en run_parallel
SEARCH_TIMEOUT = 45000   # ms
NAV_TIMEOUT = 30000      # ms
RETRY_ATTEMPTS = 2
RETRY_BACKOFF = 0.8      # seconds
SCROLL_PAUSE_MS = 350
MAX_LINKS_PER_JOB = 1000  # stop de seguridad por job (TigerStyle: stops)
# ----------------------------


# --------------------------
# UTIL: SCROLL (ASYNC)
# --------------------------
async def auto_scroll(page: Page) -> None:
    """
    Realiza lazy-loading scroll hasta que no cambie la altura del documento.

    Args:
        page: Playwright Page
    """
    assert page is not None, "page no puede ser None"
    previous_height = await page.evaluate("() => document.body.scrollHeight")
    # TigerStyle stop guard
    max_cycles = 120  # failsafe to avoid infinite loops
    cycles = 0

    while True:
        cycles += 1
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(SCROLL_PAUSE_MS)
        new_height = await page.evaluate("() => document.body.scrollHeight")
        if new_height == previous_height or cycles >= max_cycles:
            break
        previous_height = new_height


# --------------------------
# UTIL: NORMALIZAR URL
# --------------------------
def normalize_url(href: Optional[str]) -> Optional[str]:
    """
    Normaliza href relativo a URL absoluta de GetOnBoard.

    Args:
        href: href extraído de atributos

    Returns:
        URL absoluta o None si href es falsy.
    """
    if not href:
        return None
    return href if href.startswith("http") else f"{BASE_URL}{href}"


# --------------------------
# EXTRACT LINKS (async, multi-context)
# --------------------------
async def extract_links_for_job(browser: Browser, job_title: str) -> List[str]:
    """
    Extrae enlaces de ofertas para un job_title usando un Browser ya abierto.
    Crea su propio BrowserContext para aislamiento.

    Args:
        browser: instancia Playwright Browser (ya lanzada).
        job_title: término de búsqueda.

    Returns:
        Lista de URLs absolutas (puede estar vacía).
    """
    assert browser is not None, "browser no puede ser None"
    assert isinstance(job_title, str) and job_title.strip(), "job_title debe ser string no vacío"

    ctx = await browser.new_context()
    page = await ctx.new_page()

    links: List[str] = []
    try:
        await page.goto(BASE_URL, timeout=SEARCH_TIMEOUT)
        # Rellenar el input #search_term y presionar Enter (igual que tu versión)
        try:
            await page.locator("#search_term").fill(job_title)
            await page.keyboard.press("Enter")
        except Exception:
            # Si el selector cambia o no está, fallamos rápido (TigerStyle assert)
            print(f"[WARN] No se pudo llenar #search_term para: {job_title}", file=sys.stderr)

        # Esperar resultados
        try:
            await page.wait_for_selector("ul.gb-results-list", timeout=SEARCH_TIMEOUT)
        except Exception:
            # Si no hay selector, devolvemos lista vacía para continuar
            print(f"[WARN] No se encontró 'ul.gb-results-list' para {job_title}", file=sys.stderr)
            return []

        await auto_scroll(page)

        # Obtenemos anchors y normalizamos
        hrefs = await page.locator("ul.gb-results-list a").evaluate_all(
            "nodes => nodes.map(n => n.getAttribute('href'))"
        )

        # stops y defensiva: limitar cantidad por job
        if hrefs:
            for href in hrefs[:MAX_LINKS_PER_JOB]:
                n = normalize_url(href)
                if n:
                    links.append(n)

    finally:
        await ctx.close()

    # invariantes TigerStyle
    assert isinstance(links, list)
    return links


# --------------------------
# SCRAPE DETAIL (async)
# --------------------------
async def scrape_offer_detail(page: Page, link: str, save_html: Callable[[str, str], Coroutine]) -> None:
    """
    Visita una oferta (link) y utiliza save_html async para guardarla.

    Args:
        page: Playwright Page (ya abierto)
        link: URL absoluta
        save_html: función async save_html(content: str, path: str) -> None
    """
    assert page is not None
    assert link and isinstance(link, str)
    assert callable(save_html)

    # retries ligeros
    last_exc = None
    for attempt in range(RETRY_ATTEMPTS + 1):
        try:
            await page.goto(link, timeout=NAV_TIMEOUT)
            # selector conocido en tu versión: "#right-col"
            try:
                await page.wait_for_selector("#right-col", timeout=8000)
                content = await page.locator("#right-col").inner_html()
            except Exception:
                # fallback a page.content() si no existe el selector
                content = await page.content()

            wrapped = f'<div id="LINK">{link}</div>\n{content}'
            filename = f"get_on_board_{abs(hash(link))}.html"
            # save_html es async según dijiste
            await save_html(wrapped, filename)
            return
        except Exception as e:
            last_exc = e
            if attempt < RETRY_ATTEMPTS:
                await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
            else:
                print(f"[ERROR] scrape_offer_detail failed for {link}: {e}", file=sys.stderr)
    # Si llegamos aquí, todas las reintentos fallaron
    raise last_exc


# --------------------------
# CHILD-PROCESS: async worker that processes MANY URLs (runs inside each process)
# --------------------------
async def extract_offers_concurrently(urls: List[str], save_html_func: Callable[[str, str], Coroutine], concurrency: int = DEFAULT_CONCURRENCY) -> None:
    """
    En un proceso: lanza Playwright y procesa una lista de URLs concurrentemente.

    Args:
        urls: lista de URLs a procesar
        save_html_func: función async save_html(content, path)
        concurrency: semáforo para limitar tabs simultáneos
    """
    assert isinstance(urls, list)
    if not urls:
        return

    sem = asyncio.Semaphore(concurrency)
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        # NO creamos un context por tarea aquí; cada tarea creará su contexto para mayor aislamiento
        async def worker(url: str) -> None:
            retries = 1
            for attempt in range(retries + 1):
                try:
                    async with sem:
                        ctx = await browser.new_context()
                        page = await ctx.new_page()
                        try:
                            await scrape_offer_detail(page, url, save_html_func)
                        finally:
                            # cerrar page/context siempre
                            try:
                                await page.close()
                            except Exception:
                                pass
                            try:
                                await ctx.close()
                            except Exception:
                                pass
                    return
                except Exception as e:
                    if attempt < retries:
                        await asyncio.sleep(0.6)
                    else:
                        print(f"[WARN] worker failed for {url}: {e}", file=sys.stderr)

        # fire all tasks
        tasks = [worker(u) for u in urls]
        # TigerStyle: limitar gather a evitar memory blowup
        # si hay demasiadas URLs, procesamos en batches
        BATCH_SIZE = max(50, concurrency * 10)
        for i in range(0, len(tasks), BATCH_SIZE):
            batch = tasks[i:i + BATCH_SIZE]
            await asyncio.gather(*batch, return_exceptions=False)

        await browser.close()


# --------------------------
# PROCESS worker entrypoint (runs in separate OS process)
# --------------------------
def worker_process(urls_chunk: List[str]) -> str:
    """
    Punto de entrada que se ejecuta en cada proceso del ProcessPoolExecutor.

    Importante: importamos save_html dentro del proceso (no se serializa).
    Ejecuta un loop asyncio que lanza Playwright localmente en ese proceso.

    Args:
        urls_chunk: lista parcial de URLs que este worker debe procesar.

    Returns:
        Mensaje de reporte.
    """
    # import dentro del proceso
    try:
        from utils.save_to_html import save_html  # async def save_html(content: str, path: str)
    except Exception as e:
        return f"[ERROR worker] no se pudo importar save_html: {e}"

    try:
        # ejecutamos el loop async en el proceso
        asyncio.run(extract_offers_concurrently(urls_chunk, save_html))
        return f"[OK worker] procesadas {len(urls_chunk)} ofertas"
    except Exception as e:
        return f"[ERROR worker] fallo procesando chunk ({len(urls_chunk)}): {e}"


# --------------------------
# MASTER: run_parallel (divide y conquista con ProcessPoolExecutor)
# --------------------------
def run_parallel(urls: List[str], workers: int = DEFAULT_WORKERS) -> None:
    """
    Divide las URLs en chunks y lanza varios procesos para procesarlas.

    Args:
        urls: lista completa de URLs
        workers: cantidad de procesos paralelos (1..n)
    """
    assert isinstance(urls, list)
    if not urls:
        print("[STOP] No hay URLs para procesar.")
        return

    workers = max(1, int(workers))
    # chunking defensivo: repartir lo más equitativo posible
    n = len(urls)
    chunk_size = math.ceil(n / workers)
    chunks = [urls[i:i + chunk_size] for i in range(0, n, chunk_size)]

    with ProcessPoolExecutor(max_workers=min(workers, len(chunks))) as ex:
        futures = [ex.submit(worker_process, chunk) for chunk in chunks]
        for f in futures:
            try:
                res = f.result()
                print(res)
            except Exception as e:
                print(f"[ERROR run_parallel] future failed: {e}", file=sys.stderr)


# --------------------------
# ORQUESTADOR PRINCIPAL (async)
# --------------------------
async def extract_get_on_board(save_html_callback: Callable[[str, str], Coroutine], job_titles: List[str], workers: int = DEFAULT_WORKERS) -> dict:
    """
    Orquesta:
     1) Ejecuta búsquedas para cada job_title (usando contextos separados)
     2) Deduplica links
     3) Lanza multiproceso para descargar detalles (cada proceso crea su propio Playwright)

    Args:
        save_html_callback: función async save_html(content: str, path: str) -> None
        job_titles: lista de strings con términos de búsqueda
        workers: cantidad de procesos a usar para la fase de detalle

    Returns:
        Estadísticas dict
    """
    # preconditions (TigerStyle)
    assert callable(save_html_callback), "save_html_callback debe ser callable async"
    assert isinstance(job_titles, list) and job_titles, "job_titles debe ser una lista no vacía"

    # usamos un solo async_playwright en el proceso maestro para las búsquedas
    all_links: List[str] = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        # Crear tasks por job (cada uno crea su propio context)
        search_tasks = [extract_links_for_job(browser, jt) for jt in job_titles]
        # Limitar el gather por batches por seguridad (TigerStyle)
        BATCH = 8
        results: List[List[str]] = []
        for i in range(0, len(search_tasks), BATCH):
            batch = search_tasks[i:i + BATCH]
            batch_res = await asyncio.gather(*batch, return_exceptions=False)
            results.extend(batch_res)

        await browser.close()

    # combinar y deduplicar
    unique_links: Set[str] = set()
    for lst in results:
        if lst:
            for l in lst:
                unique_links.add(l)

    total_found = sum(len(lst) for lst in results)
    print(f"🔎 Total encontrados (incl. duplicados): {total_found}")
    print(f"🔎 Unicos para procesar: {len(unique_links)}")

    if len(unique_links) == 0:
        print("[STOP] No hay enlaces únicos para procesar.")
        return {
            "jobs": job_titles,
            "total_links": total_found,
            "unique_links": 0,
            "duplicates_removed": total_found,
        }

    # Preparar lista para pasar a multiproceso
    to_process = list(unique_links)

    # Ejecutar multiproceso: cada proceso importará utils.save_to_html internamente
    print("[INFO] Enviando a procesos worker...")
    run_parallel(to_process, workers=workers)

    return {
        "jobs": job_titles,
        "total_links": total_found,
        "unique_links": len(unique_links),
        "duplicates_removed": total_found - len(unique_links),
    }
