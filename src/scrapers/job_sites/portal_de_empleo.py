# portal_empleo.py
import asyncio
from typing import List, Callable
from concurrent.futures import ProcessPoolExecutor
from playwright.async_api import async_playwright, Page
from typing import List, Dict, Set

# ============================================================
BASE_SEARCH_URL = "https://portalempleo.gob.ar/OfertasLaborales"


# ============================================================
# UTIL: SCROLL (ASYNC)
# ============================================================
async def auto_scroll(page: Page) -> None:
    """
    Scroll lazy-loading hasta que no cambie la altura del documento.

    Args:
        page (Page): Instancia Playwright Page.
    """
    previous_height = await page.evaluate("document.body.scrollHeight")
    while True:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(350)
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == previous_height:
            break
        previous_height = new_height


# ============================================================
# UTIL: NORMALIZAR URL
# ============================================================
def normalize_url(href: str) -> str:
    """
    Convierte una URL relativa de Portal Empleo en absoluta.

    Args:
        href (str): Enlace relativo o absoluto.

    Returns:
        str: URL absoluta normalizada.
    """
    if not href:
        return href
    return href if href.startswith("http") else f"https://portalempleo.gob.ar{href}"


# ============================================================
# SCRAPER: BUSCAR LINKS DE OFERTAS
# ============================================================
async def extract_job_links(job_title: str) -> List[str]:
    """
    Realiza la búsqueda en Portal Empleo y extrae los links de ofertas
    para la palabra clave dada.

    Args:
        job_title (str): Puesto a buscar.

    Returns:
        List[str]: Lista de URLs de ofertas encontradas.
    """

    search_url = f"{BASE_SEARCH_URL}?Q={job_title.replace(' ', '+')}"
    links = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(search_url, timeout=45000)
        await auto_scroll(page)  # Permite cargar todas las tarjetas vía lazy-load

        anchors = await page.locator("a.btn.btn-success.comp-button-ciudadanos").evaluate_all(
            "nodes => nodes.map(n => n.getAttribute('href'))"
        )

        for href in anchors:
            links.append(normalize_url(href))

        await context.close()
        await browser.close()

    return links


# ============================================================
# SCRAPER: EXTRAER UNA OFERTA
# ============================================================
async def extract_offer(url: str, context, save_html: Callable):
    """
    Descarga el HTML de una oferta y lo guarda usando save_html.

    Args:
        url (str): Enlace a la oferta.
        context: Playwright BrowserContext.
        save_html (Callable): Función asíncrona para guardar el HTML.
    """

    url = normalize_url(url)
    page = await context.new_page()

    try:
        await page.goto(url, timeout=30000)
        html = await page.content()

        await save_html(
            f'<div id="LINK">{url}</div>\n{html}',
            f"portalempleo_{hash(url)}.html"
        )

    finally:
        await page.close()


# ============================================================
# SCRAPER: PROCESAR MUCHAS OFERTAS EN PARALELO (ASYNC)
# ============================================================
async def extract_offers_concurrently(urls: List[str], save_html: Callable):
    """
    Abre un navegador y extrae múltiples ofertas en paralelo
    usando async + gather.

    Args:
        urls (List[str]): Lista de URLs de ofertas.
        save_html (Callable): Función asíncrona para guardar HTML.
    """

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()

        tasks = [
            extract_offer(url, context, save_html)
            for url in urls
        ]

        await asyncio.gather(*tasks, return_exceptions=False)

        await context.close()
        await browser.close()


# ============================================================
# MULTIPROCESO: WORKER
# ============================================================
def worker_process(urls_chunk: List[str]):
    """
    Worker multiproceso que procesa un batch de URLs.

    Nota: No puede recibir funciones async como save_html directamente porque
    multiprocess no puede serializarlas (pickle).

    En su lugar importamos la función dentro del worker.

    Args:
        urls_chunk (List[str]): Lista parcial de URLs de ofertas.

    Returns:
        str: Mensaje de finalización del worker.
    """
    from utils.save_to_html import save_html  # Import seguro dentro del proceso

    asyncio.run(extract_offers_concurrently(urls_chunk, save_html))
    return f"Worker procesó {len(urls_chunk)} ofertas"


# ============================================================
# MASTER: EJECUTAR MULTIPROCESO + ASYNC
# ============================================================
def run_parallel(urls: List[str], workers: int = 4):
    """
    Divide las URLs en chunks y ejecuta cada chunk en un proceso separado,
    permitiendo paralelismo real (multiproceso + async Playwright).

    Args:
        urls (List[str]): Lista de URLs a descargar.
        workers (int): Número de procesos paralelos.
    """

    if not urls:
        print("No hay URLs para procesar.")
        return

    chunk_size = max(1, len(urls) // workers)
    chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]

    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(worker_process, chunk) for chunk in chunks]

        for f in futures:
            print(f.result())


# ============================================================
# ORQUESTADOR PRINCIPAL
# ============================================================
async def extract_portal_empleo(job_titles: List[str], workers: int = 4) -> Dict:
    """
    Orquesta todo el flujo de scraping de Portal Empleo:
        1. Ejecuta búsquedas para cada término.
        2. Extrae todos los links de ofertas por trabajo.
        3. Deduplica los links.
        4. Procesa todas las ofertas usando multiproceso + async.

    Args:
        job_titles (List[str]):
            Lista de trabajos a buscar (ej: ["python", "data analyst"]).

        workers (int):
            Cantidad de procesos paralelos (1 por CPU recomendado).

    Returns:
        Dict: Estadísticas del scraping:
            {
                "jobs": [...],
                "total_links": int,
                "unique_links": int,
                "duplicates_removed": int,
            }
    """
    print("\n========== PORTAL EMPLEO SCRAPER ==========\n")

    all_links: List[str] = []

    # 1. Buscar links para cada job (async sequential o parallelizable)
    print("[INFO] Buscando links para cada término...")
    for job in job_titles:
        print(f" → Buscando: {job}")
        links = await extract_job_links(job)
        print(f"   - {len(links)} links encontrados")
        all_links.extend(links)

    # 2. Deduplicar
    print("\n[INFO] Deduplicando links...")
    unique_links: Set[str] = set(all_links)

    print(f" → Total encontrados: {len(all_links)}")
    print(f" → Únicos: {len(unique_links)}")
    print(f" → Duplicados eliminados: {len(all_links) - len(unique_links)}")

    if len(unique_links) == 0:
        print("[STOP] No se encontraron enlaces para procesar.")
        return {
            "jobs": job_titles,
            "total_links": len(all_links),
            "unique_links": 0,
            "duplicates_removed": 0,
        }

    # 3. Procesar las ofertas usando multiproceso + async
    print("\n[INFO] Procesando ofertas en paralelo...")
    run_parallel(list(unique_links), workers)

    # 4. Devolver stats
    return {
        "jobs": job_titles,
        "total_links": len(all_links),
        "unique_links": len(unique_links),
        "duplicates_removed": len(all_links) - len(unique_links),
    }