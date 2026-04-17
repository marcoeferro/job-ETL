import asyncio
from playwright.async_api import async_playwright
import re

BASE_URL = "https://ar.computrabajo.com"


# ----------------------------------------------------
# UTILS
# ----------------------------------------------------
def sanitize_filename(url: str) -> str:
    """
    Sanitize a string to be used as a safe filename.

    Replaces unsafe characters with underscores and trims
    the final result to avoid extremely long filenames.

    Args:
        url (str): Original URL or string to sanitize.

    Returns:
        str: A sanitized, filesystem-safe filename.
    """
    return re.sub(r"[^a-zA-Z0-9_-]", "_", url)[:200]


# ----------------------------------------------------
# SCROLL PARA LAZY-LOADING
# ----------------------------------------------------
async def auto_scroll(page):
    """
    Auto-scroll a Playwright page until no new content loads.

    Many job sites use lazy-loading to render job cards
    as the user scrolls. This function keeps scrolling
    until the page stops increasing in height.

    Args:
        page (playwright.async_api.Page): The page to scroll.

    Returns:
        None
    """
    previous_height = await page.evaluate("document.body.scrollHeight")
    while True:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1200)

        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == previous_height:
            break
        previous_height = new_height


# ----------------------------------------------------
# SCRAPE LINKS DE UNA PÁGINA
# ----------------------------------------------------
async def extract_links_from_page(page):
    """
    Extract job offer links from a single search results page.

    Performs lazy-loading scroll, waits for job cards to appear,
    and extracts all anchor tags that match Computrabajo’s job selector.

    Args:
        page (playwright.async_api.Page): Current browser page.

    Returns:
        list[str]: Unique list of offer URLs extracted.
    """
    await page.wait_for_timeout(1500)
    await auto_scroll(page)

    try:
        await page.wait_for_selector("a.js-o-link.fc_base", timeout=6000)
    except:
        print("[WARN] No aparecieron ofertas.")
        return []

    links = await page.locator("a.js-o-link.fc_base").evaluate_all(
        "nodes => nodes.map(n => n.href)"
    )

    return list(set(links))


# ----------------------------------------------------
# SCRAPE LINKS DE COMPUTRABAJO
# ----------------------------------------------------
async def get_links(query: str, browser) -> list:
    """
    Paginate through Computrabajo search results and extract all job links.

    Continues visiting pages until a page yields zero job offers
    (Tiger-style pagination stop condition).

    Args:
        query (str): Job title to search (e.g. 'data-scientist').

    Returns:
        list[str]: Deduplicated list of URLs for all job offers found.
    """
    print(f"\n=== SCRAPING: {query} ===")

    all_links = []
    page_number = 1

    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        ),
        locale="es-AR",
        viewport={"width": 1400, "height": 900}
    )

    page = await context.new_page()

    while True:
        url = f"{BASE_URL}/trabajo-de-{query}?p={page_number}"

        resp = await page.goto(url, timeout=30000)
        assert resp.status < 400, f"[ASSERT] HTTP {resp.status}"

        # Extraer links
        links = await extract_links_from_page(page)

        # Tiger-style -> cortar apenas no haya ofertas
        if len(links) == 0:
            print("[STOP] No hay más ofertas. Fin del paginado.")
            break

        all_links.extend(links)
        page_number += 1
        await browser.close()

    print(f"\n[TOTAL LINKS] {len(set(all_links))}")
    return list(set(all_links))


# ----------------------------------------------------
# SCRAPE DETALLES (VISITAR CADA LINK)
# ----------------------------------------------------
async def scrape_offer_details(url, save_html, browser):
    """
    Visit a specific job offer page, extract raw HTML, and save it.

    Args:
        url (str): URL of the job offer page.
        save_html (Callable): Function that receives (url, html_content) 
        and performs persistence (save to file, db, etc).

    Returns:
        None
    """
    try:
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/119.0.0.0 Safari/537.36"
            ),
            locale="es-AR",
            viewport={"width": 1400, "height": 900}
        )
        page = await context.new_page()

        resp = await page.goto(url, timeout=25000)
        assert resp.status < 400, f"[ASSERT] Oferta {url} devolvió HTTP {resp.status}"

        await page.wait_for_timeout(1500)

        html = await page.content() 
        content_with_link = f'<div id="LINK">{url}</div>\n{html}'

        await save_html(content_with_link, f"computrabajo_{str(hash(sanitize_filename(url)))}.html")

        print(f"[OK] Guardado: {url}")

        await browser.close()
    except Exception as e:
        print(f"[ERR] {url} -> {e}")


# ----------------------------------------------------
# CONCURRENCIA PARA OFERTAS
# ----------------------------------------------------
async def scrape_all_details(save_html, urls: list, browser, concurrency: int = 8):
    """
    Scrape HTML for each job offer concurrently with a limited number of workers.

    Args:
        save_html (Callable): Function to save HTML content.
        urls (list[str]): List of job offer URLs to scrape.
        concurrency (int): Max number of concurrent browser workers.

    Returns:
        None
    """
    print(f"\n[DETAILS] scrapeando {len(urls)} páginas...")

    sem = asyncio.Semaphore(concurrency)

    async def worker(url):
        async with sem:
            await scrape_offer_details(url, save_html, browser)

    await asyncio.gather(*(worker(u) for u in urls))


# ----------------------------------------------------
# MAIN
# ----------------------------------------------------
async def extractor_computrabajo(save_html, job_title: str):
    """
    Main entrypoint for the Computrabajo extractor.

    Given a job title, fetch all job offer links for it,
    then scrape and store their raw HTML pages.

    Args:
        save_html (Callable): Function (url, html) → None to persist HTML.
        job_title (str): Job keyword to scrape.

    Returns:
        None
    """
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        links = await get_links(job_title, browser)

        if links:
            await scrape_all_details(save_html, links, browser, concurrency=8)
    print("\n🚀 COMPLETADO")


if __name__ == "__main__":
    asyncio.run(extractor_computrabajo())
