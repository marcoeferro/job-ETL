import json
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright
from utils.save_to_JSON import save_to_JSON
from utils.logger import get_logger

logger = get_logger("zonajobs_scraper_playwright")


# ----------------------------
# FUNCIONES PURAS
# ----------------------------

def build_fetch_script(query, filtros, page=0, page_size=20):
    filtros_json = json.dumps(filtros, ensure_ascii=False)
    return f"""
        () => fetch("https://www.zonajobs.com.ar/api/avisos/searchV2?pageSize={page_size}&page={page}&sort=RELEVANTES", {{
            method: "POST",
            headers: {{
                "accept": "application/json",
                "content-type": "application/json",
                "x-site-id": "ZJAR"
            }},
            body: JSON.stringify({{
                filtros: {filtros_json},
                query: "{query}"
            }})
        }}).then(r => r.json())
    """


def parse_api_response(raw_result):
    if not raw_result:
        return None

    content = raw_result.get("content", None)
    if content is None or (isinstance(content, list) and len(content) == 0):
        return None

    return content


# ----------------------------
# GET DATA — ahora con Playwright
# ----------------------------

async def get_api_data_with_playwright(fetch_script):
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=False)
            page = await browser.new_page()

            await page.goto("https://www.zonajobs.com.ar/", timeout=60000)

            # Ejecutamos el fetch dentro del navegador
            result = await page.evaluate(fetch_script)

            await browser.close()
            return result

    except Exception as e:
        logger.error(f"Error ejecutando Playwright: {e}")
        print(f"Error ejecutando Playwright: {e}")
        return None


# ----------------------------
# MAIN
# ----------------------------

async def main(trabajo, filtros, page=0, page_size=20):

    logger.info(f"Iniciando scraping | trabajo='{trabajo}' | page={page}")
    print(f"Iniciando scraping | trabajo='{trabajo}' | page={page}")

    script = build_fetch_script(trabajo, filtros, page, page_size)

    raw_result = await get_api_data_with_playwright(script)
    cleaned = parse_api_response(raw_result)

    if cleaned is None:
        logger.warning(f"No hay más resultados en page={page}. Deteniendo scraping.")
        print(f"No hay más resultados en page={page}. Deteniendo scraping.")
        return False

    save_to_JSON(cleaned, f"zonajobs_{trabajo}_page_{page}.json")

    logger.info(f"Página {page} guardada correctamente ({len(cleaned)} resultados).")
    print(f"Página {page} guardada correctamente ({len(cleaned)} resultados).")

    return True


# ----------------------------
# PROGRAMA PRINCIPAL
# ----------------------------

async def run():
    filtros = []
    jobs = [
        "analista de datos","cientifico de datos","ingeniero de datos",
        "data analyst","data scientist","data engineer"
    ]
    max_pages = 5

    for trabajo in jobs:
        for page in range(max_pages):
            should_continue = await main(trabajo, filtros, page=page, page_size=20)

            if not should_continue:
                logger.info(f"Scraping finalizado para '{trabajo}' en page={page}.")
                print(f"Scraping finalizado para '{trabajo}' en page={page}.")
                break


if __name__ == "__main__":
    asyncio.run(run())