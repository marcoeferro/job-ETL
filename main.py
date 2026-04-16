# from playwright.sync_api import Playwright, sync_playwright
from utils.save_to_html import save_html
from utils.save_to_JSON import save_to_JSON
from extractors.extractor_portal_de_empleo import extract_portal_empleo
from extractors.scraper_get_on_board import extract_get_on_board
from transformer.transformer_computrabajo import computrabajo_transformer
from transformer.transformer_portal_empleo import portal_empleo_transformer
from transformer.transformer_get_on_board import get_on_board_transformer
import sys
import asyncio



# --------------------------
# EJEMPLO DE EJECUCIÓN
# --------------------------
if __name__ == "__main__":
    # # ejemplo rápido cuando se ejecuta como script (ajusta job_titles)
    # example_jobs = ["python developer", "data scientist", "product manager"]

    # # Import save_html para pasar solo como verificación (no se envía a procesos)
    # # Nota: worker_process importará utils.save_to_html en el proceso hijo.
    # async def _main():
    #     try:
    #         from utils.save_to_html import save_html  # async def save_html(content, path)
    #     except Exception as e:
    #         print(f"[ERROR] No se puede importar utils.save_to_html: {e}", file=sys.stderr)
    #         return

    #     stats = await extract_get_on_board(save_html, example_jobs)
    #     print("\nFinished. Stats:", stats)

    # asyncio.run(_main())

    # transformed_data = portal_empleo_transformer(save_func=save_to_JSON)
    transformed_data = get_on_board_transformer(save_func=save_to_JSON)