import asyncio
from pathlib import Path


async def save_html(content: str, path: str) -> None:
    """
    Guarda contenido HTML en un archivo dentro de 'data/raw'
    de forma asíncrona usando asyncio.to_thread().

    Esta función es TigerStyle: pura, sin efectos globales,
    y evita bloquear el event loop moviendo todo el I/O a un thread.

    Args:
        content (str): HTML a guardar.
        path (str): Ruta relativa donde guardar el archivo.
    """

    def write_file():
        base_path = Path(path).parent.parent / "data" / "raw"
        base_path.mkdir(parents=True, exist_ok=True)

        final_path = base_path / path
        final_path.parent.mkdir(parents=True, exist_ok=True)

        with open(final_path, "w", encoding="utf-8") as file:
            file.write(content)

        return final_path

    try:
        final_path = await asyncio.to_thread(write_file)
        print(f"Saved HTML page to {final_path}")

    except Exception as e:
        print(f"Error saving HTML to {path}: {e}")
