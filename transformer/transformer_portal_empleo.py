import os
from pathlib import Path
from typing import Optional, Dict, List
from concurrent.futures import ProcessPoolExecutor, as_completed
from bs4 import BeautifulSoup

# ===========================
#   HTML HELPERS
# ===========================

def get_html_document(document_path: Path) -> BeautifulSoup:
    """
    Abre un archivo HTML y devuelve el objeto BeautifulSoup.

    Args:
        document_path (Path): Ruta del archivo HTML.

    Returns:
        BeautifulSoup: Árbol DOM parseado.

    Raises:
        AssertionError: Si el archivo no existe o está vacío.
    """
    assert document_path.exists(), f"[ERROR] Archivo no encontrado: {document_path}"
    assert document_path.stat().st_size > 5, f"[ERROR] Archivo vacío o corrupto: {document_path}"

    with open(document_path, "r", encoding="utf-8") as file:
        html_doc = file.read()

    soup = BeautifulSoup(html_doc, "html.parser")
    assert soup is not None, "[ERROR] BeautifulSoup devolvió None"

    return soup

def get_link(soup: BeautifulSoup) -> Optional[str]:
    tag = soup.select_one("#LINK")
    return tag.get_text(strip=True) if tag else None

def dispose_garbage(soup: BeautifulSoup) -> BeautifulSoup:
    """
    Devuelve el bloque principal de información.

    Tigerstyle: si no existe, aborta el procesamiento del archivo.

    Args:
        soup (BeautifulSoup): Documento HTML completo.

    Returns:
        BeautifulSoup: Fragmento relevante del HTML.

    Raises:
        AssertionError: Si el contenedor principal no existe.
    """
    block = soup.find("div", class_="container-fluid container-footer-0")
    assert block is not None, "[STOP] No se encontró el contenedor principal del portal."
    return block


def get_job_title(soup: BeautifulSoup) -> Optional[str]:
    tag = soup.find("h1", class_="text-capitalize text-turqueza")
    return tag.get_text(strip=True) if tag else None


def get_business_name(soup: BeautifulSoup) -> Optional[str]:
    """
    Col-12 aparece varias veces.
    
    Tigerstyle: se revisa .first de forma defensiva.
    """
    tag = soup.find("div", class_="col-12")
    if tag:
        return tag.get_text(strip=True)
    return None


def get_location(soup: BeautifulSoup) -> Optional[str]:
    """
    Toma el div.col-12[3].

    Tigerstyle: si no existe el índice, devuelve None pero avisa.
    """
    tags = soup.select("div.col-12")
    if len(tags) <= 3:
        print("[WARN] No se encontró el índice 3 para ubicación.")
        return None

    return tags[3].get_text(strip=True)



def get_description(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    """
    Extrae campos específicos por índice.

    Tigerstyle: si un índice no existe, simplemente se ignora.
    """
    tags = soup.select("div.row.p-2")
    description = {}

    index_map = {
        1: "Oferta",
        3: "Tareas a Realizar",
        5: "Detalles",
        7: "Requisitos",
    }

    for idx, key in index_map.items():
        if idx < len(tags):
            description[key] = tags[idx].get_text(strip=True)
        else:
            print(f"[WARN] Campo '{key}' no encontrado en índice {idx}.")

    return description if description else None


def get_publication_time(soup: BeautifulSoup) -> Optional[str]:
    tag = soup.find("div", class_="float-right mt-2")
    return tag.get_text(strip=True) if tag else None


# ===========================
#   TRANSFORMACIÓN
# ===========================

def transform_data(document_path: Path) -> Dict:
    """
    Extrae contenido relevante de un archivo HTML del portal Empleo.

    Args:
        document_path (Path): Ruta del archivo HTML.

    Returns:
        dict: Registro limpio para guardar en JSON.
    """
    soup = get_html_document(document_path)
    link = get_link(soup)
    cleaned = dispose_garbage(soup)

    return {
        "link": link,
        "title": get_job_title(cleaned),
        "publication_time": get_publication_time(cleaned),
        "business_name": get_business_name(cleaned),
        "location": get_location(cleaned),
        "description": get_description(cleaned),
        "source_file": document_path.name,
    }


# ===========================
#   EXECUTOR PARALELO
# ===========================

def portal_empleo_transformer(save_func) -> List[Dict]:
    """
    Procesa en paralelo todos los HTML de portalempleo en /data/raw.

    Tigerstyle:
    - límites de seguridad
    - logs explícitos
    - asserts en cada paso
    - paralelización completa
    - carpeta cleaned creada automáticamente

    Args:
        save_func (callable): Función para guardar cada JSON.

    Returns:
        List[dict]: Listado de dicts procesados.
    """
    base = Path(__file__).resolve().parent.parent
    raw_folder = base / "data" / "raw"
    cleaned_folder = base / "data" / "cleaned"

    cleaned_folder.mkdir(parents=True, exist_ok=True)

    prefix = "portalempleo"
    html_files = [
        f for f in raw_folder.iterdir()
        if f.is_file() and f.suffix == ".html" and f.name.startswith(prefix)
    ]

    assert len(html_files) > 0, "[STOP] No hay archivos HTML para procesar."

    print(f"[INFO] Archivos detectados: {len(html_files)}")

    # Seguridad: límite de 5000 archivos
    assert len(html_files) <= 5000, "[STOP] Demasiados archivos. Revise la carpeta."

    results = []

    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(transform_data, file): file
            for file in html_files
        }

        for future in as_completed(futures):
            file = futures[future]
            try:
                data = future.result()
                results.append(data)

                output_file = cleaned_folder / f"{file.stem}.json"
                
                save_func(data = data, path=str(output_file))

                print(f"[OK] Guardado: {output_file}")

            except AssertionError as e:
                print(f"[FAIL] Error en archivo {file}: {e}")
            except Exception as e:
                print(f"[CRITICAL] Excepción inesperada en {file}: {e}")

    return results
