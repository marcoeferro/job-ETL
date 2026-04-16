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

#revisar
def get_job_title(soup: BeautifulSoup) -> Optional[str]:
    tag = soup.find("span", itemprop="title")
    return tag.get_text(strip=True) if tag else None

#revisar
def get_business_name(soup: BeautifulSoup) -> Optional[str]:
    """
    Col-12 aparece varias veces.
    
    Tigerstyle: se revisa .first de forma defensiva.
    """
    tag = soup.find("span", class_="fake-hidden size-3")
    if tag:
        return tag.get_text(strip=True)
    return None

def get_description(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    """
    Extrae campos específicos por índice.

    Tigerstyle: si un índice no existe, simplemente se ignora.
    """
    tags = soup.select("div.mb4")
    description = {}

    index_map = {
        1: "Funciones del cargo",
        2: "Requerimientos del cargo",
        3: "Beneficios",
        4: "Conditions",
    }

    for idx, key in index_map.items():
        description[key] = tags[idx].div.get_text(strip=True)
    return description if description else None

#revisar
def get_job_location(soup: BeautifulSoup) -> Optional[str]:
    tag = soup.find("span", attrs={"itemprop": "jobLocation"})
    return tag.get_text(strip=True) if tag else None

#revisar
def get_job_seniority(soup: BeautifulSoup) -> Optional[str]:
    tag = soup.find("span", attrs={"itemprop": "qualifications"})
    return tag.get_text(strip=True) if tag else None

#revisar
def get_applications_count(soup: BeautifulSoup) -> Optional[int]:
    tag = soup.find("div", class_="size0 mt1")
    if tag :
        tag = tag.get_text(strip=True) 
        return tag.split("applications")[0]
    else :
        return None

#revisar
def get_all_tags(soup: BeautifulSoup) -> Optional[List[str]]:
    tag = soup.find_all("a", class_="gb-tags__item")
    return [tag.get_text(strip=True) for tag in tag] if tag else None

#revisar
def get_work_schedule(soup: BeautifulSoup) -> Optional[str]:
    tag = soup.find("span", attrs={"itemprop": "employmentType"})
    return tag.get_text(strip=True) if tag else None

#revisar
def get_publication_time(soup: BeautifulSoup) -> Optional[str]:
    tag = soup.time
    return tag.get_text(strip=True) if tag else None


# ===========================
#   TRANSFORMACIÓN
# ===========================

def transform_data(document_path: Path) -> Dict:
    """
    Extrae contenido relevante de un archivo HTML del portal Get on Board.
    
    Args:
        document_path (Path): Ruta del archivo HTML.

    Returns:
        dict: Registro limpio para guardar en JSON.
    """
    soup = get_html_document(document_path)
    link = get_link(soup)

    return {
        "link": link,
        "title": get_job_title(soup),
        "business_name": get_business_name(soup),
        "description": get_description(soup),
        "location": get_job_location(soup),
        "seniority": get_job_seniority(soup),
        "applications_count": get_applications_count(soup),
        "all_tags": get_all_tags(soup),
        "work_schedule": get_work_schedule(soup),
        "publication_time": get_publication_time(soup),
    }


# ===========================
#   EXECUTOR PARALELO
# ===========================

def get_on_board_transformer(save_func) -> List[Dict]:
    """
    Procesa en paralelo todos los HTML de get_on_board en /data/raw.

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

    prefix = "get_on_board"
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
