import os
from bs4 import BeautifulSoup

def get_html_document(document_title: str) -> BeautifulSoup:
    """Abre un archivo HTML y devuelve el objeto BeautifulSoup."""
    with open(document_title, "r", encoding="utf-8") as file:
        html_doc = file.read()
    return BeautifulSoup(html_doc, "html.parser")

def dispose_garbage(soup: BeautifulSoup):
    """Devuelve el bloque principal de información."""
    return soup.find("main", class_="detail_fs")

def get_job_title(soup: BeautifulSoup) -> str | None:
    tag = soup.h1
    return tag.get_text(strip=True) if tag else None

def get_business_name(soup: BeautifulSoup) -> str | None:
    tag = soup.find("p", class_="fs16")
    if tag:
        parts = tag.get_text(strip=True).split(" - ")
        return parts[0] if parts else None
    return None

def get_location(soup: BeautifulSoup) -> str | None:
    tag = soup.find("p", class_="fs16")
    if tag:
        parts = tag.get_text(strip=True).split(" - ")
        return parts[1] if len(parts) > 1 else None
    return None

def get_description(soup: BeautifulSoup) -> str | None:
    tag = soup.find("p", class_="mbB")
    return tag.get_text(strip=True) if tag else None

def get_requirements(soup: BeautifulSoup) -> str | None:
    tag = soup.ul
    return tag.get_text(strip=True) if tag else None

def transform_data(document_title: str) -> dict:
    """Extrae la información relevante de un archivo HTML."""
    soup = get_html_document(document_title)
    soup = dispose_garbage(soup)
    return {
        "title": get_job_title(soup),
        "business_name": get_business_name(soup),
        "location": get_location(soup),
        "description": get_description(soup),
        "requirements": get_requirements(soup),
    }

def computrabajo_transformer(folder_path: str, prefix: str, save_funct) -> list[dict]:
    """
    Recorre una carpeta, abre los archivos HTML que empiezan con `prefix`,
    y devuelve una lista de diccionarios con la información extraída.
    """
    i = 0
    for filename in os.listdir(folder_path):
        if filename.endswith(".html") and filename.startswith(prefix):
            full_path = os.path.join(folder_path, filename)
            data = transform_data(full_path)
            save_funct(data, path=f"cleaned/computrabajo{i}.json")
            i += 1
    