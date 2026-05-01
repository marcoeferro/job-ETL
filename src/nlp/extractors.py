import re
from unidecode import unidecode

# Lista de tecnologías clave
TECH_KEYWORDS = {
    'python', 'java', 'javascript', 'typescript', 'c++', 'c#', 'go', 'rust', 
    'sql', 'postgresql', 'mysql', 'mongodb', 'redis', 'elasticsearch',
    'django', 'flask', 'fastapi', 'spring', 'react', 'angular', 'vue',
    'docker', 'kubernetes', 'aws', 'azure', 'gcp', 'terraform',
    'airflow', 'spark', 'hadoop', 'kafka', 'pandas', 'numpy', 'tensorflow'
}

def normalize_technologies(text: str) -> list:
    if not isinstance(text, str):
        return []
    text_lower = unidecode(text.lower())
    found = set()
    for tech in TECH_KEYWORDS:
        if re.search(r'\b' + re.escape(tech) + r'\b', text_lower):
            found.add(tech.capitalize())
    return list(found)

def normalize_location(loc: str) -> str:
    if not isinstance(loc, str):
        return ""
    loc = unidecode(loc.lower())
    # Mapeos básicos
    if 'buenos aires' in loc or 'caba' in loc:
        return 'Buenos Aires'
    if 'cordoba' in loc:
        return 'Córdoba'
    if 'rosario' in loc:
        return 'Rosario'
    if 'mendoza' in loc:
        return 'Mendoza'
    return loc.title()

def normalize_title(title: str) -> str:
    if not isinstance(title, str):
        return ""
    title = unidecode(title.lower())
    # Eliminar texto redundante
    title = re.sub(r'\b(remoto|hibrido|presencial|full time|part time|urgente)\b', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title.title()