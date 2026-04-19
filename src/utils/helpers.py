# src/utils/helpers.py
import time
import random
import hashlib
import re
from pathlib import Path
from typing import Dict, Any, Optional

# ==================== USER AGENTS ====================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
]

def get_random_headers() -> Dict[str, str]:
    """Devuelve headers aleatorios para requests / Playwright"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    }


def random_delay(min_sec: float = 1.0, max_sec: float = 3.0):
    """Delay aleatorio para rate limiting"""
    time.sleep(random.uniform(min_sec, max_sec))


def generate_job_key(job: Dict[str, Any]) -> str:
    """Genera una business key fuerte para idempotencia"""
    key_str = f"{job.get('url','')}{job.get('title','')}{job.get('company','')}"
    return hashlib.sha256(key_str.encode('utf-8')).hexdigest()[:20]


def sanitize_filename(name: str) -> str:
    """Sanitiza un string para usarlo como nombre de archivo seguro"""
    return re.sub(r"[^a-zA-Z0-9_-]", "_", name)[:200]


def normalize_url(href: Optional[str], base_url: str) -> Optional[str]:
    """Convierte URL relativa a absoluta"""
    if not href:
        return None
    return href if href.startswith("http") else f"{base_url.rstrip('/')}/{href.lstrip('/')}"


def ensure_path(path: str | Path) -> Path:
    """Crea el directorio si no existe (útil para load.py y raw data)"""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path