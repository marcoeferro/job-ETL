# src/scrapers/utils.py
import time
import random
import hashlib
from pathlib import Path
from typing import Dict, Any
import pandas as pd

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
]

def get_random_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    }

def random_delay(min_sec: float = 3.5, max_sec: float = 8.0):
    time.sleep(random.uniform(min_sec, max_sec))

def generate_job_key(job: Dict[str, Any]) -> str:
    """Business key fuerte para idempotencia"""
    key_str = f"{job.get('url','')}{job.get('title','')}{job.get('company','')}"
    return hashlib.sha256(key_str.encode('utf-8')).hexdigest()[:20]

def ensure_path(path: str | Path):
    Path(path).mkdir(parents=True, exist_ok=True)