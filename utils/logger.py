import logging
from pathlib import Path

def get_logger(name: str = "scraper") -> logging.Logger:
    """
    Configura y devuelve un logger que escribe en la carpeta 'loggs'.

    Args:
        name (str, optional): Nombre del logger. Default = "scraper".

    Returns:
        logging.Logger: Instancia configurada del logger.
    """
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "scrapers.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
