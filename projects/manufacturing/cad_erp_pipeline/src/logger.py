"""Simple logging utility for ETL pipeline."""

import logging
from pathlib import Path

from config import LOG_DIR

LOG_DIR.mkdir(exist_ok=True, parents=True)

LOG_FILE = LOG_DIR / "pipeline.log"


def get_logger(name: str = "cad_erp_pipeline") -> logging.Logger:
    """Create and configure logger for pipeline."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_FILE)
        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(message)s"
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
