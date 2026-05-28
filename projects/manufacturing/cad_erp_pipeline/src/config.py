"""Configuration settings and file paths for the CAD-to-ERP pipeline."""

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR: Path = Path(__file__).resolve().parent.parent
load_dotenv()

SOURCE_DATA_DIR: Path = BASE_DIR / "data" / "source"
RAW_DATA_DIR: Path = BASE_DIR / "data" / "raw"
PROCESSED_DATA_DIR: Path = BASE_DIR / "data" / "processed"
LOG_DIR: Path = BASE_DIR / "logs"

PARTS_RAW_PATH: Path = RAW_DATA_DIR / "parts.csv"
ASSEMBLIES_RAW_PATH: Path = RAW_DATA_DIR / "assemblies.csv"
BOM_RAW_PATH: Path = RAW_DATA_DIR / "bom.csv"
SUPPLIERS_RAW_PATH: Path = RAW_DATA_DIR / "suppliers.csv"
INVENTORY_RAW_PATH: Path = RAW_DATA_DIR / "inventory.csv"

DATABASE_URL: str = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
