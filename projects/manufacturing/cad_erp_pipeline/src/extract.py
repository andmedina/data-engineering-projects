"""Extract source engineering and manufacturing data files."""

import json
from pathlib import Path

import pandas as pd

from config import SOURCE_DATA_DIR


def load_json_file(file_path: Path) -> list[dict]:
    """Load a JSON file and return a list of records."""
    with file_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def extract_cad_parts() -> pd.DataFrame:
    """Extract CAD part metadata from JSON source export."""
    records: list[dict] = load_json_file(
        SOURCE_DATA_DIR / "cad_parts_export.json"
    )
    return pd.DataFrame(records)


def extract_assembly_bom() -> pd.DataFrame:
    """Extract nested assembly BOM data from JSON source export."""
    records: list[dict] = load_json_file(
        SOURCE_DATA_DIR / "assembly_bom_export.json"
    )
    return pd.DataFrame(records)


def extract_suppliers() -> pd.DataFrame:
    """Extract supplier data from CSV source export."""
    return pd.read_csv(SOURCE_DATA_DIR / "suppliers_export.csv")


def extract_inventory() -> pd.DataFrame:
    """Extract inventory data from CSV source export."""
    return pd.read_csv(SOURCE_DATA_DIR / "inventory_export.csv")


def extract_all_sources() -> dict[str, pd.DataFrame]:
    """Extract all source datasets into pandas DataFrames."""
    return {
        "parts": extract_cad_parts(),
        "assemblies": extract_assembly_bom(),
        "suppliers": extract_suppliers(),
        "inventory": extract_inventory(),
    }
