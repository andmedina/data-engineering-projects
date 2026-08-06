"""Extract scaled engineering and manufacturing data from raw CSV files."""

import pandas as pd

from .config import (
    ASSEMBLIES_RAW_PATH,
    BOM_RAW_PATH,
    INVENTORY_RAW_PATH,
    PARTS_RAW_PATH,
    SUPPLIERS_RAW_PATH,
)


def extract_cad_parts() -> pd.DataFrame:
    """Extract scaled CAD part metadata from the raw layer."""
    return pd.read_csv(PARTS_RAW_PATH)


def extract_assembly_bom() -> pd.DataFrame:
    """Extract scaled assembly metadata from the raw layer."""
    return pd.read_csv(ASSEMBLIES_RAW_PATH)


def extract_bom() -> pd.DataFrame:
    """Extract scaled relational BOM rows from the raw layer."""
    return pd.read_csv(BOM_RAW_PATH)


def extract_suppliers() -> pd.DataFrame:
    """Extract supplier data from the raw layer."""
    return pd.read_csv(SUPPLIERS_RAW_PATH)


def extract_inventory() -> pd.DataFrame:
    """Extract inventory data from the raw layer."""
    return pd.read_csv(INVENTORY_RAW_PATH)


def extract_all_sources() -> dict[str, pd.DataFrame]:
    """Extract all source datasets into pandas DataFrames."""
    return {
        "parts": extract_cad_parts(),
        "assemblies": extract_assembly_bom(),
        "bom": extract_bom(),
        "suppliers": extract_suppliers(),
        "inventory": extract_inventory(),
    }
