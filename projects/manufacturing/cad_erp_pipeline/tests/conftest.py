"""Shared test fixtures for the CAD-to-ERP pipeline."""

# pylint: disable=redefined-outer-name

import pandas as pd
import pytest


@pytest.fixture
def seed_parts() -> pd.DataFrame:
    """Return a minimal valid part seed dataset."""
    return pd.DataFrame(
        [
            {
                "part_number": "KSD-AER-1001",
                "part_name": "Bracket",
                "revision": "A",
                "material": "Aluminum",
                "weight_kg": 1.5,
                "cad_system": "CATIA",
                "engineering_status": "released",
                "supplier_id": "SUP-001",
            },
            {
                "part_number": "KSD-AER-1002",
                "part_name": "Panel",
                "revision": "B",
                "material": "Composite",
                "weight_kg": 2.5,
                "cad_system": "NX",
                "engineering_status": "in_review",
                "supplier_id": "SUP-002",
            },
        ]
    )


@pytest.fixture
def valid_dataframes(seed_parts: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Return a complete set of small, valid raw-layer DataFrames."""
    return {
        "parts": seed_parts,
        "suppliers": pd.DataFrame(
            [
                {
                    "supplier_id": "SUP-001",
                    "supplier_name": "One",
                    "country": "USA",
                    "supplier_type": "metal",
                },
                {
                    "supplier_id": "SUP-002",
                    "supplier_name": "Two",
                    "country": "USA",
                    "supplier_type": "composite",
                },
            ]
        ),
        "inventory": pd.DataFrame(
            [
                {
                    "part_number": "KSD-AER-1001",
                    "stock_quantity": 2,
                    "reorder_level": 5,
                    "warehouse_location": "WH-A",
                    "last_updated": "2026-05-01",
                },
                {
                    "part_number": "KSD-AER-1002",
                    "stock_quantity": 8,
                    "reorder_level": 5,
                    "warehouse_location": "WH-B",
                    "last_updated": "2026-05-01",
                },
            ]
        ),
        "assemblies": pd.DataFrame(
            [{"assembly_id": "ASM-001", "assembly_name": "Assembly", "revision": "A"}]
        ),
        "bom": pd.DataFrame(
            [
                {
                    "assembly_id": "ASM-001",
                    "assembly_name": "Assembly",
                    "assembly_revision": "A",
                    "part_number": "KSD-AER-1001",
                    "quantity": 2,
                },
                {
                    "assembly_id": "ASM-001",
                    "assembly_name": "Assembly",
                    "assembly_revision": "A",
                    "part_number": "KSD-AER-1002",
                    "quantity": 1,
                },
            ]
        ),
    }
