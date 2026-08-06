"""Tests for raw extraction and transformations."""

# pylint: disable=missing-function-docstring

import pandas as pd

from src import extract
from src.transform import (
    flatten_assembly_bom,
    transform_inventory,
    transform_parts,
)


def test_extract_all_sources_reads_raw_csv_files(tmp_path, monkeypatch):
    paths = {}
    datasets = {
        "PARTS_RAW_PATH": pd.DataFrame([{"part_number": "P-1"}]),
        "ASSEMBLIES_RAW_PATH": pd.DataFrame([{"assembly_id": "A-1"}]),
        "BOM_RAW_PATH": pd.DataFrame([{"assembly_id": "A-1", "part_number": "P-1"}]),
        "SUPPLIERS_RAW_PATH": pd.DataFrame([{"supplier_id": "S-1"}]),
        "INVENTORY_RAW_PATH": pd.DataFrame(
            [{"part_number": "P-1", "stock_quantity": 1}]
        ),
    }
    for constant, dataframe in datasets.items():
        path = tmp_path / f"{constant.lower()}.csv"
        dataframe.to_csv(path, index=False)
        monkeypatch.setattr(extract, constant, path)
        paths[constant] = path

    extracted = extract.extract_all_sources()

    assert set(extracted) == {"parts", "assemblies", "bom", "suppliers", "inventory"}
    assert extracted["parts"].iloc[0]["part_number"] == "P-1"


def test_part_transformation_normalizes_values(seed_parts):
    parts = seed_parts.copy()
    parts.loc[0, ["part_number", "revision", "material", "engineering_status"]] = [
        "p-1", "b", " Aluminum ", "RELEASED"
    ]

    transformed = transform_parts(parts)

    assert transformed.loc[0, "part_number"] == "P-1"
    assert transformed.loc[0, "revision"] == "B"
    assert transformed.loc[0, "material"] == "Aluminum"
    assert transformed.loc[0, "engineering_status"] == "released"


def test_flatten_nested_bom():
    assemblies = pd.DataFrame(
        [
            {
                "assembly_id": "ASM-1",
                "assembly_name": "Wing",
                "revision": "B",
                "parts": [
                    {"part_number": "P-1", "quantity": 2},
                    {"part_number": "P-2", "quantity": 4},
                ],
            }
        ]
    )

    flattened = flatten_assembly_bom(assemblies)

    assert flattened.to_dict("records") == [
        {
            "assembly_id": "ASM-1", "assembly_name": "Wing",
            "assembly_revision": "B", "part_number": "P-1", "quantity": 2,
        },
        {
            "assembly_id": "ASM-1", "assembly_name": "Wing",
            "assembly_revision": "B", "part_number": "P-2", "quantity": 4,
        },
    ]


def test_inventory_reorder_flag_uses_strictly_below():
    inventory = pd.DataFrame(
        [
            {
                "part_number": "p-1", "stock_quantity": 4,
                "reorder_level": 5, "warehouse_location": "wh-a",
                "last_updated": "2026-01-01",
            },
            {
                "part_number": "p-2", "stock_quantity": 5,
                "reorder_level": 5, "warehouse_location": "wh-b",
                "last_updated": "2026-01-01",
            },
        ]
    )

    transformed = transform_inventory(inventory)

    assert transformed["below_reorder_level"].tolist() == [True, False]
