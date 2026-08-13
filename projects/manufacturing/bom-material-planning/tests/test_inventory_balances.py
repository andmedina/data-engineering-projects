"""Tests for inventory-balance generation rules."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.etl.generate_inventory_balances import generate_inventory_balances


MATERIAL_REQUIREMENTS = [
    {
        "material_id": 1,
        "material_code": "MAT-AL2117-WR",
        "material_category": "Metal Wire",
        "gross_requirement": Decimal("1000.000"),
    },
    {
        "material_id": 2,
        "material_code": "CHEM-PASS",
        "material_category": "Process Chemical",
        "gross_requirement": Decimal("200.000"),
    },
    {
        "material_id": 3,
        "material_code": "PKG-TRAY-100",
        "material_category": "Packaging",
        "gross_requirement": Decimal("5000.000"),
    },
]


def test_inventory_generation_is_reproducible():
    snapshot = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    first = generate_inventory_balances(MATERIAL_REQUIREMENTS, snapshot, 9)
    second = generate_inventory_balances(MATERIAL_REQUIREMENTS, snapshot, 9)

    assert first == second
    assert len(first) == 3


def test_inventory_locations_follow_material_category():
    rows = generate_inventory_balances(
        MATERIAL_REQUIREMENTS,
        datetime(2026, 8, 13, tzinfo=timezone.utc),
    )

    assert [row["location_code"] for row in rows] == [
        "CHEM-WH",
        "RAW-WH",
        "PKG-WH",
    ]


def test_inventory_quantities_are_valid_and_have_three_decimals():
    rows = generate_inventory_balances(
        MATERIAL_REQUIREMENTS,
        datetime(2026, 8, 13, tzinfo=timezone.utc),
    )

    for row in rows:
        assert row["on_hand_quantity"] > 0
        assert row["safety_stock_quantity"] > 0
        assert row["reserved_quantity"] + row["restricted_quantity"] <= row[
            "on_hand_quantity"
        ]
        assert all(
            row[field].as_tuple().exponent == -3
            for field in (
                "on_hand_quantity",
                "reserved_quantity",
                "restricted_quantity",
                "safety_stock_quantity",
            )
        )


def test_inventory_generation_rejects_unknown_category():
    invalid_materials = [dict(MATERIAL_REQUIREMENTS[0], material_category="Unknown")]

    with pytest.raises(ValueError, match="No inventory location rule"):
        generate_inventory_balances(invalid_materials)
