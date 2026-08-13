"""Tests for chronological material supply netting."""

from datetime import date
from decimal import Decimal

import pytest

from src.planning.netting import net_material_requirements


def requirement(need_date, quantity):
    """Build a compact material-requirement fixture."""
    return {
        "need_date": need_date,
        "material_id": 1,
        "material_code": "MAT-TEST",
        "material_name": "Test Material",
        "base_unit_of_measure": "KG",
        "gross_requirement": Decimal(quantity),
    }


def test_inventory_is_consumed_only_once_across_requirements():
    requirements = [
        requirement(date(2026, 9, 1), "80.000"),
        requirement(date(2026, 9, 15), "50.000"),
    ]
    inventory = [{"material_id": 1, "usable_inventory": Decimal("100.000")}]

    rows = net_material_requirements(requirements, inventory, [])

    assert rows[0]["supply_applied"] == Decimal("80.000")
    assert rows[0]["net_requirement"] == Decimal("0.000")
    assert rows[0]["projected_supply_after_requirement"] == Decimal("20.000")
    assert rows[1]["supply_applied"] == Decimal("20.000")
    assert rows[1]["net_requirement"] == Decimal("30.000")
    assert rows[1]["projected_supply_after_requirement"] == Decimal("0.000")


def test_late_receipt_cannot_cover_earlier_requirement():
    requirements = [
        requirement(date(2026, 9, 1), "50.000"),
        requirement(date(2026, 9, 20), "50.000"),
    ]
    receipts = [
        {
            "material_id": 1,
            "expected_receipt_date": date(2026, 9, 10),
            "open_receipt_quantity": Decimal("60.000"),
        }
    ]

    rows = net_material_requirements(requirements, [], receipts)

    assert rows[0]["receipts_available_by_date"] == Decimal("0.000")
    assert rows[0]["net_requirement"] == Decimal("50.000")
    assert rows[1]["receipts_available_by_date"] == Decimal("60.000")
    assert rows[1]["net_requirement"] == Decimal("0.000")
    assert rows[1]["projected_supply_after_requirement"] == Decimal("10.000")


def test_unused_receipt_supply_carries_forward():
    requirements = [
        requirement(date(2026, 9, 10), "25.000"),
        requirement(date(2026, 9, 20), "50.000"),
    ]
    receipts = [
        {
            "material_id": 1,
            "expected_receipt_date": date(2026, 9, 5),
            "open_receipt_quantity": Decimal("100.000"),
        }
    ]

    rows = net_material_requirements(requirements, [], receipts)

    assert rows[0]["projected_supply_after_requirement"] == Decimal("75.000")
    assert rows[1]["receipts_available_by_date"] == Decimal("0.000")
    assert rows[1]["projected_supply_after_requirement"] == Decimal("25.000")


def test_materials_are_netted_independently():
    requirements = [
        requirement(date(2026, 9, 1), "40.000"),
        dict(
            requirement(date(2026, 9, 1), "25.000"),
            material_id=2,
            material_code="MAT-SECOND",
        ),
    ]
    inventory = [
        {"material_id": 1, "usable_inventory": Decimal("30.000")},
        {"material_id": 2, "usable_inventory": Decimal("25.000")},
    ]

    rows = net_material_requirements(requirements, inventory, [])

    results_by_material = {row["material_id"]: row for row in rows}
    assert results_by_material[1]["net_requirement"] == Decimal("10.000")
    assert results_by_material[2]["net_requirement"] == Decimal("0.000")


def test_netting_rejects_negative_supply():
    with pytest.raises(ValueError, match="Usable inventory cannot be negative"):
        net_material_requirements(
            [requirement(date(2026, 9, 1), "10.000")],
            [{"material_id": 1, "usable_inventory": Decimal("-1.000")}],
            [],
        )
