"""Tests for constrained purchase recommendations."""

from datetime import date
from decimal import Decimal

import pytest

from src.planning.recommendations import create_purchase_recommendations


SOURCE = {
    "material_id": 1,
    "supplier_id": 10,
    "supplier_code": "SUP-TEST",
    "supplier_name": "Test Supplier",
    "unit_price": Decimal("2.5000"),
    "lead_time_days": 10,
    "minimum_order_quantity": Decimal("500.000"),
    "order_multiple": Decimal("100.000"),
}


def net_row(need_date, net_quantity):
    """Build a compact net-requirement fixture."""
    return {
        "need_date": need_date,
        "material_id": 1,
        "material_code": "MAT-TEST",
        "material_name": "Test Material",
        "base_unit_of_measure": "KG",
        "net_requirement": Decimal(net_quantity),
    }


def test_recommendation_applies_moq_and_estimates_cost():
    rows = create_purchase_recommendations(
        [net_row(date(2026, 9, 1), "120.000")],
        [SOURCE],
        planning_date=date(2026, 8, 1),
    )

    assert len(rows) == 1
    assert rows[0]["recommended_order_quantity"] == Decimal("500.000")
    assert rows[0]["estimated_purchase_cost"] == Decimal("1250.00")
    assert rows[0]["excess_supply_carried_forward"] == Decimal("380.000")


def test_moq_excess_offsets_later_shortage():
    rows = create_purchase_recommendations(
        [
            net_row(date(2026, 9, 1), "120.000"),
            net_row(date(2026, 9, 15), "200.000"),
            net_row(date(2026, 10, 1), "300.000"),
        ],
        [SOURCE],
        planning_date=date(2026, 8, 1),
    )

    assert len(rows) == 2
    assert rows[0]["recommended_order_quantity"] == Decimal("500.000")
    assert rows[1]["prior_order_excess_applied"] == Decimal("180.000")
    assert rows[1]["remaining_net_requirement"] == Decimal("120.000")
    assert rows[1]["recommended_order_quantity"] == Decimal("500.000")


def test_order_date_and_urgency_use_supplier_lead_time():
    rows = create_purchase_recommendations(
        [net_row(date(2026, 8, 20), "600.000")],
        [SOURCE],
        planning_date=date(2026, 8, 13),
    )

    assert rows[0]["recommended_order_date"] == date(2026, 8, 10)
    assert rows[0]["urgency_status"] == "Past Due"


def test_missing_source_fails_for_short_material():
    with pytest.raises(ValueError, match="No preferred approved source"):
        create_purchase_recommendations(
            [net_row(date(2026, 9, 1), "100.000")],
            [],
            planning_date=date(2026, 8, 1),
        )
