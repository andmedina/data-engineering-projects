"""Tests for purchase-order generation and supplier constraints."""

from datetime import date
from decimal import Decimal

import pytest

from src.etl.generate_purchase_orders import generate_purchase_orders
from src.purchasing import apply_order_constraints


MATERIAL_REQUIREMENTS = [
    {
        "material_id": 1,
        "material_code": "MAT-AL2117-WR",
        "gross_requirement": Decimal("1000.000"),
    },
    {
        "material_id": 2,
        "material_code": "PKG-TRAY-100",
        "gross_requirement": Decimal("5000.000"),
    },
]

PREFERRED_SOURCES = [
    {
        "material_id": 1,
        "supplier_id": 10,
        "unit_price": Decimal("8.6000"),
        "lead_time_days": 21,
        "minimum_order_quantity": Decimal("500.000"),
        "order_multiple": Decimal("100.000"),
    },
    {
        "material_id": 2,
        "supplier_id": 20,
        "unit_price": Decimal("1.1000"),
        "lead_time_days": 12,
        "minimum_order_quantity": Decimal("500.000"),
        "order_multiple": Decimal("100.000"),
    },
]


@pytest.mark.parametrize(
    ("required", "expected"),
    [
        (Decimal("0"), Decimal("0.000")),
        (Decimal("120"), Decimal("500.000")),
        (Decimal("550"), Decimal("600.000")),
        (Decimal("600"), Decimal("600.000")),
    ],
)
def test_order_constraints_apply_moq_and_multiple(required, expected):
    assert (
        apply_order_constraints(required, Decimal("500"), Decimal("100"))
        == expected
    )


def test_purchase_order_generation_is_reproducible():
    planning_date = date(2026, 8, 13)
    first = generate_purchase_orders(
        MATERIAL_REQUIREMENTS, PREFERRED_SOURCES, planning_date, random_seed=8
    )
    second = generate_purchase_orders(
        MATERIAL_REQUIREMENTS, PREFERRED_SOURCES, planning_date, random_seed=8
    )

    assert first == second
    assert len(first) == 5
    assert len({row["purchase_order_number"] for row in first}) == 5


def test_purchase_order_statuses_match_received_quantities():
    rows = generate_purchase_orders(
        MATERIAL_REQUIREMENTS,
        PREFERRED_SOURCES,
        date(2026, 8, 13),
        random_seed=12,
    )

    for row in rows:
        if row["purchase_order_status"] == "Received":
            assert row["received_quantity"] == row["ordered_quantity"]
        elif row["purchase_order_status"] == "Open":
            assert row["received_quantity"] == 0
        elif row["purchase_order_status"] == "Partially Received":
            assert 0 < row["received_quantity"] < row["ordered_quantity"]
        else:
            assert row["purchase_order_status"] == "Cancelled"
            assert row["received_quantity"] == 0


def test_generation_requires_preferred_source_for_each_material():
    with pytest.raises(ValueError, match="No preferred approved source"):
        generate_purchase_orders(
            MATERIAL_REQUIREMENTS,
            PREFERRED_SOURCES[:1],
            date(2026, 8, 13),
        )
