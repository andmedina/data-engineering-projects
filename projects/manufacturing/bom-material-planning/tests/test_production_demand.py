"""Tests for production-demand business rules."""

from datetime import date

import pytest

from src.etl.generate_production_demand import (
    DEMAND_QUANTITY_RULES,
    generate_production_demand,
)


PRODUCTS = [
    {"product_id": 1, "product_code": "BB-TI-250", "product_family": "Blind Bolt"},
    {"product_id": 2, "product_code": "SR-AD-316", "product_family": "Solid Rivet"},
]


def test_generation_is_reproducible_and_unique():
    planning_date = date(2026, 8, 13)
    first = generate_production_demand(PRODUCTS, 4, planning_date, random_seed=7)
    second = generate_production_demand(PRODUCTS, 4, planning_date, random_seed=7)

    assert first == second
    assert len(first) == 8
    assert len({row["demand_reference"] for row in first}) == 8


def test_quantities_follow_product_family_increments():
    rows = generate_production_demand(
        PRODUCTS, 5, date(2026, 8, 13), random_seed=10
    )

    families_by_id = {
        product["product_id"]: product["product_family"] for product in PRODUCTS
    }
    for row in rows:
        minimum, maximum, increment = DEMAND_QUANTITY_RULES[
            families_by_id[row["product_id"]]
        ]
        assert minimum <= row["demand_quantity"] <= maximum
        assert (row["demand_quantity"] - minimum) % increment == 0


def test_open_demand_is_future_dated_and_valid():
    planning_date = date(2026, 8, 13)
    rows = generate_production_demand(PRODUCTS, 6, planning_date, random_seed=20)

    assert all(row["required_date"] > planning_date for row in rows)
    assert all(row["demand_status"] in {"Planned", "Released"} for row in rows)
    assert all(row["priority"] in {"Standard", "High", "Critical"} for row in rows)
    assert rows == sorted(
        rows, key=lambda row: (row["required_date"], row["demand_reference"])
    )


def test_generation_requires_products():
    with pytest.raises(ValueError, match="At least one active product"):
        generate_production_demand([], planning_date=date(2026, 8, 13))
