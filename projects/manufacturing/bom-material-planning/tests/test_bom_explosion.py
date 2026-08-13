"""Tests for BOM gross-requirement calculations."""

from decimal import Decimal

import pytest

from src.planning.bom_explosion import calculate_gross_requirement


def test_gross_requirement_without_loss():
    assert calculate_gross_requirement(10_000, "0.005", 0) == Decimal("50.000")


def test_gross_requirement_accounts_for_input_loss():
    # 100 kg of usable material with 2% loss needs 102.041 kg of input.
    assert calculate_gross_requirement(100, 1, 2) == Decimal("102.041")


def test_gross_requirement_rounds_to_three_decimals():
    assert calculate_gross_requirement(8_000, "0.0125", 4) == Decimal("104.167")


@pytest.mark.parametrize(
    ("demand", "quantity", "loss"),
    [
        (0, 1, 0),
        (1, 0, 0),
        (1, 1, -1),
        (1, 1, 100),
    ],
)
def test_gross_requirement_rejects_invalid_inputs(demand, quantity, loss):
    with pytest.raises(ValueError):
        calculate_gross_requirement(demand, quantity, loss)
