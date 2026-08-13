"""Shared purchasing quantity calculations."""

from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP


THREE_DECIMALS = Decimal("0.001")


def round_quantity(value):
    """Round a purchasing quantity to three decimal places."""
    return Decimal(str(value)).quantize(THREE_DECIMALS, rounding=ROUND_HALF_UP)


def apply_order_constraints(net_quantity, minimum_order_quantity, order_multiple):
    """Apply MOQ and round a positive requirement up to the order multiple."""
    quantity = Decimal(str(net_quantity))
    minimum = Decimal(str(minimum_order_quantity))
    multiple = Decimal(str(order_multiple))

    if quantity <= 0:
        return Decimal("0.000")
    if minimum < 0 or multiple <= 0:
        raise ValueError("Supplier ordering constraints are invalid")

    constrained = max(quantity, minimum)
    multiple_count = (constrained / multiple).to_integral_value(
        rounding=ROUND_CEILING
    )
    return round_quantity(multiple_count * multiple)
