from decimal import Decimal
from unittest.mock import patch

from src.etl.generate_customer_order_items import (
    generate_customer_order_items,
    generate_ordered_quantity,
    get_line_status,
)


def test_line_status_matches_parent_order_status():
    assert get_line_status("Open") == "Open"
    assert get_line_status("Released") == "Allocated"
    assert get_line_status("Partially Fulfilled") == "Partially Fulfilled"
    assert get_line_status("Completed") == "Completed"
    assert get_line_status("Cancelled") == "Cancelled"


def test_ordered_quantities_follow_product_family_rules():
    for product_family in (
        "Solid Rivet",
        "Blind Rivet",
        "Blind Bolt",
        "Temporary Fastener",
        "Threaded Insert",
        "Installation Tool",
    ):
        quantity = generate_ordered_quantity(product_family)
        assert quantity > 0


@patch("src.etl.generate_customer_order_items.random.randint", return_value=2)
def test_generated_order_has_unique_products_and_sequential_lines(_):
    customer_orders = [
        {"customer_order_id": 1, "order_status": "Released"}
    ]
    products = [
        {
            "product_id": 10,
            "product_family": "Solid Rivet",
            "standard_unit_cost": Decimal("0.1200"),
        },
        {
            "product_id": 11,
            "product_family": "Blind Bolt",
            "standard_unit_cost": Decimal("4.7500"),
        },
    ]

    order_items = generate_customer_order_items(customer_orders, products)

    assert len(order_items) == 2
    assert {item["product_id"] for item in order_items} == {10, 11}
    assert [item["line_number"] for item in order_items] == [1, 2]
    assert all(item["line_status"] == "Allocated" for item in order_items)
    assert all(item["unit_price"] > 0 for item in order_items)
