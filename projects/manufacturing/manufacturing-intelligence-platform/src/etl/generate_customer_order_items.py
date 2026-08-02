import random
from decimal import Decimal, ROUND_HALF_UP


QUANTITY_RULES = {
    "Solid Rivet": (1000, 20000, 100),
    "Blind Rivet": (500, 10000, 100),
    "Blind Bolt": (100, 3000, 50),
    "Temporary Fastener": (100, 2000, 50),
    "Threaded Insert": (250, 5000, 50),
    "Installation Tool": (1, 20, 1),
    "Other": (100, 1000, 50),
}

PRICE_MARKUP_RANGES = {
    "Solid Rivet": (1.65, 2.10),
    "Blind Rivet": (1.60, 2.05),
    "Blind Bolt": (1.50, 1.90),
    "Temporary Fastener": (1.45, 1.80),
    "Threaded Insert": (1.55, 1.95),
    "Installation Tool": (1.30, 1.55),
    "Other": (1.50, 1.90),
}


def generate_ordered_quantity(product_family):
    """Generate a realistic order quantity for a product family."""

    minimum, maximum, increment = QUANTITY_RULES[product_family]
    return random.randrange(minimum, maximum + increment, increment)


def generate_unit_price(product, ordered_quantity):
    """Calculate a selling price from product cost, margin, and volume."""

    product_family = product["product_family"]
    minimum_markup, maximum_markup = PRICE_MARKUP_RANGES[product_family]
    markup = Decimal(str(random.uniform(minimum_markup, maximum_markup)))

    _, maximum_quantity, _ = QUANTITY_RULES[product_family]
    quantity_ratio = ordered_quantity / maximum_quantity

    if quantity_ratio >= 0.75:
        discount = Decimal("0.08")
    elif quantity_ratio >= 0.40:
        discount = Decimal("0.04")
    else:
        discount = Decimal("0.00")

    standard_unit_cost = Decimal(str(product["standard_unit_cost"]))
    unit_price = standard_unit_cost * markup * (Decimal("1.00") - discount)

    return unit_price.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def get_line_status(order_status):
    """Return a line status that is consistent with its order header."""

    status_mapping = {
        "Open": "Open",
        "Released": "Allocated",
        "Partially Fulfilled": "Partially Fulfilled",
        "Completed": "Completed",
        "Cancelled": "Cancelled",
    }
    return status_mapping[order_status]


def generate_customer_order_items(customer_orders, products):
    """Generate one to five unique product lines for each customer order."""

    if not products:
        raise ValueError("At least one product is required to generate order items.")

    order_items = []
    maximum_lines = min(5, len(products))

    for customer_order in customer_orders:
        line_count = random.randint(1, maximum_lines)
        selected_products = random.sample(products, k=line_count)

        for line_number, product in enumerate(selected_products, start=1):
            ordered_quantity = generate_ordered_quantity(
                product["product_family"]
            )

            order_items.append(
                {
                    "customer_order_id": customer_order["customer_order_id"],
                    "line_number": line_number,
                    "product_id": product["product_id"],
                    "ordered_quantity": ordered_quantity,
                    "unit_price": generate_unit_price(
                        product,
                        ordered_quantity,
                    ),
                    "line_status": get_line_status(
                        customer_order["order_status"]
                    ),
                }
            )

    return order_items
