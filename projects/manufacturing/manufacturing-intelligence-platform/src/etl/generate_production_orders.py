import math
import random
from datetime import datetime, time, timedelta, timezone


PRIMARY_OPERATION_BY_FAMILY = {
    "Solid Rivet": "Cold Heading",
    "Blind Rivet": "Cold Heading",
    "Blind Bolt": "Cold Heading",
    "Temporary Fastener": "Cold Heading",
    "Threaded Insert": "Thread Rolling",
    "Installation Tool": "Assembly",
    "Other": "Multi-Purpose",
}

PRODUCTION_DAYS_BY_FAMILY = {
    "Solid Rivet": (2, 5),
    "Blind Rivet": (3, 6),
    "Blind Bolt": (4, 8),
    "Temporary Fastener": (2, 5),
    "Threaded Insert": (3, 7),
    "Installation Tool": (5, 10),
    "Other": (3, 7),
}


def get_production_status(line_status):
    """Return a production status consistent with an order-line status."""

    if line_status == "Allocated":
        return random.choices(
            ["Released", "Scheduled"],
            weights=[0.30, 0.70],
            k=1,
        )[0]

    status_mapping = {
        "Partially Fulfilled": "In Production",
        "Completed": "Completed",
        "Cancelled": "Cancelled",
    }
    return status_mapping[line_status]


def generate_planned_quantity(ordered_quantity, product_family):
    """Add a small production allowance for expected manufacturing scrap."""

    if product_family == "Installation Tool":
        extra_quantity = 1 if random.random() < 0.10 else 0
        return ordered_quantity + extra_quantity

    scrap_allowance = random.uniform(0.01, 0.04)
    return math.ceil(ordered_quantity * (1 + scrap_allowance))


def generate_schedule(order_date, requested_delivery_date, product_family):
    """Generate scheduled dates within the customer order's delivery window."""

    minimum_days, maximum_days = PRODUCTION_DAYS_BY_FAMILY[product_family]
    available_days = max((requested_delivery_date - order_date).days, 1)
    duration_days = min(
        random.randint(minimum_days, maximum_days),
        available_days,
    )
    latest_start_offset = max(1, available_days - duration_days)
    start_offset = random.randint(1, latest_start_offset)
    scheduled_start_date = order_date + timedelta(days=start_offset)
    scheduled_end_date = scheduled_start_date + timedelta(days=duration_days)

    return scheduled_start_date, min(
        scheduled_end_date,
        requested_delivery_date,
    )


def generate_actual_timestamps(
    production_status,
    scheduled_start_date,
    scheduled_end_date,
):
    """Generate actual timestamps appropriate for the production status."""

    if production_status not in {"In Production", "Completed"}:
        return None, None

    start_hour = random.randint(6, 14)
    actual_start = datetime.combine(
        scheduled_start_date,
        time(hour=start_hour),
        tzinfo=timezone.utc,
    )

    if production_status == "In Production":
        return actual_start, None

    end_hour = random.randint(14, 22)
    actual_end = datetime.combine(
        scheduled_end_date,
        time(hour=end_hour),
        tzinfo=timezone.utc,
    )
    return actual_start, actual_end


def generate_output_quantities(
    production_status,
    ordered_quantity,
    planned_quantity,
):
    """Generate completed and scrapped quantities for a work-order status."""

    if production_status in {"Released", "Scheduled", "Cancelled"}:
        return 0, 0

    if production_status == "Completed":
        return ordered_quantity, planned_quantity - ordered_quantity

    completed_quantity = math.floor(
        ordered_quantity * random.uniform(0.25, 0.75)
    )
    maximum_scrap = planned_quantity - completed_quantity
    scrapped_quantity = min(
        maximum_scrap,
        math.floor(completed_quantity * random.uniform(0.005, 0.025)),
    )
    return completed_quantity, scrapped_quantity


def generate_production_orders(customer_order_items, machines_by_operation):
    """Generate work orders for manufacturing-ready customer order lines."""

    production_orders = []

    for customer_order_item in customer_order_items:
        line_status = customer_order_item["line_status"]

        if line_status == "Open":
            continue

        if line_status == "Cancelled" and random.random() < 0.50:
            continue

        product_family = customer_order_item["product_family"]
        primary_operation = PRIMARY_OPERATION_BY_FAMILY[product_family]
        available_machine_ids = machines_by_operation.get(primary_operation, [])

        if not available_machine_ids:
            raise ValueError(
                f"No available machine found for operation: {primary_operation}"
            )

        production_status = get_production_status(line_status)
        ordered_quantity = customer_order_item["ordered_quantity"]
        planned_quantity = generate_planned_quantity(
            ordered_quantity,
            product_family,
        )

        if production_status == "Released":
            machine_id = None
            scheduled_start_date = None
            scheduled_end_date = None
        else:
            machine_id = random.choice(available_machine_ids)
            scheduled_start_date, scheduled_end_date = generate_schedule(
                customer_order_item["order_date"],
                customer_order_item["requested_delivery_date"],
                product_family,
            )

        actual_start, actual_end = generate_actual_timestamps(
            production_status,
            scheduled_start_date,
            scheduled_end_date,
        )
        completed_quantity, scrapped_quantity = generate_output_quantities(
            production_status,
            ordered_quantity,
            planned_quantity,
        )

        production_orders.append(
            {
                "production_order_number": (
                    f"PO-{100001 + len(production_orders)}"
                ),
                "customer_order_item_id": customer_order_item[
                    "customer_order_item_id"
                ],
                "machine_id": machine_id,
                "scheduled_start_date": scheduled_start_date,
                "scheduled_end_date": scheduled_end_date,
                "actual_start_timestamp": actual_start,
                "actual_end_timestamp": actual_end,
                "planned_quantity": planned_quantity,
                "completed_quantity": completed_quantity,
                "scrapped_quantity": scrapped_quantity,
                "production_status": production_status,
            }
        )

    return production_orders
