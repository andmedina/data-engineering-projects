"""Generate realistic finished-product demand for material planning."""

from datetime import date, timedelta
import random


DEMAND_QUANTITY_RULES = {
    "Solid Rivet": (40_000, 140_000, 5_000),
    "Blind Rivet": (25_000, 90_000, 5_000),
    "Blind Bolt": (8_000, 35_000, 1_000),
    "Threaded Insert": (12_000, 50_000, 1_000),
    "Temporary Fastener": (15_000, 60_000, 1_000),
}


def generate_demand_quantity(product_family, rng):
    """Return a family-appropriate quantity aligned to its planning increment."""
    minimum, maximum, increment = DEMAND_QUANTITY_RULES[product_family]
    step_count = (maximum - minimum) // increment
    return minimum + rng.randint(0, step_count) * increment


def choose_demand_status(days_until_required, rng):
    """Use Released more often for near-term demand and Planned farther out."""
    if days_until_required <= 21:
        return rng.choices(["Released", "Planned"], weights=[0.85, 0.15], k=1)[0]
    if days_until_required <= 60:
        return rng.choices(["Released", "Planned"], weights=[0.45, 0.55], k=1)[0]
    return rng.choices(["Released", "Planned"], weights=[0.10, 0.90], k=1)[0]


def choose_priority(days_until_required, rng):
    """Assign higher priority somewhat more often to near-term demand."""
    if days_until_required <= 14:
        return rng.choices(
            ["Standard", "High", "Critical"], weights=[0.45, 0.40, 0.15], k=1
        )[0]
    return rng.choices(
        ["Standard", "High", "Critical"], weights=[0.75, 0.22, 0.03], k=1
    )[0]


def validate_demand_record(record, valid_product_ids, planning_date):
    """Raise ValueError when a generated demand row violates a business rule."""
    if record["product_id"] not in valid_product_ids:
        raise ValueError("Demand references an unknown or inactive product")
    if record["demand_quantity"] <= 0:
        raise ValueError("Demand quantity must be positive")
    if record["required_date"] <= planning_date:
        raise ValueError("Open demand must have a future required date")
    if record["demand_status"] not in {"Planned", "Released"}:
        raise ValueError("Generated open demand must be Planned or Released")
    if record["priority"] not in {"Standard", "High", "Critical"}:
        raise ValueError("Demand priority is invalid")


def generate_production_demand(
    products,
    records_per_product=8,
    planning_date=None,
    random_seed=42,
):
    """Generate reproducible open demand over a 120-day planning horizon."""
    if not products:
        raise ValueError("At least one active product is required")
    if records_per_product <= 0:
        raise ValueError("records_per_product must be positive")

    planning_date = planning_date or date.today()
    rng = random.Random(random_seed)
    valid_product_ids = {product["product_id"] for product in products}
    demand_rows = []
    sequence = 1

    for product in sorted(products, key=lambda row: row["product_code"]):
        product_family = product["product_family"]
        if product_family not in DEMAND_QUANTITY_RULES:
            raise ValueError(f"No demand rule for product family: {product_family}")

        # Each product receives requirements spread across the planning horizon.
        required_offsets = sorted(
            rng.sample(range(7, 121), k=records_per_product)
        )
        for days_until_required in required_offsets:
            record = {
                "demand_reference": (
                    f"PD-{planning_date.strftime('%Y%m%d')}-{sequence:04d}"
                ),
                "product_id": product["product_id"],
                "required_date": planning_date + timedelta(days=days_until_required),
                "demand_quantity": generate_demand_quantity(product_family, rng),
                "demand_status": choose_demand_status(days_until_required, rng),
                "priority": choose_priority(days_until_required, rng),
            }
            validate_demand_record(record, valid_product_ids, planning_date)
            demand_rows.append(record)
            sequence += 1

    return sorted(
        demand_rows,
        key=lambda row: (row["required_date"], row["demand_reference"]),
    )
