import math
import random
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP

from .generate_production_order_materials import calculate_required_material


RECEIPT_QUANTITY_RANGES = {
    "Wire": (1000, 6000),
    "Rod": (1000, 5000),
    "Bar": (1500, 7000),
    "Coil": (2000, 9000),
    "Sheet": (1000, 5000),
    "Component": (500, 3000),
}


def choose_supplier(suppliers):
    """Choose a supplier with preference for higher quality ratings."""

    return random.choices(
        suppliers,
        weights=[float(supplier["quality_rating"]) for supplier in suppliers],
        k=1,
    )[0]


def generate_lot_status(quality_rating):
    """Generate a lot status influenced by supplier quality."""

    rejection_probability = max(
        0.01,
        (100 - float(quality_rating)) / 100,
    )
    on_hold_probability = 0.06
    depleted_probability = 0.22
    available_probability = (
        1
        - rejection_probability
        - on_hold_probability
        - depleted_probability
    )

    return random.choices(
        ["Available", "On Hold", "Depleted", "Rejected"],
        weights=[
            available_probability,
            on_hold_probability,
            depleted_probability,
            rejection_probability,
        ],
        k=1,
    )[0]


def generate_received_quantity(material_form):
    """Generate a receipt quantity appropriate for the material form."""

    minimum, maximum = RECEIPT_QUANTITY_RANGES[material_form]
    quantity = Decimal(str(random.uniform(minimum, maximum)))
    return quantity.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


def generate_available_quantity(lot_status, quantity_received):
    """Generate current usable inventory consistent with the lot status."""

    if lot_status in {"Depleted", "Rejected"}:
        return Decimal("0.000")

    if lot_status == "On Hold":
        remaining_ratio = random.uniform(0.40, 1.00)
    else:
        remaining_ratio = random.uniform(0.15, 0.90)

    quantity_available = quantity_received * Decimal(str(remaining_ratio))
    return quantity_available.quantize(
        Decimal("0.001"),
        rounding=ROUND_HALF_UP,
    )


def generate_material_lots(
    materials,
    suppliers,
    production_orders,
    start_date,
    end_date,
    background_lots_per_material=3,
):
    """Generate demand-aware receipts plus background inventory history."""

    if not materials:
        raise ValueError("At least one material is required to generate lots.")

    if not suppliers:
        raise ValueError(
            "At least one raw-material supplier is required to generate lots."
        )

    if end_date < start_date:
        raise ValueError("Material-lot end date cannot precede start date.")

    material_lots = []
    date_range_days = (end_date - start_date).days
    demand_by_material_month = defaultdict(list)

    for production_order in production_orders:
        if production_order["production_status"] not in {
            "Scheduled",
            "In Production",
            "Completed",
        }:
            continue

        scheduled_start_date = production_order["scheduled_start_date"]
        month_key = (
            production_order["material_id"],
            scheduled_start_date.year,
            scheduled_start_date.month,
        )
        demand_by_material_month[month_key].append(production_order)

    for material in materials:
        material_id = material["material_id"]
        monthly_keys = sorted(
            key for key in demand_by_material_month if key[0] == material_id
        )

        for month_key in monthly_keys:
            monthly_orders = demand_by_material_month[month_key]
            monthly_demand = sum(
                (
                    calculate_required_material(production_order)
                    for production_order in monthly_orders
                ),
                start=Decimal("0.000"),
            )
            supply_target = monthly_demand * Decimal("1.25")
            minimum_quantity, maximum_quantity = RECEIPT_QUANTITY_RANGES[
                material["material_form"]
            ]
            lot_count = max(
                1,
                math.ceil(supply_target / Decimal(str(maximum_quantity))),
            )
            quantity_per_lot = max(
                Decimal(str(minimum_quantity)),
                supply_target / lot_count,
            ).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
            earliest_production_date = min(
                order["scheduled_start_date"] for order in monthly_orders
            )

            for _ in range(lot_count):
                supplier = choose_supplier(suppliers)
                received_date = max(
                    start_date,
                    earliest_production_date
                    - timedelta(days=random.randint(7, 21)),
                )
                received_date = min(received_date, end_date)
                lot_sequence = len(material_lots) + 1

                material_lots.append(
                    {
                        "material_id": material_id,
                        "supplier_id": supplier["supplier_id"],
                        "supplier_lot_number": (
                            f"ML-{received_date.year}-{lot_sequence:05d}"
                        ),
                        "received_date": received_date,
                        "quantity_received": quantity_per_lot,
                        "quantity_available": quantity_per_lot,
                        "lot_status": "Available",
                    }
                )

        for _ in range(background_lots_per_material):
            supplier = choose_supplier(suppliers)
            received_date = start_date + timedelta(
                days=random.randint(0, date_range_days)
            )
            quantity_received = generate_received_quantity(
                material["material_form"]
            )
            lot_status = generate_lot_status(supplier["quality_rating"])
            lot_sequence = len(material_lots) + 1

            material_lots.append(
                {
                    "material_id": material_id,
                    "supplier_id": supplier["supplier_id"],
                    "supplier_lot_number": (
                        f"ML-{received_date.year}-{lot_sequence:05d}"
                    ),
                    "received_date": received_date,
                    "quantity_received": quantity_received,
                    "quantity_available": generate_available_quantity(
                        lot_status,
                        quantity_received,
                    ),
                    "lot_status": lot_status,
                }
            )

    return material_lots
