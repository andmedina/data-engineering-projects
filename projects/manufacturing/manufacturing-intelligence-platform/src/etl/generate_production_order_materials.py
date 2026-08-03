import math
import random
from collections import defaultdict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP


MATERIAL_DENSITY_LB_PER_CUBIC_INCH = {
    "Aluminum": Decimal("0.098"),
    "Titanium": Decimal("0.160"),
    "Stainless Steel": Decimal("0.285"),
    "Alloy Steel": Decimal("0.283"),
    "Nickel Alloy": Decimal("0.297"),
}

GEOMETRY_FACTOR_BY_FAMILY = {
    "Solid Rivet": Decimal("1.35"),
    "Blind Rivet": Decimal("2.25"),
    "Blind Bolt": Decimal("2.00"),
    "Temporary Fastener": Decimal("2.50"),
    "Threaded Insert": Decimal("1.80"),
}


def calculate_required_material(production_order):
    """Estimate primary material required for a production order in pounds."""

    product_family = production_order["product_family"]
    planned_quantity = Decimal(str(production_order["planned_quantity"]))

    if product_family == "Installation Tool":
        unit_weight_lb = Decimal("3.000")
    else:
        diameter = Decimal(str(production_order["diameter_in"]))
        length = Decimal(str(production_order["length_in"]))
        radius = diameter / Decimal("2")
        volume = Decimal(str(math.pi)) * radius * radius * length
        density = MATERIAL_DENSITY_LB_PER_CUBIC_INCH[
            production_order["material_category"]
        ]
        geometry_factor = GEOMETRY_FACTOR_BY_FAMILY[product_family]
        unit_weight_lb = volume * density * geometry_factor

    process_loss_factor = Decimal(str(random.uniform(1.02, 1.05)))
    required_quantity = planned_quantity * unit_weight_lb * process_loss_factor

    return max(required_quantity, Decimal("0.001")).quantize(
        Decimal("0.001"),
        rounding=ROUND_HALF_UP,
    )


def generate_production_order_materials(production_orders, material_lots):
    """Allocate compatible material lots to production orders using FIFO."""

    lots_by_material = defaultdict(list)

    for material_lot in material_lots:
        if material_lot["lot_status"] == "Rejected":
            continue

        lot = material_lot.copy()
        lot["quantity_received"] = Decimal(str(lot["quantity_received"]))
        lot["quantity_available"] = Decimal(
            str(lot["quantity_available"])
        )
        lot["historical_capacity"] = (
            lot["quantity_received"] - lot["quantity_available"]
        )
        lots_by_material[lot["material_id"]].append(lot)

    for lots in lots_by_material.values():
        lots.sort(key=lambda lot: (lot["received_date"], lot["material_lot_id"]))

    allocations = []

    ordered_production_orders = sorted(
        production_orders,
        key=lambda production_order: (
            production_order["scheduled_start_date"] or date.max,
            production_order["production_order_id"],
        ),
    )

    for production_order in ordered_production_orders:
        if production_order["production_status"] not in {
            "Scheduled",
            "In Production",
            "Completed",
        }:
            continue

        material_id = production_order["material_id"]
        required_quantity = calculate_required_material(production_order)
        remaining_quantity = required_quantity

        if production_order["production_status"] == "Completed":
            eligible_lots = [
                lot
                for lot in lots_by_material.get(material_id, [])
                if lot["received_date"]
                <= production_order["scheduled_start_date"]
                and (
                    lot["historical_capacity"]
                    + (
                        lot["quantity_available"]
                        if lot["lot_status"] == "Available"
                        else Decimal("0")
                    )
                )
                > 0
            ]
        else:
            capacity_field = "quantity_available"
            eligible_lots = [
                lot
                for lot in lots_by_material.get(material_id, [])
                if lot["lot_status"] == "Available"
                and lot["received_date"]
                <= production_order["scheduled_start_date"]
                and lot[capacity_field] > 0
            ]

        for material_lot in eligible_lots:
            if production_order["production_status"] == "Completed":
                available_capacity = material_lot["historical_capacity"]

                if material_lot["lot_status"] == "Available":
                    available_capacity += material_lot["quantity_available"]
            else:
                available_capacity = material_lot[capacity_field]

            allocated_quantity = min(
                remaining_quantity,
                available_capacity,
            )

            if allocated_quantity <= 0:
                continue

            allocations.append(
                {
                    "production_order_id": production_order[
                        "production_order_id"
                    ],
                    "material_lot_id": material_lot["material_lot_id"],
                    "allocated_quantity": allocated_quantity,
                }
            )
            if production_order["production_status"] == "Completed":
                historical_usage = min(
                    allocated_quantity,
                    material_lot["historical_capacity"],
                )
                current_usage = allocated_quantity - historical_usage
                material_lot["historical_capacity"] -= historical_usage
                material_lot["quantity_available"] -= current_usage
            else:
                material_lot[capacity_field] -= allocated_quantity
            remaining_quantity -= allocated_quantity

            if remaining_quantity == 0:
                break

        if remaining_quantity > 0:
            raise ValueError(
                "Insufficient available material for production order "
                f"{production_order['production_order_number']}: "
                f"{remaining_quantity} lb short."
            )

    updated_lots = [
        {
            "material_lot_id": lot["material_lot_id"],
            "quantity_available": lot["quantity_available"].quantize(
                Decimal("0.001"),
                rounding=ROUND_HALF_UP,
            ),
            "lot_status": (
                "Depleted"
                if lot["lot_status"] == "Available"
                and lot["quantity_available"] == 0
                else lot["lot_status"]
            ),
        }
        for lots in lots_by_material.values()
        for lot in lots
    ]

    return allocations, updated_lots
