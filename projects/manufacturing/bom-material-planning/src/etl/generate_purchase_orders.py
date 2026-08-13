"""Generate supplier purchase-order lines for material planning."""

from datetime import date, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
import random


THREE_DECIMALS = Decimal("0.001")


def round_quantity(value):
    """Round quantities to the database's three-decimal precision."""
    return Decimal(str(value)).quantize(THREE_DECIMALS, rounding=ROUND_HALF_UP)


def apply_order_constraints(net_quantity, minimum_order_quantity, order_multiple):
    """Apply MOQ and round a required quantity up to the supplier multiple."""
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


def validate_purchase_order_record(record, valid_material_ids, valid_supplier_ids):
    """Raise ValueError when a purchase-order row violates a business rule."""
    if record["material_id"] not in valid_material_ids:
        raise ValueError("Purchase order references an unknown material")
    if record["supplier_id"] not in valid_supplier_ids:
        raise ValueError("Purchase order references an unknown supplier")
    if record["line_number"] <= 0 or record["ordered_quantity"] <= 0:
        raise ValueError("Purchase order line and quantity must be positive")
    if record["expected_receipt_date"] < record["order_date"]:
        raise ValueError("Expected receipt cannot precede the order date")
    if not Decimal("0") <= record["received_quantity"] <= record["ordered_quantity"]:
        raise ValueError("Received quantity is outside the ordered quantity")

    status = record["purchase_order_status"]
    received = record["received_quantity"]
    ordered = record["ordered_quantity"]
    if status == "Open" and received != 0:
        raise ValueError("Open purchase orders cannot have receipts")
    if status == "Partially Received" and not Decimal("0") < received < ordered:
        raise ValueError("Partially received quantity is inconsistent")
    if status == "Received" and received != ordered:
        raise ValueError("Received purchase orders must be complete")


def generate_purchase_orders(
    material_requirements,
    preferred_sources,
    planning_date=None,
    random_seed=42,
):
    """Generate historical and open supply for every planned material."""
    if not material_requirements or not preferred_sources:
        raise ValueError("Material requirements and preferred sources are required")

    planning_date = planning_date or date.today()
    rng = random.Random(random_seed)
    sources_by_material = {
        source["material_id"]: source for source in preferred_sources
    }
    valid_material_ids = {
        material["material_id"] for material in material_requirements
    }
    valid_supplier_ids = {
        source["supplier_id"] for source in preferred_sources
    }
    purchase_orders = []
    sequence = 1

    ordered_materials = sorted(
        material_requirements, key=lambda row: row["material_code"]
    )
    for material_position, material in enumerate(ordered_materials, start=1):
        material_id = material["material_id"]
        if material_id not in sources_by_material:
            raise ValueError(
                f"No preferred approved source for {material['material_code']}"
            )

        source = sources_by_material[material_id]
        gross_requirement = Decimal(str(material["gross_requirement"] or 0))
        target_quantity = max(gross_requirement, source["minimum_order_quantity"])

        # Historical received supply demonstrates status handling but does not
        # count as a future scheduled receipt in the planning engine.
        historical_quantity = apply_order_constraints(
            target_quantity * Decimal(str(rng.uniform(0.08, 0.16))),
            source["minimum_order_quantity"],
            source["order_multiple"],
        )
        historical_receipt = planning_date - timedelta(days=rng.randint(10, 35))
        historical_order = historical_receipt - timedelta(
            days=source["lead_time_days"]
        )
        historical = {
            "purchase_order_number": f"PO-{planning_date.year}-{sequence:04d}",
            "line_number": 1,
            "supplier_id": source["supplier_id"],
            "material_id": material_id,
            "order_date": historical_order,
            "expected_receipt_date": historical_receipt,
            "ordered_quantity": historical_quantity,
            "received_quantity": historical_quantity,
            "unit_price": source["unit_price"],
            "purchase_order_status": "Received",
        }
        validate_purchase_order_record(
            historical, valid_material_ids, valid_supplier_ids
        )
        purchase_orders.append(historical)
        sequence += 1

        # Current open supply. Expected dates vary so some receipts will arrive
        # before early demand and others will only support later requirements.
        open_quantity = apply_order_constraints(
            target_quantity * Decimal(str(rng.uniform(0.16, 0.32))),
            source["minimum_order_quantity"],
            source["order_multiple"],
        )
        receipt_offset = rng.randint(4, max(12, source["lead_time_days"] + 8))
        open_receipt = planning_date + timedelta(days=receipt_offset)
        open_order = min(
            planning_date,
            open_receipt - timedelta(days=source["lead_time_days"]),
        )
        current_status = rng.choice(["Open", "Partially Received"])
        if current_status == "Partially Received":
            received_ratio = Decimal(str(rng.uniform(0.20, 0.55)))
            received_quantity = round_quantity(open_quantity * received_ratio)
            if received_quantity >= open_quantity:
                received_quantity = open_quantity - THREE_DECIMALS
        else:
            received_quantity = Decimal("0.000")

        current = {
            "purchase_order_number": f"PO-{planning_date.year}-{sequence:04d}",
            "line_number": 1,
            "supplier_id": source["supplier_id"],
            "material_id": material_id,
            "order_date": open_order,
            "expected_receipt_date": open_receipt,
            "ordered_quantity": open_quantity,
            "received_quantity": received_quantity,
            "unit_price": source["unit_price"],
            "purchase_order_status": current_status,
        }
        validate_purchase_order_record(current, valid_material_ids, valid_supplier_ids)
        purchase_orders.append(current)
        sequence += 1

        # Add cancelled history for alternating materials. Cancelled quantities
        # remain visible for audit but never count as scheduled supply.
        if material_position % 2 == 0:
            cancelled_quantity = apply_order_constraints(
                target_quantity * Decimal("0.10"),
                source["minimum_order_quantity"],
                source["order_multiple"],
            )
            cancelled_order = planning_date - timedelta(days=rng.randint(5, 20))
            cancelled = {
                "purchase_order_number": (
                    f"PO-{planning_date.year}-{sequence:04d}"
                ),
                "line_number": 1,
                "supplier_id": source["supplier_id"],
                "material_id": material_id,
                "order_date": cancelled_order,
                "expected_receipt_date": cancelled_order
                + timedelta(days=source["lead_time_days"]),
                "ordered_quantity": cancelled_quantity,
                "received_quantity": Decimal("0.000"),
                "unit_price": source["unit_price"],
                "purchase_order_status": "Cancelled",
            }
            validate_purchase_order_record(
                cancelled, valid_material_ids, valid_supplier_ids
            )
            purchase_orders.append(cancelled)
            sequence += 1

    return sorted(
        purchase_orders,
        key=lambda row: (row["expected_receipt_date"], row["purchase_order_number"]),
    )
