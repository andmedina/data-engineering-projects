"""Generate material inventory balances for the planning snapshot."""

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import random


THREE_DECIMALS = Decimal("0.001")

LOCATION_BY_CATEGORY = {
    "Metal Wire": "RAW-WH",
    "Process Chemical": "CHEM-WH",
    "Packaging": "PKG-WH",
}


def round_quantity(value):
    """Round a planning quantity to the database's three-decimal precision."""
    return Decimal(str(value)).quantize(THREE_DECIMALS, rounding=ROUND_HALF_UP)


def validate_inventory_record(record, valid_material_ids):
    """Raise ValueError when an inventory row violates a business rule."""
    if record["material_id"] not in valid_material_ids:
        raise ValueError("Inventory references an unknown material")
    if not record["location_code"].strip():
        raise ValueError("Inventory location cannot be blank")

    quantity_fields = (
        "on_hand_quantity",
        "reserved_quantity",
        "restricted_quantity",
        "safety_stock_quantity",
    )
    if any(record[field] < 0 for field in quantity_fields):
        raise ValueError("Inventory quantities cannot be negative")
    if (
        record["reserved_quantity"] + record["restricted_quantity"]
        > record["on_hand_quantity"]
    ):
        raise ValueError("Reserved and restricted inventory exceed on hand")


def generate_inventory_balances(
    material_requirements,
    snapshot_time=None,
    random_seed=42,
):
    """Generate one location balance per active material.

    On-hand and safety-stock quantities are tied to the current planning
    horizon so the later netting process produces meaningful shortages.
    """
    if not material_requirements:
        raise ValueError("Material requirement totals are required")

    rng = random.Random(random_seed)
    snapshot_time = snapshot_time or datetime.now(timezone.utc)
    valid_material_ids = {
        material["material_id"] for material in material_requirements
    }
    balances = []

    for material in sorted(
        material_requirements, key=lambda row: row["material_code"]
    ):
        category = material["material_category"]
        if category not in LOCATION_BY_CATEGORY:
            raise ValueError(f"No inventory location rule for category: {category}")

        total_requirement = Decimal(str(material["gross_requirement"] or 0))

        # On hand intentionally covers only part of the 120-day horizon. This
        # creates realistic purchasing needs without making every material short.
        coverage_ratio = Decimal(str(rng.uniform(0.28, 0.62)))
        on_hand = round_quantity(max(total_requirement * coverage_ratio, Decimal("1")))
        reserved = round_quantity(on_hand * Decimal(str(rng.uniform(0.04, 0.14))))
        restricted = round_quantity(on_hand * Decimal(str(rng.uniform(0.00, 0.04))))
        safety_stock = round_quantity(
            max(total_requirement * Decimal(str(rng.uniform(0.06, 0.12))), Decimal("0.5"))
        )

        record = {
            "material_id": material["material_id"],
            "location_code": LOCATION_BY_CATEGORY[category],
            "on_hand_quantity": on_hand,
            "reserved_quantity": reserved,
            "restricted_quantity": restricted,
            "safety_stock_quantity": safety_stock,
            "last_counted_at": snapshot_time,
        }
        validate_inventory_record(record, valid_material_ids)
        balances.append(record)

    return balances
