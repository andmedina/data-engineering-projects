"""Time-phase material supply against gross BOM requirements."""

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import text

from .bom_explosion import get_material_requirements


THREE_DECIMALS = Decimal("0.001")


INVENTORY_SUPPLY_QUERY = text(
    """
    SELECT
        material.material_id,
        material.material_code,
        ROUND(
            COALESCE(
                SUM(
                    GREATEST(
                        inventory.on_hand_quantity
                        - inventory.reserved_quantity
                        - inventory.restricted_quantity
                        - inventory.safety_stock_quantity,
                        0
                    )
                ),
                0
            ),
            3
        ) AS usable_inventory
    FROM materials AS material
    LEFT JOIN inventory_balances AS inventory
        ON inventory.material_id = material.material_id
    WHERE material.active_flag = TRUE
    GROUP BY material.material_id, material.material_code
    ORDER BY material.material_code
    """
)


SCHEDULED_RECEIPTS_QUERY = text(
    """
    SELECT
        purchase_order.material_id,
        material.material_code,
        purchase_order.expected_receipt_date,
        ROUND(
            SUM(
                purchase_order.ordered_quantity
                - purchase_order.received_quantity
            ),
            3
        ) AS open_receipt_quantity
    FROM purchase_orders AS purchase_order
    JOIN materials AS material
        ON material.material_id = purchase_order.material_id
    WHERE purchase_order.purchase_order_status IN ('Open', 'Partially Received')
    GROUP BY
        purchase_order.material_id,
        material.material_code,
        purchase_order.expected_receipt_date
    HAVING SUM(
        purchase_order.ordered_quantity - purchase_order.received_quantity
    ) > 0
    ORDER BY
        purchase_order.material_id,
        purchase_order.expected_receipt_date
    """
)


def round_quantity(value):
    """Round planning quantities to three decimal places."""
    return Decimal(str(value)).quantize(THREE_DECIMALS, rounding=ROUND_HALF_UP)


def validate_supply_inputs(requirements, inventory_supply, scheduled_receipts):
    """Validate nonnegative quantities and required identifiers."""
    for requirement in requirements:
        if requirement["gross_requirement"] < 0:
            raise ValueError("Gross requirements cannot be negative")
        if not requirement.get("need_date"):
            raise ValueError("Every requirement needs a date")

    for inventory in inventory_supply:
        if inventory["usable_inventory"] < 0:
            raise ValueError("Usable inventory cannot be negative")

    for receipt in scheduled_receipts:
        if receipt["open_receipt_quantity"] <= 0:
            raise ValueError("Scheduled receipt quantities must be positive")
        if not receipt.get("expected_receipt_date"):
            raise ValueError("Every scheduled receipt needs an expected date")


def net_material_requirements(requirements, inventory_supply, scheduled_receipts):
    """Chronologically consume inventory and timely receipts by material.

    Supply used for an earlier requirement is removed from the projected
    balance and cannot be reused. A purchase receipt is unavailable until its
    expected receipt date is on or before the current need date.
    """
    validate_supply_inputs(requirements, inventory_supply, scheduled_receipts)

    inventory_by_material = {
        row["material_id"]: round_quantity(row["usable_inventory"])
        for row in inventory_supply
    }
    receipts_by_material = defaultdict(list)
    for receipt in scheduled_receipts:
        receipts_by_material[receipt["material_id"]].append(receipt)
    for material_receipts in receipts_by_material.values():
        material_receipts.sort(key=lambda row: row["expected_receipt_date"])

    requirements_by_material = defaultdict(list)
    for requirement in requirements:
        requirements_by_material[requirement["material_id"]].append(requirement)

    results = []
    for material_id, material_requirements in requirements_by_material.items():
        material_requirements.sort(
            key=lambda row: (row["need_date"], row["material_code"])
        )
        material_receipts = receipts_by_material.get(material_id, [])
        receipt_index = 0
        projected_available = inventory_by_material.get(
            material_id, Decimal("0.000")
        )

        for requirement in material_requirements:
            receipts_available_by_date = Decimal("0.000")
            while (
                receipt_index < len(material_receipts)
                and material_receipts[receipt_index]["expected_receipt_date"]
                <= requirement["need_date"]
            ):
                receipt_quantity = round_quantity(
                    material_receipts[receipt_index]["open_receipt_quantity"]
                )
                projected_available += receipt_quantity
                receipts_available_by_date += receipt_quantity
                receipt_index += 1

            projected_before_requirement = round_quantity(projected_available)
            gross_requirement = round_quantity(requirement["gross_requirement"])
            supply_applied = min(projected_before_requirement, gross_requirement)
            net_requirement = gross_requirement - supply_applied
            projected_available = projected_before_requirement - supply_applied

            results.append(
                {
                    "need_date": requirement["need_date"],
                    "material_id": material_id,
                    "material_code": requirement["material_code"],
                    "material_name": requirement["material_name"],
                    "base_unit_of_measure": requirement[
                        "base_unit_of_measure"
                    ],
                    "gross_requirement": gross_requirement,
                    "receipts_available_by_date": round_quantity(
                        receipts_available_by_date
                    ),
                    "projected_supply_before_requirement": (
                        projected_before_requirement
                    ),
                    "supply_applied": round_quantity(supply_applied),
                    "net_requirement": round_quantity(net_requirement),
                    "projected_supply_after_requirement": round_quantity(
                        projected_available
                    ),
                }
            )

    return sorted(results, key=lambda row: (row["need_date"], row["material_code"]))


def get_inventory_supply(engine):
    """Return usable inventory after reservations, restrictions, and safety stock."""
    with engine.connect() as connection:
        return [
            dict(row) for row in connection.execute(INVENTORY_SUPPLY_QUERY).mappings()
        ]


def get_scheduled_receipts(engine):
    """Return remaining quantities on open purchase orders by expected date."""
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(SCHEDULED_RECEIPTS_QUERY).mappings()
        ]


def get_netted_material_requirements(engine):
    """Return the complete time-phased gross-to-net requirements plan."""
    return net_material_requirements(
        get_material_requirements(engine),
        get_inventory_supply(engine),
        get_scheduled_receipts(engine),
    )
