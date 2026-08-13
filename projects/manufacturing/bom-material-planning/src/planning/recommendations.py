"""Create supplier-constrained purchase recommendations from material shortages."""

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import text

from src.purchasing import apply_order_constraints, round_quantity

from .netting import get_netted_material_requirements


TWO_DECIMALS = Decimal("0.01")


PREFERRED_SOURCE_QUERY = text(
    """
    SELECT
        source.material_id,
        material.material_code,
        supplier.supplier_id,
        supplier.supplier_code,
        supplier.supplier_name,
        source.unit_price,
        source.lead_time_days,
        source.minimum_order_quantity,
        source.order_multiple
    FROM supplier_materials AS source
    JOIN materials AS material
        ON material.material_id = source.material_id
        AND material.active_flag = TRUE
    JOIN suppliers AS supplier
        ON supplier.supplier_id = source.supplier_id
        AND supplier.supplier_status = 'Approved'
    WHERE source.preferred_flag = TRUE
      AND source.source_status = 'Approved'
    ORDER BY material.material_code
    """
)


def get_urgency_status(recommended_order_date, planning_date):
    """Classify whether a recommended order is future, due today, or past due."""
    if recommended_order_date < planning_date:
        return "Past Due"
    if recommended_order_date == planning_date:
        return "Due Today"
    return "Future"


def validate_source_coverage(netted_requirements, sources_by_material):
    """Require a preferred approved source for every material with shortage."""
    missing_materials = sorted(
        {
            row["material_code"]
            for row in netted_requirements
            if row["net_requirement"] > 0
            and row["material_id"] not in sources_by_material
        }
    )
    if missing_materials:
        raise ValueError(
            "No preferred approved source for: " + ", ".join(missing_materials)
        )


def create_purchase_recommendations(
    netted_requirements,
    preferred_sources,
    planning_date=None,
):
    """Convert dated shortages into constrained purchasing recommendations.

    An MOQ or order-multiple may make a recommendation larger than the current
    shortage. That excess supply is carried forward and offsets later shortage
    rows for the same material.
    """
    planning_date = planning_date or date.today()
    sources_by_material = {
        source["material_id"]: source for source in preferred_sources
    }
    validate_source_coverage(netted_requirements, sources_by_material)

    rows_by_material = defaultdict(list)
    for row in netted_requirements:
        rows_by_material[row["material_id"]].append(row)

    recommendations = []
    sequence = 1
    for material_id, material_rows in rows_by_material.items():
        material_rows.sort(key=lambda row: row["need_date"])
        source = sources_by_material.get(material_id)
        excess_planned_supply = Decimal("0.000")

        for row in material_rows:
            net_requirement = round_quantity(row["net_requirement"])
            excess_applied = min(excess_planned_supply, net_requirement)
            remaining_shortage = round_quantity(net_requirement - excess_applied)
            excess_planned_supply = round_quantity(
                excess_planned_supply - excess_applied
            )

            if remaining_shortage <= 0:
                continue

            recommended_quantity = apply_order_constraints(
                remaining_shortage,
                source["minimum_order_quantity"],
                source["order_multiple"],
            )
            order_excess = round_quantity(
                recommended_quantity - remaining_shortage
            )
            excess_planned_supply = round_quantity(
                excess_planned_supply + order_excess
            )
            recommended_order_date = row["need_date"] - timedelta(
                days=source["lead_time_days"]
            )
            estimated_cost = (
                recommended_quantity * Decimal(str(source["unit_price"]))
            ).quantize(TWO_DECIMALS, rounding=ROUND_HALF_UP)

            recommendations.append(
                {
                    "recommendation_id": f"REC-{sequence:04d}",
                    "material_id": material_id,
                    "material_code": row["material_code"],
                    "material_name": row["material_name"],
                    "base_unit_of_measure": row["base_unit_of_measure"],
                    "need_date": row["need_date"],
                    "original_net_requirement": net_requirement,
                    "prior_order_excess_applied": round_quantity(excess_applied),
                    "remaining_net_requirement": remaining_shortage,
                    "supplier_id": source["supplier_id"],
                    "supplier_code": source["supplier_code"],
                    "supplier_name": source["supplier_name"],
                    "lead_time_days": source["lead_time_days"],
                    "minimum_order_quantity": source[
                        "minimum_order_quantity"
                    ],
                    "order_multiple": source["order_multiple"],
                    "recommended_order_quantity": recommended_quantity,
                    "recommended_order_date": recommended_order_date,
                    "urgency_status": get_urgency_status(
                        recommended_order_date, planning_date
                    ),
                    "estimated_purchase_cost": estimated_cost,
                    "excess_supply_carried_forward": excess_planned_supply,
                }
            )
            sequence += 1

    return sorted(
        recommendations,
        key=lambda row: (
            row["recommended_order_date"],
            row["need_date"],
            row["material_code"],
        ),
    )


def get_preferred_sources(engine):
    """Return preferred approved supplier rules used for recommendations."""
    with engine.connect() as connection:
        return [
            dict(row) for row in connection.execute(PREFERRED_SOURCE_QUERY).mappings()
        ]


def get_purchase_recommendations(engine, planning_date=None):
    """Run gross-to-net planning and return actionable purchase recommendations."""
    return create_purchase_recommendations(
        get_netted_material_requirements(engine),
        get_preferred_sources(engine),
        planning_date,
    )
