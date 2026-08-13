"""Explode finished-product demand into dated raw-material requirements."""

from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import text


THREE_DECIMALS = Decimal("0.001")


MISSING_BOM_QUERY = text(
    """
    SELECT demand.demand_reference, product.product_code, demand.required_date
    FROM production_demand AS demand
    JOIN products AS product
        ON product.product_id = demand.product_id
    WHERE demand.demand_status IN ('Planned', 'Released')
      AND NOT EXISTS (
          SELECT 1
          FROM bills_of_materials AS bom
          WHERE bom.product_id = demand.product_id
            AND bom.bom_status = 'Active'
            AND demand.required_date >= bom.effective_start_date
            AND (
                bom.effective_end_date IS NULL
                OR demand.required_date <= bom.effective_end_date
            )
      )
    ORDER BY demand.required_date, demand.demand_reference
    """
)


BOM_EXPLOSION_QUERY = text(
    """
    SELECT
        demand.demand_id,
        demand.demand_reference,
        demand.required_date AS need_date,
        demand.priority,
        product.product_code,
        product.product_name,
        bom.revision_code AS bom_revision,
        component.line_number AS bom_line_number,
        material.material_id,
        material.material_code,
        material.material_name,
        material.base_unit_of_measure,
        demand.demand_quantity,
        component.quantity_per_unit,
        component.expected_loss_pct,
        ROUND(
            demand.demand_quantity
            * component.quantity_per_unit
            / (1 - component.expected_loss_pct / 100),
            3
        ) AS gross_requirement
    FROM production_demand AS demand
    JOIN products AS product
        ON product.product_id = demand.product_id
        AND product.active_flag = TRUE
    JOIN bills_of_materials AS bom
        ON bom.product_id = demand.product_id
        AND bom.bom_status = 'Active'
        AND demand.required_date >= bom.effective_start_date
        AND (
            bom.effective_end_date IS NULL
            OR demand.required_date <= bom.effective_end_date
        )
    JOIN bom_components AS component
        ON component.bom_id = bom.bom_id
    JOIN materials AS material
        ON material.material_id = component.material_id
        AND material.active_flag = TRUE
    WHERE demand.demand_status IN ('Planned', 'Released')
    ORDER BY
        demand.required_date,
        demand.demand_reference,
        component.line_number
    """
)


MATERIAL_REQUIREMENTS_QUERY = text(
    """
    WITH exploded_requirements AS (
        SELECT
            demand.required_date AS need_date,
            material.material_id,
            material.material_code,
            material.material_name,
            material.base_unit_of_measure,
            demand.demand_quantity
            * component.quantity_per_unit
            / (1 - component.expected_loss_pct / 100) AS gross_requirement
        FROM production_demand AS demand
        JOIN products AS product
            ON product.product_id = demand.product_id
            AND product.active_flag = TRUE
        JOIN bills_of_materials AS bom
            ON bom.product_id = demand.product_id
            AND bom.bom_status = 'Active'
            AND demand.required_date >= bom.effective_start_date
            AND (
                bom.effective_end_date IS NULL
                OR demand.required_date <= bom.effective_end_date
            )
        JOIN bom_components AS component
            ON component.bom_id = bom.bom_id
        JOIN materials AS material
            ON material.material_id = component.material_id
            AND material.active_flag = TRUE
        WHERE demand.demand_status IN ('Planned', 'Released')
    )
    SELECT
        need_date,
        material_id,
        material_code,
        material_name,
        base_unit_of_measure,
        ROUND(SUM(gross_requirement), 3) AS gross_requirement
    FROM exploded_requirements
    GROUP BY
        need_date,
        material_id,
        material_code,
        material_name,
        base_unit_of_measure
    ORDER BY need_date, material_code
    """
)


def calculate_gross_requirement(demand_quantity, quantity_per_unit, loss_pct):
    """Calculate required material input after expected process loss."""
    demand = Decimal(str(demand_quantity))
    quantity = Decimal(str(quantity_per_unit))
    loss = Decimal(str(loss_pct))

    if demand <= 0 or quantity <= 0:
        raise ValueError("Demand and component quantities must be positive")
    if loss < 0 or loss >= 100:
        raise ValueError("Expected loss must be at least zero and below 100")

    gross_requirement = demand * quantity / (Decimal("1") - loss / 100)
    return gross_requirement.quantize(THREE_DECIMALS, rounding=ROUND_HALF_UP)


def validate_bom_coverage(connection):
    """Fail planning when any open demand lacks an effective active BOM."""
    missing_boms = [
        dict(row) for row in connection.execute(MISSING_BOM_QUERY).mappings()
    ]
    if missing_boms:
        references = ", ".join(row["demand_reference"] for row in missing_boms)
        raise ValueError(f"Open demand is missing an effective BOM: {references}")


def get_bom_explosion(engine):
    """Return detailed demand-to-material requirement rows."""
    with engine.connect() as connection:
        validate_bom_coverage(connection)
        return [
            dict(row) for row in connection.execute(BOM_EXPLOSION_QUERY).mappings()
        ]


def get_material_requirements(engine):
    """Return gross requirements aggregated by material and need date."""
    with engine.connect() as connection:
        validate_bom_coverage(connection)
        return [
            dict(row)
            for row in connection.execute(MATERIAL_REQUIREMENTS_QUERY).mappings()
        ]
