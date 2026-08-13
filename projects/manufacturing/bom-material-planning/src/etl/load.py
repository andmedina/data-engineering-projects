"""Retrieve master data and load generated planning transactions."""

from sqlalchemy import text


ACTIVE_PRODUCTS_QUERY = text(
    """
    SELECT product_id, product_code, product_family
    FROM products
    WHERE active_flag = TRUE
    ORDER BY product_code
    """
)

MATERIAL_REQUIREMENT_TOTALS_QUERY = text(
    """
    SELECT
        material.material_id,
        material.material_code,
        material.material_category,
        material.base_unit_of_measure,
        COALESCE(
            SUM(
                demand.demand_quantity
                * component.quantity_per_unit
                / (1 - component.expected_loss_pct / 100)
            ),
            0
        ) AS gross_requirement
    FROM materials AS material
    LEFT JOIN bom_components AS component
        ON component.material_id = material.material_id
    LEFT JOIN bills_of_materials AS bom
        ON bom.bom_id = component.bom_id
        AND bom.bom_status = 'Active'
    LEFT JOIN production_demand AS demand
        ON demand.product_id = bom.product_id
        AND demand.demand_status IN ('Planned', 'Released')
        AND demand.required_date >= bom.effective_start_date
        AND (
            bom.effective_end_date IS NULL
            OR demand.required_date <= bom.effective_end_date
        )
    WHERE material.active_flag = TRUE
    GROUP BY
        material.material_id,
        material.material_code,
        material.material_category,
        material.base_unit_of_measure
    ORDER BY material.material_code
    """
)

PREFERRED_MATERIAL_SOURCES_QUERY = text(
    """
    SELECT
        source.material_id,
        source.supplier_id,
        source.unit_price,
        source.lead_time_days,
        source.minimum_order_quantity,
        source.order_multiple
    FROM supplier_materials AS source
    JOIN suppliers AS supplier
        ON supplier.supplier_id = source.supplier_id
    WHERE source.preferred_flag = TRUE
      AND source.source_status = 'Approved'
      AND supplier.supplier_status = 'Approved'
    ORDER BY source.material_id
    """
)

INSERT_PRODUCTION_DEMAND = text(
    """
    INSERT INTO production_demand (
        demand_reference,
        product_id,
        required_date,
        demand_quantity,
        demand_status,
        priority
    )
    VALUES (
        :demand_reference,
        :product_id,
        :required_date,
        :demand_quantity,
        :demand_status,
        :priority
    )
    """
)

INSERT_INVENTORY_BALANCES = text(
    """
    INSERT INTO inventory_balances (
        material_id,
        location_code,
        on_hand_quantity,
        reserved_quantity,
        restricted_quantity,
        safety_stock_quantity,
        last_counted_at
    )
    VALUES (
        :material_id,
        :location_code,
        :on_hand_quantity,
        :reserved_quantity,
        :restricted_quantity,
        :safety_stock_quantity,
        :last_counted_at
    )
    """
)

INSERT_PURCHASE_ORDERS = text(
    """
    INSERT INTO purchase_orders (
        purchase_order_number,
        line_number,
        supplier_id,
        material_id,
        order_date,
        expected_receipt_date,
        ordered_quantity,
        received_quantity,
        unit_price,
        purchase_order_status
    )
    VALUES (
        :purchase_order_number,
        :line_number,
        :supplier_id,
        :material_id,
        :order_date,
        :expected_receipt_date,
        :ordered_quantity,
        :received_quantity,
        :unit_price,
        :purchase_order_status
    )
    """
)


def get_active_products(engine):
    """Return active product master records from PostgreSQL."""
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(ACTIVE_PRODUCTS_QUERY).mappings()]


def get_material_requirement_totals(engine):
    """Return active materials with gross demand across the planning horizon."""
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(MATERIAL_REQUIREMENT_TOTALS_QUERY).mappings()
        ]


def get_preferred_material_sources(engine):
    """Return each material's preferred approved purchasing rules."""
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(PREFERRED_MATERIAL_SOURCES_QUERY).mappings()
        ]


def table_has_rows(connection, table_name):
    """Return whether a supported transactional table already contains data."""
    supported_tables = {
        "production_demand",
        "inventory_balances",
        "purchase_orders",
    }
    if table_name not in supported_tables:
        raise ValueError(f"Unsupported table: {table_name}")
    return connection.execute(text(f"SELECT EXISTS (SELECT 1 FROM {table_name})")).scalar_one()


def load_production_demand(engine, demand_rows):
    """Insert demand rows once and safely skip an already-populated table."""
    if not demand_rows:
        raise ValueError("No production demand rows were provided")

    with engine.begin() as connection:
        if table_has_rows(connection, "production_demand"):
            return 0
        connection.execute(INSERT_PRODUCTION_DEMAND, demand_rows)
    return len(demand_rows)


def load_inventory_balances(engine, inventory_rows):
    """Insert inventory rows once and skip an already-populated table."""
    if not inventory_rows:
        raise ValueError("No inventory balance rows were provided")

    with engine.begin() as connection:
        if table_has_rows(connection, "inventory_balances"):
            return 0
        connection.execute(INSERT_INVENTORY_BALANCES, inventory_rows)
    return len(inventory_rows)


def load_purchase_orders(engine, purchase_order_rows):
    """Insert purchase-order rows once and skip an already-populated table."""
    if not purchase_order_rows:
        raise ValueError("No purchase-order rows were provided")

    with engine.begin() as connection:
        if table_has_rows(connection, "purchase_orders"):
            return 0
        connection.execute(INSERT_PURCHASE_ORDERS, purchase_order_rows)
    return len(purchase_order_rows)
