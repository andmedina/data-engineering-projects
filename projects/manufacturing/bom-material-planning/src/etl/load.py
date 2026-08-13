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


def get_active_products(engine):
    """Return active product master records from PostgreSQL."""
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(ACTIVE_PRODUCTS_QUERY).mappings()]


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
