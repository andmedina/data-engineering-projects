"""Orchestrate synthetic planning-data generation and loading."""

from sqlalchemy import create_engine

from .config import DATABASE_URL
from .etl.generate_inventory_balances import generate_inventory_balances
from .etl.generate_production_demand import generate_production_demand
from .etl.load import (
    get_active_products,
    get_material_requirement_totals,
    load_inventory_balances,
    load_production_demand,
)


def main():
    """Generate and load the first planning dataset in dependency order."""
    engine = create_engine(DATABASE_URL)
    products = get_active_products(engine)
    demand_rows = generate_production_demand(products)
    inserted_rows = load_production_demand(engine, demand_rows)

    if inserted_rows:
        print(f"Loaded {inserted_rows} production demand rows.")
    else:
        print("Skipped production_demand because the table already contains data.")

    material_requirements = get_material_requirement_totals(engine)
    inventory_rows = generate_inventory_balances(material_requirements)
    inserted_rows = load_inventory_balances(engine, inventory_rows)

    if inserted_rows:
        print(f"Loaded {inserted_rows} inventory balance rows.")
    else:
        print("Skipped inventory_balances because the table already contains data.")


if __name__ == "__main__":
    main()
