from sqlalchemy import create_engine, text

from .etl.generate_customer_order_items import generate_customer_order_items
from .etl.generate_customer_orders import generate_customer_orders
from .etl.load import load_customer_order_items, load_customer_orders


DATABASE_URL = (
    "postgresql+psycopg2://amed@localhost/"
    "manufacturing_intelligence"
)


def get_engine():
    """Create and return the SQLAlchemy database engine."""
    return create_engine(DATABASE_URL)


def get_customer_ids(engine):
    """Retrieve valid customer IDs from PostgreSQL."""

    query = text(
        """
        SELECT customer_id
        FROM customers
        ORDER BY customer_id
        """
    )

    with engine.connect() as connection:
        result = connection.execute(query)
        return [row.customer_id for row in result]


def get_products(engine):
    """Retrieve product information from PostgreSQL."""

    query = text(
        """
        SELECT
            product_id,
            product_name,
            product_family,
            standard_unit_cost
        FROM products
        WHERE active_flag = TRUE
        ORDER BY product_id
        """
    )

    with engine.connect() as connection:
        result = connection.execute(query)

        return [
            {
                "product_id": row.product_id,
                "product_name": row.product_name,
                "product_family": row.product_family,
                "standard_unit_cost": row.standard_unit_cost,
            }
            for row in result
        ]


def get_customer_order_count(engine):
    """Return the number of customer orders in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM customer_orders
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_customer_orders(engine):
    """Retrieve customer orders needed to generate their line items."""

    query = text(
        """
        SELECT customer_order_id, order_status
        FROM customer_orders
        ORDER BY customer_order_id
        """
    )

    with engine.connect() as connection:
        result = connection.execute(query)
        return [
            {
                "customer_order_id": row.customer_order_id,
                "order_status": row.order_status,
            }
            for row in result
        ]


def get_customer_order_item_count(engine):
    """Return the number of customer order items in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM customer_order_items
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def main():
    engine = get_engine()

    customer_ids = get_customer_ids(engine)
    products = get_products(engine)

    print("Database connected successfully.")
    print(f"Customer rows found: {len(customer_ids)}")
    print(f"Product rows found: {len(products)}")

    existing_order_count = get_customer_order_count(engine)

    if existing_order_count == 0:
        customer_orders = generate_customer_orders(
            customer_ids=customer_ids,
            num_orders=500,
        )

        print(f"Generated {len(customer_orders)} customer orders.")

        load_customer_orders(
            engine=engine,
            customer_orders=customer_orders,
        )
    else:
        print(
            "Customer orders already exist. "
            "Skipping customer order generation."
        )

    stored_count = get_customer_order_count(engine)

    print(f"Customer orders stored in PostgreSQL: {stored_count}")

    existing_item_count = get_customer_order_item_count(engine)

    if existing_item_count == 0:
        customer_orders = get_customer_orders(engine)
        customer_order_items = generate_customer_order_items(
            customer_orders=customer_orders,
            products=products,
        )

        print(f"Generated {len(customer_order_items)} customer order items.")

        load_customer_order_items(
            engine=engine,
            customer_order_items=customer_order_items,
        )
    else:
        print(
            "Customer order items already exist. "
            "Skipping customer order item generation."
        )

    stored_item_count = get_customer_order_item_count(engine)
    print(f"Customer order items stored in PostgreSQL: {stored_item_count}")


if __name__ == "__main__":
    main()
