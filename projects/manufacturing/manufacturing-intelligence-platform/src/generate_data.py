from sqlalchemy import create_engine, text

from etl.generate_customer_orders import generate_customer_orders
from etl.load import load_customer_orders


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
            product_family
        FROM products
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

    print("\nFirst product retrieved:")
    print(products[0])


if __name__ == "__main__":
    main()