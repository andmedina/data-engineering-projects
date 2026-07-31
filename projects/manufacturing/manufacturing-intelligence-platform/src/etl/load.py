from sqlalchemy import text


def load_customer_orders(engine, customer_orders):
    """Insert generated customer orders into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM customer_orders
        """
    )

    insert_query = text(
        """
        INSERT INTO customer_orders (
            customer_order_number,
            customer_id,
            order_date,
            requested_delivery_date,
            priority,
            order_status,
            notes
        )
        VALUES (
            :customer_order_number,
            :customer_id,
            :order_date,
            :requested_delivery_date,
            :priority,
            :order_status,
            :notes
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "customer_orders already contains data. "
                "The load was stopped to prevent duplicate order numbers."
            )

        connection.execute(insert_query, customer_orders)

    print(f"Loaded {len(customer_orders)} customer orders into PostgreSQL.")