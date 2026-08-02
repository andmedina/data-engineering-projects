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


def load_customer_order_items(engine, customer_order_items):
    """Insert generated customer order items into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM customer_order_items
        """
    )

    insert_query = text(
        """
        INSERT INTO customer_order_items (
            customer_order_id,
            line_number,
            product_id,
            ordered_quantity,
            unit_price,
            line_status
        )
        VALUES (
            :customer_order_id,
            :line_number,
            :product_id,
            :ordered_quantity,
            :unit_price,
            :line_status
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "customer_order_items already contains data. "
                "The load was stopped to prevent duplicate order lines."
            )

        connection.execute(insert_query, customer_order_items)

    print(
        f"Loaded {len(customer_order_items)} customer order items "
        "into PostgreSQL."
    )
