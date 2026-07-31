import random
from datetime import timedelta

from faker import Faker


fake = Faker()


def generate_customer_orders(customer_ids, num_orders=500):
    """Generate synthetic customer-order header records."""

    orders = []

    priorities = ["Low", "Standard", "High", "Rush"]
    priority_weights = [0.05, 0.70, 0.20, 0.05]

    statuses = [
        "Open",
        "Released",
        "Partially Fulfilled",
        "Completed",
        "Cancelled",
    ]
    status_weights = [0.15, 0.15, 0.08, 0.60, 0.02]

    for index in range(num_orders):
        order_date = fake.date_between(
            start_date="-1y",
            end_date="today",
        )

        requested_delivery_date = order_date + timedelta(
            days=random.randint(7, 45)
        )

        order = {
            "customer_order_number": f"SO-{100001 + index}",
            "customer_id": random.choice(customer_ids),
            "order_date": order_date,
            "requested_delivery_date": requested_delivery_date,
            "priority": random.choices(
                priorities,
                weights=priority_weights,
                k=1,
            )[0],
            "order_status": random.choices(
                statuses,
                weights=status_weights,
                k=1,
            )[0],
            "notes": None,
        }

        orders.append(order)

    return orders