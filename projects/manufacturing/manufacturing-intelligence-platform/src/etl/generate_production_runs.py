import random
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP


ROUTES_BY_PRODUCT_FAMILY = {
    "Solid Rivet": [
        "Cold Heading",
        "Heat Treatment",
        "Surface Finishing",
        "Inspection",
        "Packaging",
    ],
    "Blind Rivet": [
        "Cold Heading",
        "Surface Finishing",
        "Assembly",
        "Inspection",
        "Packaging",
    ],
    "Blind Bolt": [
        "Cold Heading",
        "Thread Rolling",
        "Heat Treatment",
        "Surface Finishing",
        "Assembly",
        "Inspection",
        "Packaging",
    ],
    "Temporary Fastener": [
        "Cold Heading",
        "Thread Rolling",
        "Heat Treatment",
        "Surface Finishing",
        "Assembly",
        "Inspection",
        "Packaging",
    ],
    "Threaded Insert": [
        "Thread Rolling",
        "Heat Treatment",
        "Surface Finishing",
        "Inspection",
        "Packaging",
    ],
    "Installation Tool": ["Assembly", "Inspection", "Packaging"],
    "Other": ["Multi-Purpose", "Inspection", "Packaging"],
}

CYCLE_TIME_MULTIPLIER = {
    "Cold Heading": Decimal("1.00"),
    "Thread Rolling": Decimal("1.15"),
    "Heat Treatment": Decimal("0.50"),
    "Surface Finishing": Decimal("0.80"),
    "Assembly": Decimal("1.40"),
    "Inspection": Decimal("1.20"),
    "Packaging": Decimal("0.50"),
    "Multi-Purpose": Decimal("1.00"),
}


def distribute_quantity(total_quantity, bucket_count):
    """Distribute an integer quantity across operation buckets."""

    if bucket_count == 0:
        return []

    base_quantity, remainder = divmod(total_quantity, bucket_count)
    quantities = [base_quantity] * bucket_count

    for index in range(remainder):
        quantities[index] += 1

    random.shuffle(quantities)
    return quantities


def get_run_statuses(production_status, operation_count):
    """Return route-level run statuses for a production-order status."""

    if production_status == "Scheduled":
        return ["Planned"] * operation_count

    if production_status == "Completed":
        return ["Completed"] * operation_count

    if production_status == "Cancelled":
        return ["Cancelled"] * operation_count

    if production_status == "In Production":
        running_index = random.randrange(operation_count)
        return (
            ["Completed"] * running_index
            + ["Running"]
            + ["Planned"] * (operation_count - running_index - 1)
        )

    return []


def generate_cycle_times(standard_cycle_time, operation_type, run_status):
    """Generate planned and actual cycle times for one operation."""

    planned_cycle_time = (
        Decimal(str(standard_cycle_time))
        * CYCLE_TIME_MULTIPLIER[operation_type]
    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    if run_status not in {"Completed", "Running", "Interrupted"}:
        return planned_cycle_time, None

    performance_factor = Decimal(str(random.uniform(0.92, 1.18)))
    actual_cycle_time = (planned_cycle_time * performance_factor).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )
    return planned_cycle_time, actual_cycle_time


def generate_operation_quantities(production_order, run_statuses):
    """Generate balanced quantities that flow through active operations."""

    quantities = []
    active_count = sum(
        status in {"Completed", "Running"} for status in run_statuses
    )

    if active_count == 0:
        return [(0, 0, 0, 0)] * len(run_statuses)

    scrap_by_operation = distribute_quantity(
        production_order["scrapped_quantity"],
        active_count,
    )
    current_input = (
        production_order["completed_quantity"]
        + production_order["scrapped_quantity"]
    )
    active_index = 0

    for run_status in run_statuses:
        if run_status not in {"Completed", "Running"}:
            quantities.append((0, 0, 0, 0))
            continue

        scrap_quantity = scrap_by_operation[active_index]
        is_last_active_operation = active_index == active_count - 1

        if is_last_active_operation:
            rework_quantity = 0
        else:
            rework_quantity = min(
                current_input - scrap_quantity,
                round(current_input * random.uniform(0.002, 0.015)),
            )

        good_quantity = (
            current_input - scrap_quantity - rework_quantity
        )
        quantities.append(
            (
                current_input,
                good_quantity,
                scrap_quantity,
                rework_quantity,
            )
        )
        current_input = good_quantity + rework_quantity
        active_index += 1

    return quantities


def generate_run_timestamps(production_order, run_statuses):
    """Generate sequential timestamps consistent with run statuses."""

    timestamps = []
    actual_start = production_order["actual_start_timestamp"]
    actual_end = production_order["actual_end_timestamp"]
    active_count = sum(
        status in {"Completed", "Running"} for status in run_statuses
    )

    if active_count == 0 or actual_start is None:
        return [(None, None)] * len(run_statuses)

    if actual_end is not None:
        total_seconds = (actual_end - actual_start).total_seconds()
        slot_seconds = total_seconds / active_count
    else:
        slot_seconds = 10 * 60 * 60

    active_index = 0

    for run_status in run_statuses:
        if run_status not in {"Completed", "Running"}:
            timestamps.append((None, None))
            continue

        start_timestamp = actual_start + timedelta(
            seconds=slot_seconds * active_index
        )

        if run_status == "Running":
            end_timestamp = None
        else:
            end_timestamp = start_timestamp + timedelta(
                seconds=slot_seconds * 0.85
            )

        timestamps.append((start_timestamp, end_timestamp))
        active_index += 1

    return timestamps


def generate_production_runs(
    production_orders,
    machines_by_operation,
    operators_by_role,
):
    """Generate routed manufacturing-operation runs for production orders."""

    production_runs = []

    for production_order in production_orders:
        route = ROUTES_BY_PRODUCT_FAMILY[
            production_order["product_family"]
        ]
        run_statuses = get_run_statuses(
            production_order["production_status"],
            len(route),
        )

        if not run_statuses:
            continue

        operation_quantities = generate_operation_quantities(
            production_order,
            run_statuses,
        )
        run_timestamps = generate_run_timestamps(
            production_order,
            run_statuses,
        )

        for index, operation_type in enumerate(route):
            machine_ids = machines_by_operation.get(operation_type, [])
            operator_role = (
                "Inspector" if operation_type == "Inspection" else "Operator"
            )
            operator_ids = operators_by_role.get(operator_role, [])

            if not machine_ids:
                raise ValueError(
                    f"No available machine found for operation: {operation_type}"
                )

            if not operator_ids:
                raise ValueError(
                    f"No available employee found for role: {operator_role}"
                )

            planned_cycle_time, actual_cycle_time = generate_cycle_times(
                production_order["standard_cycle_time_seconds"],
                operation_type,
                run_statuses[index],
            )
            input_quantity, good_quantity, scrap_quantity, rework_quantity = (
                operation_quantities[index]
            )
            start_timestamp, end_timestamp = run_timestamps[index]

            production_runs.append(
                {
                    "production_order_id": production_order[
                        "production_order_id"
                    ],
                    "machine_id": random.choice(machine_ids),
                    "operator_id": random.choice(operator_ids),
                    "operation_sequence": index + 1,
                    "operation_type": operation_type,
                    "start_timestamp": start_timestamp,
                    "end_timestamp": end_timestamp,
                    "planned_cycle_time_seconds": planned_cycle_time,
                    "actual_cycle_time_seconds": actual_cycle_time,
                    "input_quantity": input_quantity,
                    "good_quantity": good_quantity,
                    "scrap_quantity": scrap_quantity,
                    "rework_quantity": rework_quantity,
                    "run_status": run_statuses[index],
                }
            )

    return production_runs
