import random
from decimal import Decimal, ROUND_HALF_UP


TENSILE_STRENGTH_BY_MATERIAL = {
    "Aluminum": (Decimal("40000"), Decimal("65000")),
    "Titanium": (Decimal("120000"), Decimal("160000")),
    "Stainless Steel": (Decimal("75000"), Decimal("110000")),
    "Alloy Steel": (Decimal("95000"), Decimal("150000")),
    "Nickel Alloy": (Decimal("110000"), Decimal("170000")),
}


def choose_measurement_type(production_run):
    """Choose a primary measurement appropriate for an operation."""

    operation_measurements = {
        "Cold Heading": ["Diameter", "Head Height"],
        "Thread Rolling": ["Thread Pitch"],
        "Heat Treatment": ["Tensile Strength"],
        "Surface Finishing": ["Surface Finish", "Coating Thickness"],
        "Assembly": ["Assembly Gap"],
        "Inspection": ["Diameter", "Length"],
        "Packaging": ["Package Count"],
        "Multi-Purpose": ["Length"],
    }
    available_measurements = operation_measurements[
        production_run["operation_type"]
    ]

    if production_run["diameter_in"] is None:
        available_measurements = [
            measurement
            for measurement in available_measurements
            if measurement not in {"Diameter", "Head Height"}
        ] or ["Assembly Gap"]

    if production_run["length_in"] is None:
        available_measurements = [
            measurement
            for measurement in available_measurements
            if measurement != "Length"
        ] or ["Assembly Gap"]

    return random.choice(available_measurements)


def get_specification(production_run, measurement_type):
    """Return lower limit, upper limit, and nominal measurement value."""

    diameter = production_run["diameter_in"]
    length = production_run["length_in"]

    if measurement_type == "Diameter":
        nominal = Decimal(str(diameter))
        tolerance = Decimal("0.0025")
        return nominal - tolerance, nominal + tolerance, nominal

    if measurement_type == "Length":
        nominal = Decimal(str(length))
        tolerance = Decimal("0.0050")
        return nominal - tolerance, nominal + tolerance, nominal

    if measurement_type == "Head Height":
        nominal = Decimal(str(diameter)) * Decimal("0.60")
        tolerance = Decimal("0.0030")
        return nominal - tolerance, nominal + tolerance, nominal

    if measurement_type == "Thread Pitch":
        nominal = Decimal("0.0313")
        tolerance = Decimal("0.0010")
        return nominal - tolerance, nominal + tolerance, nominal

    if measurement_type == "Surface Finish":
        return Decimal("16.0000"), Decimal("63.0000"), Decimal("32.0000")

    if measurement_type == "Coating Thickness":
        return Decimal("0.0003"), Decimal("0.0008"), Decimal("0.0005")

    if measurement_type == "Tensile Strength":
        lower_limit, upper_limit = TENSILE_STRENGTH_BY_MATERIAL[
            production_run["material_category"]
        ]
        nominal = (lower_limit + upper_limit) / Decimal("2")
        return lower_limit, upper_limit, nominal

    if measurement_type == "Assembly Gap":
        return Decimal("0.0000"), Decimal("0.0050"), Decimal("0.0020")

    return Decimal("0.0000"), Decimal("0.0000"), Decimal("0.0000")


def generate_sample_results(production_run):
    """Generate balanced sampled pass and failure quantities."""

    processed_quantity = (
        production_run["good_quantity"]
        + production_run["scrap_quantity"]
        + production_run["rework_quantity"]
    )

    if processed_quantity == 0:
        return 0, 0, 0, "Pending"

    target_sample_size = random.choice([5, 10, 20, 32, 50])
    sample_size = min(processed_quantity, target_sample_size)
    observed_loss_rate = (
        production_run["scrap_quantity"]
        + production_run["rework_quantity"]
    ) / processed_quantity
    failure_probability = min(0.15, max(0.005, observed_loss_rate * 1.5))
    failed_quantity = sum(
        random.random() < failure_probability for _ in range(sample_size)
    )
    passed_quantity = sample_size - failed_quantity

    if failed_quantity == 0:
        inspection_result = "Pass"
    elif failed_quantity / sample_size <= 0.05:
        inspection_result = "Conditional"
    else:
        inspection_result = "Fail"

    return (
        sample_size,
        passed_quantity,
        failed_quantity,
        inspection_result,
    )


def generate_measured_value(
    lower_limit,
    upper_limit,
    nominal,
    inspection_result,
):
    """Generate a measurement consistent with the inspection result."""

    if inspection_result == "Pending":
        return None

    specification_width = upper_limit - lower_limit

    if inspection_result == "Fail":
        if random.random() < 0.50:
            measured_value = lower_limit - specification_width * Decimal(
                str(random.uniform(0.02, 0.15))
            )
        else:
            measured_value = upper_limit + specification_width * Decimal(
                str(random.uniform(0.02, 0.15))
            )
    elif inspection_result == "Conditional":
        direction = random.choice([-1, 1])
        measured_value = nominal + Decimal(direction) * specification_width * Decimal(
            str(random.uniform(0.40, 0.52))
        )
    else:
        measured_value = nominal + specification_width * Decimal(
            str(random.uniform(-0.25, 0.25))
        )

    measured_value = measured_value.quantize(
        Decimal("0.0001"),
        rounding=ROUND_HALF_UP,
    )

    if (
        inspection_result == "Fail"
        and lower_limit <= measured_value <= upper_limit
    ):
        measured_value = random.choice(
            [lower_limit - Decimal("0.0001"), upper_limit + Decimal("0.0001")]
        )

    return measured_value


def should_generate_inspection(production_run):
    """Return whether a production run should receive an inspection."""

    if production_run["run_status"] == "Running":
        return production_run["operation_type"] == "Inspection"

    if production_run["run_status"] != "Completed":
        return False

    if production_run["operation_type"] == "Inspection":
        return True

    in_process_operations = {
        "Cold Heading",
        "Thread Rolling",
        "Heat Treatment",
        "Surface Finishing",
        "Assembly",
        "Multi-Purpose",
    }
    return (
        production_run["operation_type"] in in_process_operations
        and random.random() < 0.20
    )


def generate_quality_inspections(production_runs, inspector_ids):
    """Generate in-process and final quality inspection events."""

    if not inspector_ids:
        raise ValueError("At least one certified inspector is required.")

    inspections = []

    for production_run in production_runs:
        if not should_generate_inspection(production_run):
            continue

        measurement_type = choose_measurement_type(production_run)
        lower_limit, upper_limit, nominal = get_specification(
            production_run,
            measurement_type,
        )

        if production_run["run_status"] == "Running":
            sample_size, passed_quantity, failed_quantity = 0, 0, 0
            inspection_result = "Pending"
        else:
            (
                sample_size,
                passed_quantity,
                failed_quantity,
                inspection_result,
            ) = generate_sample_results(production_run)

        inspection_timestamp = (
            production_run["end_timestamp"]
            or production_run["start_timestamp"]
        )

        inspections.append(
            {
                "production_run_id": production_run["production_run_id"],
                "inspector_id": random.choice(inspector_ids),
                "inspection_timestamp": inspection_timestamp,
                "sample_size": sample_size,
                "passed_quantity": passed_quantity,
                "failed_quantity": failed_quantity,
                "inspection_result": inspection_result,
                "measurement_type": measurement_type,
                "measured_value": generate_measured_value(
                    lower_limit,
                    upper_limit,
                    nominal,
                    inspection_result,
                ),
                "lower_spec_limit": lower_limit.quantize(Decimal("0.0001")),
                "upper_spec_limit": upper_limit.quantize(Decimal("0.0001")),
            }
        )

    return inspections
