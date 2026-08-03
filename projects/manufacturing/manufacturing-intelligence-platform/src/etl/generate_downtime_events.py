import random
from datetime import datetime, time, timedelta, timezone


DOWNTIME_RULES = {
    "Mechanical Failure": {
        "minutes": (30, 240),
        "planned": False,
        "reason": "Unexpected mechanical fault required troubleshooting.",
    },
    "Tool Change": {
        "minutes": (10, 45),
        "planned": True,
        "reason": "Scheduled replacement of worn production tooling.",
    },
    "Setup": {
        "minutes": (15, 90),
        "planned": True,
        "reason": "Machine setup and parameter verification before production.",
    },
    "Material Shortage": {
        "minutes": (30, 360),
        "planned": False,
        "reason": "Production paused while awaiting required material.",
    },
    "Quality Hold": {
        "minutes": (20, 180),
        "planned": False,
        "reason": "Production paused for quality review and disposition.",
    },
    "Preventive Maintenance": {
        "minutes": (60, 480),
        "planned": True,
        "reason": "Planned preventive maintenance service window.",
    },
    "Operator Unavailable": {
        "minutes": (15, 120),
        "planned": False,
        "reason": "Qualified operator was temporarily unavailable.",
    },
    "Changeover": {
        "minutes": (20, 120),
        "planned": True,
        "reason": "Planned product and tooling changeover.",
    },
    "Power Interruption": {
        "minutes": (10, 180),
        "planned": False,
        "reason": "Unexpected facility power interruption.",
    },
}

LINKED_CATEGORY_WEIGHTS = {
    "Mechanical Failure": 25,
    "Tool Change": 18,
    "Setup": 12,
    "Material Shortage": 10,
    "Quality Hold": 15,
    "Operator Unavailable": 10,
    "Changeover": 7,
    "Power Interruption": 3,
}


def choose_linked_category(operation_type):
    """Choose a downtime category for an active manufacturing run."""

    weights = LINKED_CATEGORY_WEIGHTS.copy()

    if operation_type in {"Cold Heading", "Thread Rolling"}:
        weights["Tool Change"] += 12

    if operation_type == "Inspection":
        weights["Quality Hold"] += 20

    if operation_type in {"Assembly", "Packaging"}:
        weights["Operator Unavailable"] += 10

    categories = list(weights)
    return random.choices(
        categories,
        weights=[weights[category] for category in categories],
        k=1,
    )[0]


def generate_event_timestamps(window_start, window_end, category):
    """Generate an event fully contained within an operating window."""

    available_minutes = max(
        1,
        int((window_end - window_start).total_seconds() // 60),
    )
    minimum_minutes, maximum_minutes = DOWNTIME_RULES[category]["minutes"]
    maximum_allowed = min(maximum_minutes, available_minutes)
    minimum_allowed = min(minimum_minutes, maximum_allowed)
    downtime_minutes = random.randint(minimum_allowed, maximum_allowed)
    latest_start = window_end - timedelta(minutes=downtime_minutes)
    start_range_seconds = max(
        0,
        int((latest_start - window_start).total_seconds()),
    )
    downtime_start = window_start + timedelta(
        seconds=random.randint(0, start_range_seconds)
    )
    downtime_end = downtime_start + timedelta(minutes=downtime_minutes)

    return downtime_start, downtime_end, downtime_minutes


def build_downtime_event(machine_id, production_run_id, category, start, end):
    """Build one downtime event dictionary."""

    rule = DOWNTIME_RULES[category]
    downtime_minutes = int((end - start).total_seconds() // 60)
    return {
        "machine_id": machine_id,
        "production_run_id": production_run_id,
        "downtime_start": start,
        "downtime_end": end,
        "downtime_minutes": downtime_minutes,
        "downtime_category": category,
        "downtime_reason": rule["reason"],
        "planned_flag": rule["planned"],
    }


def generate_downtime_events(production_runs, machines, start_date, end_date):
    """Generate run-linked interruptions and standalone planned downtime."""

    downtime_events = []

    for production_run in production_runs:
        run_status = production_run["run_status"]

        if run_status == "Completed":
            event_probability = 0.08
        elif run_status == "Running":
            event_probability = 0.25
        else:
            continue

        if random.random() >= event_probability:
            continue

        window_start = production_run["start_timestamp"]
        window_end = production_run["end_timestamp"]

        if window_end is None:
            window_end = window_start + timedelta(hours=10)

        category = choose_linked_category(
            production_run["operation_type"]
        )
        downtime_start, downtime_end, _ = generate_event_timestamps(
            window_start,
            window_end,
            category,
        )
        downtime_events.append(
            build_downtime_event(
                production_run["machine_id"],
                production_run["production_run_id"],
                category,
                downtime_start,
                downtime_end,
            )
        )

    operating_days = (end_date - start_date).days

    for machine in machines:
        standalone_event_count = random.randint(2, 5)

        for _ in range(standalone_event_count):
            category = random.choices(
                ["Preventive Maintenance", "Setup", "Changeover"],
                weights=[55, 20, 25],
                k=1,
            )[0]
            event_date = start_date + timedelta(
                days=random.randint(0, operating_days)
            )
            window_start = datetime.combine(
                event_date,
                time(hour=6),
                tzinfo=timezone.utc,
            )
            window_end = window_start + timedelta(hours=12)
            downtime_start, downtime_end, _ = generate_event_timestamps(
                window_start,
                window_end,
                category,
            )
            downtime_events.append(
                build_downtime_event(
                    machine["machine_id"],
                    None,
                    category,
                    downtime_start,
                    downtime_end,
                )
            )

    return downtime_events
