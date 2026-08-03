import random
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP


COMPONENTS_BY_OPERATION = {
    "Cold Heading": ["Forming Die", "Feed System", "Hydraulic System"],
    "Thread Rolling": ["Thread Dies", "Spindle", "Feed System"],
    "Heat Treatment": ["Heating Element", "Thermocouple", "Conveyor"],
    "Surface Finishing": ["Spray Nozzle", "Circulation Pump", "Filter System"],
    "Assembly": ["Parts Feeder", "Pneumatic Actuator", "Control System"],
    "Inspection": ["Optical Sensor", "Measurement Probe", "Calibration Stage"],
    "Packaging": ["Heat Sealer", "Conveyor", "Label Applicator"],
    "Multi-Purpose": ["Spindle", "Drive Motor", "Control System"],
}

MAINTENANCE_ACTIONS = {
    "Preventive": "Completed scheduled service, lubrication, and wear inspection.",
    "Corrective": "Diagnosed equipment failure and repaired the affected component.",
    "Predictive": "Investigated condition-monitoring trend and serviced the component.",
    "Calibration": "Calibrated measurement system against certified reference standards.",
    "Inspection": "Completed equipment condition and safety inspection.",
}

COST_RANGES = {
    "Preventive": (300, 1800),
    "Corrective": (1000, 12000),
    "Predictive": (500, 3000),
    "Calibration": (250, 1500),
    "Inspection": (150, 800),
}

DURATION_HOURS = {
    "Predictive": (1, 4),
    "Calibration": (1, 3),
    "Inspection": (1, 2),
}


def calculate_machine_hours(machine, maintenance_start):
    """Estimate cumulative operating hours at the service timestamp."""

    days_in_service = max(
        0,
        (maintenance_start.date() - machine["install_date"]).days,
    )
    utilization = Decimal("0.72") + Decimal(
        str((machine["machine_id"] % 5) * 0.03)
    )
    machine_hours = Decimal(days_in_service * 16) * utilization
    return machine_hours.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def generate_maintenance_cost(maintenance_type, duration_hours):
    """Generate a maintenance cost influenced by type and duration."""

    minimum_cost, maximum_cost = COST_RANGES[maintenance_type]
    base_cost = Decimal(str(random.uniform(minimum_cost, maximum_cost)))
    duration_factor = Decimal("1") + Decimal(str(duration_hours)) * Decimal(
        "0.03"
    )
    return (base_cost * duration_factor).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )


def build_maintenance_event(
    machine,
    maintenance_type,
    maintenance_start,
    maintenance_end,
    reported_timestamp,
    technician_names,
):
    """Build one internally consistent maintenance event."""

    duration_hours = max(
        Decimal("0.01"),
        Decimal(str((maintenance_end - maintenance_start).total_seconds() / 3600)),
    )
    return {
        "machine_id": machine["machine_id"],
        "maintenance_type": maintenance_type,
        "reported_timestamp": reported_timestamp,
        "maintenance_start": maintenance_start,
        "maintenance_end": maintenance_end,
        "technician": random.choice(technician_names),
        "failure_component": random.choice(
            COMPONENTS_BY_OPERATION[machine["operation_type"]]
        ),
        "maintenance_action": MAINTENANCE_ACTIONS[maintenance_type],
        "maintenance_cost": generate_maintenance_cost(
            maintenance_type,
            duration_hours,
        ),
        "machine_hours_at_service": calculate_machine_hours(
            machine,
            maintenance_start,
        ),
    }


def generate_maintenance_events(
    downtime_events,
    machines,
    technician_names,
    start_date,
    end_date,
):
    """Generate downtime-driven and routine equipment maintenance history."""

    if not technician_names:
        raise ValueError("At least one certified technician is required.")

    machines_by_id = {
        machine["machine_id"]: machine for machine in machines
    }
    maintenance_events = []

    for downtime_event in downtime_events:
        category = downtime_event["downtime_category"]

        if category == "Mechanical Failure":
            maintenance_type = "Corrective"
            reported_timestamp = downtime_event["downtime_start"] - timedelta(
                minutes=random.randint(0, 30)
            )
        elif category == "Preventive Maintenance":
            maintenance_type = "Preventive"
            reported_timestamp = downtime_event["downtime_start"] - timedelta(
                days=random.randint(7, 30)
            )
        else:
            continue

        maintenance_events.append(
            build_maintenance_event(
                machines_by_id[downtime_event["machine_id"]],
                maintenance_type,
                downtime_event["downtime_start"],
                downtime_event["downtime_end"],
                reported_timestamp,
                technician_names,
            )
        )

    operating_days = (end_date - start_date).days

    for machine in machines:
        planned_types = ["Predictive", "Inspection"]

        if machine["operation_type"] == "Inspection":
            planned_types.extend(["Calibration", "Calibration"])

        for maintenance_type in planned_types:
            event_date = start_date + timedelta(
                days=random.randint(0, operating_days)
            )
            start_hour = random.randint(6, 14)
            maintenance_start = datetime.combine(
                event_date,
                time(hour=start_hour),
                tzinfo=timezone.utc,
            )
            minimum_hours, maximum_hours = DURATION_HOURS[maintenance_type]
            maintenance_end = maintenance_start + timedelta(
                hours=random.randint(minimum_hours, maximum_hours)
            )
            reported_timestamp = maintenance_start - timedelta(
                days=random.randint(3, 21)
            )
            maintenance_events.append(
                build_maintenance_event(
                    machine,
                    maintenance_type,
                    maintenance_start,
                    maintenance_end,
                    reported_timestamp,
                    technician_names,
                )
            )

    maintenance_events.sort(
        key=lambda event: (event["maintenance_start"], event["machine_id"])
    )
    return maintenance_events
