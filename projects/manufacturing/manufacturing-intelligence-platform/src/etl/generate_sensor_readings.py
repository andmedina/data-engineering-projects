import random
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP


SENSOR_PROFILES = {
    "Cold Heading": {
        "temperature": ((55, 85), (25, 40)),
        "vibration": ((2.5, 6.5), (0.2, 1.0)),
        "power": ((35, 70), (2, 8)),
        "pressure": ((1200, 2200), (50, 200)),
        "rpm": ((900, 1800), (0, 0)),
    },
    "Thread Rolling": {
        "temperature": ((45, 75), (25, 38)),
        "vibration": ((2.0, 5.5), (0.2, 0.8)),
        "power": ((25, 55), (2, 7)),
        "pressure": ((800, 1600), (40, 150)),
        "rpm": ((700, 1500), (0, 0)),
    },
    "Heat Treatment": {
        "temperature": ((450, 950), (80, 180)),
        "vibration": None,
        "power": ((80, 180), (10, 30)),
        "pressure": None,
        "rpm": None,
    },
    "Surface Finishing": {
        "temperature": ((30, 60), (22, 32)),
        "vibration": ((1.0, 3.5), (0.1, 0.6)),
        "power": ((15, 40), (2, 6)),
        "pressure": ((40, 120), (5, 20)),
        "rpm": ((300, 900), (0, 0)),
    },
    "Assembly": {
        "temperature": ((25, 45), (20, 28)),
        "vibration": ((0.5, 2.5), (0.1, 0.4)),
        "power": ((8, 25), (1, 4)),
        "pressure": ((70, 120), (5, 20)),
        "rpm": ((100, 500), (0, 0)),
    },
    "Inspection": {
        "temperature": ((20, 25), (20, 25)),
        "vibration": None,
        "power": ((3, 12), (1, 3)),
        "pressure": None,
        "rpm": None,
    },
    "Packaging": {
        "temperature": ((24, 40), (20, 28)),
        "vibration": ((0.5, 2.0), (0.1, 0.4)),
        "power": ((10, 30), (1, 5)),
        "pressure": ((60, 100), (5, 15)),
        "rpm": ((200, 700), (0, 0)),
    },
    "Multi-Purpose": {
        "temperature": ((35, 65), (22, 35)),
        "vibration": ((1.5, 4.5), (0.2, 0.8)),
        "power": ((20, 50), (2, 7)),
        "pressure": ((100, 300), (10, 40)),
        "rpm": ((400, 1200), (0, 0)),
    },
}

COLD_HEADING_FAILURE_MULTIPLIERS = {
    "Forming Die": {
        "temperature": 1.08,
        "vibration": 2.80,
        "power": 1.30,
        "pressure": 1.00,
        "rpm": 0.95,
    },
    "Hydraulic System": {
        "temperature": 1.15,
        "vibration": 1.30,
        "power": 1.15,
        "pressure": 0.65,
        "rpm": 0.90,
    },
    "Feed System": {
        "temperature": 1.05,
        "vibration": 2.00,
        "power": 0.85,
        "pressure": 1.00,
        "rpm": 0.55,
    },
}

GENERIC_FAILURE_MULTIPLIERS = {
    "temperature": 1.15,
    "vibration": 2.50,
    "power": 1.20,
    "pressure": 0.75,
    "rpm": 0.85,
}


def is_operating(timestamp, machine_status):
    """Return whether a machine is likely operating at a timestamp."""

    weekday = timestamp.weekday()
    hour = timestamp.hour

    if 6 <= hour < 22:
        probability = 0.85 if weekday < 5 else 0.55
    else:
        probability = 0.18 if weekday < 5 else 0.08

    if machine_status == "Idle":
        probability *= 0.35

    return random.random() < probability


def generate_sensor_value(value_ranges, operating, multiplier=1.0, places=2):
    """Generate one nonnegative sensor value from operating or idle ranges."""

    if value_ranges is None:
        return None

    value_range = value_ranges[0] if operating else value_ranges[1]
    value = max(0, random.uniform(*value_range) * multiplier)
    quantum = Decimal("1").scaleb(-places)
    return Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP)


def get_failure_multipliers(operation_type, failure_component):
    """Return the sensor signature for an upcoming mechanical failure."""
    if operation_type == "Cold Heading":
        return COLD_HEADING_FAILURE_MULTIPLIERS.get(
            failure_component,
            GENERIC_FAILURE_MULTIPLIERS,
        )

    return GENERIC_FAILURE_MULTIPLIERS


def generate_sensor_readings(
    machines,
    downtime_events,
    start_timestamp,
    end_timestamp,
    interval_minutes=5,
):
    """Generate five-minute machine telemetry with downtime and anomalies."""

    downtime_by_machine = defaultdict(list)
    failures_by_machine = defaultdict(list)

    for downtime_event in downtime_events:
        downtime_by_machine[downtime_event["machine_id"]].append(
            (
                downtime_event["downtime_start"],
                downtime_event["downtime_end"],
            )
        )

        if downtime_event["downtime_category"] == "Mechanical Failure":
            failures_by_machine[downtime_event["machine_id"]].append(
                (
                    downtime_event["downtime_start"],
                    downtime_event.get("failure_component"),
                )
            )

    readings = []
    interval = timedelta(minutes=interval_minutes)

    for machine in machines:
        machine_id = machine["machine_id"]
        profile = SENSOR_PROFILES[machine["operation_type"]]
        reading_timestamp = start_timestamp

        while reading_timestamp <= end_timestamp:
            in_downtime = any(
                downtime_start <= reading_timestamp <= downtime_end
                for downtime_start, downtime_end
                in downtime_by_machine[machine_id]
            )
            upcoming_failure = next(
                (
                    (failure_start, failure_component)
                    for failure_start, failure_component
                    in failures_by_machine[machine_id]
                    if failure_start - timedelta(minutes=60)
                    <= reading_timestamp
                    < failure_start
                ),
                None,
            )
            before_failure = upcoming_failure is not None
            background_anomaly = random.random() < 0.002
            anomaly = before_failure or background_anomaly
            operating = (
                is_operating(reading_timestamp, machine["status"])
                and not in_downtime
            )

            if before_failure:
                failure_multipliers = get_failure_multipliers(
                    machine["operation_type"],
                    upcoming_failure[1],
                )
            elif background_anomaly:
                failure_multipliers = GENERIC_FAILURE_MULTIPLIERS
            else:
                failure_multipliers = {
                    sensor_name: 1.0
                    for sensor_name in GENERIC_FAILURE_MULTIPLIERS
                }

            if not anomaly or not operating:
                failure_multipliers = {
                    sensor_name: 1.0
                    for sensor_name in GENERIC_FAILURE_MULTIPLIERS
                }

            rpm_value = generate_sensor_value(
                profile["rpm"],
                operating,
                multiplier=failure_multipliers["rpm"],
                places=0,
            )

            readings.append(
                {
                    "machine_id": machine_id,
                    "reading_timestamp": reading_timestamp,
                    "temperature_c": generate_sensor_value(
                        profile["temperature"],
                        operating,
                        multiplier=failure_multipliers["temperature"],
                        places=2,
                    ),
                    "vibration_mm_s": generate_sensor_value(
                        profile["vibration"],
                        operating,
                        multiplier=failure_multipliers["vibration"],
                        places=3,
                    ),
                    "power_kw": generate_sensor_value(
                        profile["power"],
                        operating,
                        multiplier=failure_multipliers["power"],
                        places=2,
                    ),
                    "pressure_psi": generate_sensor_value(
                        profile["pressure"],
                        operating,
                        multiplier=failure_multipliers["pressure"],
                        places=2,
                    ),
                    "rpm": int(rpm_value) if rpm_value is not None else None,
                }
            )
            reading_timestamp += interval

    return readings
