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
                downtime_event["downtime_start"]
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
            before_failure = any(
                failure_start - timedelta(minutes=60)
                <= reading_timestamp
                < failure_start
                for failure_start in failures_by_machine[machine_id]
            )
            background_anomaly = random.random() < 0.002
            anomaly = before_failure or background_anomaly
            operating = (
                is_operating(reading_timestamp, machine["status"])
                and not in_downtime
            )

            temperature_multiplier = 1.15 if anomaly and operating else 1.0
            vibration_multiplier = 2.50 if anomaly and operating else 1.0
            power_multiplier = 1.20 if anomaly and operating else 1.0
            pressure_multiplier = 0.75 if anomaly and operating else 1.0
            rpm_multiplier = 0.85 if anomaly and operating else 1.0

            rpm_value = generate_sensor_value(
                profile["rpm"],
                operating,
                multiplier=rpm_multiplier,
                places=0,
            )

            readings.append(
                {
                    "machine_id": machine_id,
                    "reading_timestamp": reading_timestamp,
                    "temperature_c": generate_sensor_value(
                        profile["temperature"],
                        operating,
                        multiplier=temperature_multiplier,
                        places=2,
                    ),
                    "vibration_mm_s": generate_sensor_value(
                        profile["vibration"],
                        operating,
                        multiplier=vibration_multiplier,
                        places=3,
                    ),
                    "power_kw": generate_sensor_value(
                        profile["power"],
                        operating,
                        multiplier=power_multiplier,
                        places=2,
                    ),
                    "pressure_psi": generate_sensor_value(
                        profile["pressure"],
                        operating,
                        multiplier=pressure_multiplier,
                        places=2,
                    ),
                    "rpm": int(rpm_value) if rpm_value is not None else None,
                }
            )
            reading_timestamp += interval

    return readings
