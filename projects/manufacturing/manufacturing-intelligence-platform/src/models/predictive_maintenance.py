"""Build the cold-heading predictive-maintenance modeling dataset.

The output grain is one five-minute reading for one cold-heading machine. Each
row contains the current sensor values, trailing one-hour features, and a
binary label indicating whether a mechanical failure begins in the next hour.

This module prepares data only. Model fitting and evaluation are kept in
``train_predictive_maintenance.py`` so each stage can be read and tested on its
own.
"""

import pandas as pd
from sqlalchemy import text


SENSOR_COLUMNS = [
    "temperature_c",
    "vibration_mm_s",
    "power_kw",
    "pressure_psi",
    "rpm",
]

# Twelve five-minute readings represent the trailing 60-minute feature window.
ROLLING_WINDOW_SIZE = 12
FAILURE_HORIZON_MINUTES = 60

SENSOR_QUERY = text(
    """
    SELECT
        s.machine_id,
        m.machine_code,
        s.reading_timestamp,
        s.temperature_c,
        s.vibration_mm_s,
        s.power_kw,
        s.pressure_psi,
        s.rpm
    FROM sensor_readings s
    JOIN machines m ON m.machine_id = s.machine_id
    WHERE m.operation_type = 'Cold Heading'
      AND s.reading_timestamp <= CURRENT_TIMESTAMP
    ORDER BY s.machine_id, s.reading_timestamp
    """
)

FAILURE_QUERY = text(
    """
    SELECT
        d.machine_id,
        d.downtime_start AS failure_timestamp,
        me.failure_component
    FROM downtime_events d
    JOIN machines m ON m.machine_id = d.machine_id
    LEFT JOIN maintenance_events me
      ON me.machine_id = d.machine_id
     AND me.maintenance_type = 'Corrective'
     AND me.maintenance_start = d.downtime_start
    WHERE m.operation_type = 'Cold Heading'
      AND d.downtime_category = 'Mechanical Failure'
      AND d.downtime_start <= CURRENT_TIMESTAMP
    ORDER BY d.machine_id, d.downtime_start
    """
)

DOWNTIME_QUERY = text(
    """
    SELECT d.machine_id, d.downtime_start, d.downtime_end
    FROM downtime_events d
    JOIN machines m ON m.machine_id = d.machine_id
    WHERE m.operation_type = 'Cold Heading'
      AND d.downtime_start <= CURRENT_TIMESTAMP
    ORDER BY d.machine_id, d.downtime_start
    """
)


def load_source_data(engine):
    """Load cold-heading telemetry, failures, and downtime from PostgreSQL."""
    sensors = pd.read_sql(SENSOR_QUERY, engine)
    failures = pd.read_sql(FAILURE_QUERY, engine)
    downtime = pd.read_sql(DOWNTIME_QUERY, engine)

    sensors["reading_timestamp"] = pd.to_datetime(
        sensors["reading_timestamp"], utc=True
    )
    failures["failure_timestamp"] = pd.to_datetime(
        failures["failure_timestamp"], utc=True
    )
    downtime["downtime_start"] = pd.to_datetime(
        downtime["downtime_start"], utc=True
    )
    downtime["downtime_end"] = pd.to_datetime(
        downtime["downtime_end"], utc=True
    )
    return sensors, failures, downtime


def add_rolling_features(
    sensor_readings,
    window_size=ROLLING_WINDOW_SIZE,
    expected_frequency="5min",
):
    """Add trailing statistics without using readings from the future.

    A complete rolling window is required. The first 11 readings per machine
    are therefore removed rather than receiving incomplete feature values.
    """
    readings = sensor_readings.sort_values(
        ["machine_id", "reading_timestamp"]
    ).copy()
    # Restart the rolling window after a telemetry gap. This prevents readings
    # from opposite sides of a downtime event being treated as one continuous
    # hour of operation.
    time_gap = readings.groupby("machine_id")["reading_timestamp"].diff()
    readings["operating_segment"] = (
        time_gap.gt(pd.Timedelta(expected_frequency))
        .groupby(readings["machine_id"])
        .cumsum()
    )
    grouped = readings.groupby(
        ["machine_id", "operating_segment"], sort=False
    )

    for column in SENSOR_COLUMNS:
        rolling = grouped[column].rolling(window_size, min_periods=window_size)
        readings[f"{column}_mean_60m"] = rolling.mean().reset_index(
            level=[0, 1], drop=True
        )
        readings[f"{column}_std_60m"] = rolling.std().reset_index(
            level=[0, 1], drop=True
        )
        readings[f"{column}_max_60m"] = rolling.max().reset_index(
            level=[0, 1], drop=True
        )
        readings[f"{column}_change_60m"] = grouped[column].diff(window_size - 1)

    return readings.dropna().drop(columns="operating_segment").reset_index(drop=True)


def add_failure_labels(
    sensor_readings, failures, horizon_minutes=FAILURE_HORIZON_MINUTES
):
    """Label readings when the *next* failure starts within the horizon.

    ``allow_exact_matches=False`` excludes a reading at the failure timestamp:
    the intended target is advance warning, not detection after failure onset.
    """
    readings = sensor_readings.sort_values(
        ["machine_id", "reading_timestamp"]
    ).copy()
    failure_rows = failures.sort_values(
        ["machine_id", "failure_timestamp"]
    ).copy()

    labeled_groups = []
    for machine_id, machine_readings in readings.groupby("machine_id"):
        machine_failures = failure_rows[
            failure_rows["machine_id"] == machine_id
        ].drop(columns="machine_id")
        labeled = pd.merge_asof(
            machine_readings.sort_values("reading_timestamp"),
            machine_failures.sort_values("failure_timestamp"),
            left_on="reading_timestamp",
            right_on="failure_timestamp",
            direction="forward",
            allow_exact_matches=False,
        )
        labeled["minutes_to_failure"] = (
            labeled["failure_timestamp"] - labeled["reading_timestamp"]
        ).dt.total_seconds() / 60
        labeled["failure_within_60m"] = (
            labeled["minutes_to_failure"] <= horizon_minutes
        ).astype(int)
        labeled_groups.append(labeled)

    return pd.concat(labeled_groups, ignore_index=True)


def remove_downtime_readings(sensor_readings, downtime_events):
    """Remove readings recorded while a machine is already stopped.

    Zero or abnormal readings during downtime would reveal the outcome and
    produce misleadingly strong evaluation results.
    """
    keep = pd.Series(True, index=sensor_readings.index)

    for event in downtime_events.itertuples(index=False):
        inside_event = (
            (sensor_readings["machine_id"] == event.machine_id)
            & (sensor_readings["reading_timestamp"] >= event.downtime_start)
            & (sensor_readings["reading_timestamp"] <= event.downtime_end)
        )
        keep &= ~inside_event

    return sensor_readings.loc[keep].reset_index(drop=True)


def build_predictive_maintenance_dataset(engine):
    """Return model-ready cold-heading rows with trailing features and labels."""
    sensors, failures, downtime = load_source_data(engine)
    operating_readings = remove_downtime_readings(sensors, downtime)
    features = add_rolling_features(operating_readings)
    labeled = add_failure_labels(features, failures)
    return labeled


def time_based_split(dataset, test_start):
    """Split chronologically so evaluation data is later than training data.

    A random row split is inappropriate for time-series data because adjacent
    five-minute readings from the same failure window are highly correlated.
    """
    test_start = pd.Timestamp(test_start)
    timezone = dataset["reading_timestamp"].dt.tz
    if timezone is not None and test_start.tzinfo is None:
        test_start = test_start.tz_localize(timezone)

    train = dataset[dataset["reading_timestamp"] < test_start].copy()
    test = dataset[dataset["reading_timestamp"] >= test_start].copy()
    return train, test
