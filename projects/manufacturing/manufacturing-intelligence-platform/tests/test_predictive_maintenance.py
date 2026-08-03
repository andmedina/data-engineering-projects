"""Tests for predictive-maintenance feature and label preparation."""

import pandas as pd

from src.models.predictive_maintenance import (
    add_failure_labels,
    add_rolling_features,
    remove_downtime_readings,
    time_based_split,
)


def test_rolling_window_restarts_after_telemetry_gap():
    timestamps = list(pd.date_range("2026-01-01", periods=12, freq="5min", tz="UTC"))
    timestamps += list(
        pd.date_range("2026-01-01 02:00:00", periods=12, freq="5min", tz="UTC")
    )
    readings = pd.DataFrame(
        {
            "machine_id": [1] * 24,
            "reading_timestamp": timestamps,
            "temperature_c": [50.0] * 24,
            "vibration_mm_s": [2.0] * 24,
            "power_kw": [30.0] * 24,
            "pressure_psi": [1_000.0] * 24,
            "rpm": [750] * 24,
        }
    )

    result = add_rolling_features(readings)

    assert len(result) == 2
    assert result["reading_timestamp"].tolist() == [timestamps[11], timestamps[23]]


def test_failure_label_only_marks_readings_inside_future_horizon():
    readings = pd.DataFrame(
        {
            "machine_id": [1, 1, 1],
            "reading_timestamp": pd.to_datetime(
                [
                    "2026-01-01 10:55:00+00:00",
                    "2026-01-01 11:00:00+00:00",
                    "2026-01-01 11:30:00+00:00",
                ]
            ),
        }
    )
    failures = pd.DataFrame(
        {
            "machine_id": [1],
            "failure_timestamp": pd.to_datetime(["2026-01-01 12:00:00+00:00"]),
            "failure_component": ["Forming Die"],
        }
    )

    result = add_failure_labels(readings, failures)

    assert result["failure_within_60m"].tolist() == [0, 1, 1]


def test_downtime_readings_are_removed():
    readings = pd.DataFrame(
        {
            "machine_id": [1, 1, 1],
            "reading_timestamp": pd.to_datetime(
                [
                    "2026-01-01 12:00:00+00:00",
                    "2026-01-01 12:05:00+00:00",
                    "2026-01-01 12:10:00+00:00",
                ]
            ),
        }
    )
    downtime = pd.DataFrame(
        {
            "machine_id": [1],
            "downtime_start": pd.to_datetime(["2026-01-01 12:04:00+00:00"]),
            "downtime_end": pd.to_datetime(["2026-01-01 12:06:00+00:00"]),
        }
    )

    result = remove_downtime_readings(readings, downtime)

    assert len(result) == 2
    assert pd.Timestamp("2026-01-01 12:05:00+00:00") not in set(
        result["reading_timestamp"]
    )


def test_time_split_keeps_future_rows_out_of_training_data():
    dataset = pd.DataFrame(
        {
            "reading_timestamp": pd.to_datetime(
                ["2026-04-30 23:55:00+00:00", "2026-05-01 00:00:00+00:00"]
            )
        }
    )

    train, test = time_based_split(dataset, "2026-05-01 00:00:00")

    assert len(train) == 1
    assert len(test) == 1
