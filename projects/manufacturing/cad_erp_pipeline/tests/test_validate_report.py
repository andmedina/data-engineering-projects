"""Tests for validation rules and quality reporting."""

# pylint: disable=missing-function-docstring
# pylint: disable=too-many-arguments,too-many-positional-arguments

import pytest

from src import report
from src.validate import (
    raise_for_validation_failures,
    validate_all_sources,
    validate_bom_data,
    validate_inventory_data,
    validate_parts_data,
)


def test_all_validations_pass(valid_dataframes):
    results = validate_all_sources(valid_dataframes)
    assert all(result["passed"] for result in results)


@pytest.mark.parametrize(
    ("dataset", "column", "value", "validator", "message"),
    [
        ("parts", "weight_kg", 0, validate_parts_data, "non-positive"),
        ("inventory", "stock_quantity", -1, validate_inventory_data, "negative"),
        ("bom", "quantity", 0, validate_bom_data, "non-positive"),
    ],
)
def test_numeric_validation_failures(
    valid_dataframes, dataset, column, value, validator, message
):
    invalid = valid_dataframes[dataset].copy()
    invalid.loc[0, column] = value
    with pytest.raises(ValueError, match=message):
        validator(invalid)


def test_missing_column_and_duplicate_identifier_fail(seed_parts):
    with pytest.raises(ValueError, match="missing required columns"):
        validate_parts_data(seed_parts.drop(columns=["material"]))

    duplicated = seed_parts.iloc[[0, 0]].copy()
    with pytest.raises(ValueError, match="duplicate part_number"):
        validate_parts_data(duplicated)


@pytest.mark.parametrize(
    ("dataset", "column", "bad_value", "check_name"),
    [
        ("parts", "supplier_id", "SUP-999", "supplier references"),
        ("bom", "part_number", "P-999", "bom part references"),
        ("inventory", "part_number", "P-999", "inventory part references"),
    ],
)
def test_invalid_references_are_reported(
    valid_dataframes, dataset, column, bad_value, check_name
):
    valid_dataframes[dataset].loc[0, column] = bad_value
    results = validate_all_sources(valid_dataframes)
    result = next(item for item in results if item["check"] == check_name)
    assert result["passed"] is False
    with pytest.raises(ValueError, match="Validation failed"):
        raise_for_validation_failures(results)


def test_quality_report_reflects_failed_validation(
    tmp_path, monkeypatch, valid_dataframes
):
    monkeypatch.setattr(report, "LOG_DIR", tmp_path)
    results = [{"check": "parts", "passed": False, "message": "bad weight"}]

    report.generate_data_quality_report(valid_dataframes, results)
    contents = (tmp_path / "data_quality_report.txt").read_text()

    assert "parts: FAILED - bad weight" in contents
    assert "Status: FAILED" in contents
