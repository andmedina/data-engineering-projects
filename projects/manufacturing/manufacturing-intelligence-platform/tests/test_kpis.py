from src.analytics.kpis import calculate_percentage, format_percentage
from src.analytics.analysis import add_production_rates
from src.analytics.export_dashboard_data import write_csv


def test_calculate_percentage_returns_expected_rate():
    assert calculate_percentage(950, 1000) == 95.0


def test_calculate_percentage_rounds_to_two_decimals():
    assert calculate_percentage(2, 3) == 66.67


def test_calculate_percentage_handles_zero_denominator():
    assert calculate_percentage(0, 0) is None


def test_format_percentage_handles_available_and_missing_values():
    assert format_percentage(98.84) == "98.84%"
    assert format_percentage(None) == "N/A"


def test_add_production_rates_calculates_all_three_rates():
    rows = [
        {
            "input_quantity": 1000,
            "good_quantity": 950,
            "scrap_quantity": 20,
            "rework_quantity": 30,
        }
    ]

    result = add_production_rates(rows)

    assert result[0]["first_pass_yield_pct"] == 95.0
    assert result[0]["scrap_rate_pct"] == 2.0
    assert result[0]["rework_rate_pct"] == 3.0


def test_write_csv_creates_dashboard_file(tmp_path):
    file_path = tmp_path / "machine_kpis.csv"

    created = write_csv(
        file_path,
        [{"machine_code": "CH-01", "first_pass_yield_pct": 98.5}],
    )

    assert created is True
    assert file_path.read_text(encoding="utf-8") == (
        "machine_code,first_pass_yield_pct\nCH-01,98.5\n"
    )


def test_write_csv_skips_empty_dataset(tmp_path):
    file_path = tmp_path / "empty.csv"

    assert write_csv(file_path, []) is False
    assert not file_path.exists()
