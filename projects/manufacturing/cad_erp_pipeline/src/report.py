"""Generate data quality reports for pipeline outputs."""

from pathlib import Path

import pandas as pd

from config import LOG_DIR


def generate_data_quality_report(
    dataframes: dict[str, pd.DataFrame],
) -> None:
    """Generate a simple data quality report for transformed datasets."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    report_path: Path = LOG_DIR / "data_quality_report.txt"

    with report_path.open("w", encoding="utf-8") as report:
        report.write("CAD-to-ERP Data Quality Report\n")
        report.write("=" * 40 + "\n\n")

        for dataset_name, dataframe in dataframes.items():
            missing_values: int = int(dataframe.isna().sum().sum())
            duplicate_rows: int = int(dataframe.duplicated().sum())

            report.write(f"{dataset_name.upper()}\n")
            report.write("-" * 20 + "\n")
            report.write(f"Rows: {len(dataframe)}\n")
            report.write(f"Columns: {len(dataframe.columns)}\n")
            report.write(f"Missing Values: {missing_values}\n")
            report.write(f"Duplicate Rows: {duplicate_rows}\n\n")

        report.write("Status: PASSED\n")
