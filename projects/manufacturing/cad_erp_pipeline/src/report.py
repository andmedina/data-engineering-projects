"""Generate data quality reports for pipeline outputs."""

from pathlib import Path

import pandas as pd

from .config import LOG_DIR


def generate_data_quality_report(
    dataframes: dict[str, pd.DataFrame],
    validation_results: list[dict[str, object]],
) -> None:
    """Generate a quality report whose status reflects validation results."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    report_path: Path = LOG_DIR / "data_quality_report.txt"

    total_rows: int = 0
    total_missing_values: int = 0
    total_duplicate_rows: int = 0

    with report_path.open("w", encoding="utf-8") as report:
        report.write("CAD-to-ERP Data Quality Report\n")
        report.write("=" * 40 + "\n\n")

        for dataset_name, dataframe in dataframes.items():
            row_count: int = len(dataframe)
            missing_values: int = int(dataframe.isna().sum().sum())
            duplicate_rows: int = int(dataframe.duplicated().sum())

            total_rows += row_count
            total_missing_values += missing_values
            total_duplicate_rows += duplicate_rows

            report.write(f"{dataset_name.upper()}\n")
            report.write("-" * 20 + "\n")
            report.write(f"Rows: {row_count}\n")
            report.write(f"Columns: {len(dataframe.columns)}\n")
            report.write(f"Missing Values: {missing_values}\n")
            report.write(f"Duplicate Rows: {duplicate_rows}\n\n")

        report.write("Overall Summary\n")
        report.write("-" * 20 + "\n")
        report.write(f"Datasets Evaluated: {len(dataframes)}\n")
        report.write(f"Total Rows Processed: {total_rows}\n")
        report.write(f"Total Missing Values: {total_missing_values}\n")
        report.write(f"Total Duplicate Rows: {total_duplicate_rows}\n\n")
        report.write("Validation Checks\n")
        report.write("-" * 20 + "\n")
        for result in validation_results:
            status = "PASSED" if result["passed"] else "FAILED"
            report.write(f"{result['check']}: {status}")
            if not result["passed"]:
                report.write(f" - {result['message']}")
            report.write("\n")

        all_passed = all(result["passed"] for result in validation_results)
        report.write(f"\nStatus: {'PASSED' if all_passed else 'FAILED'}\n")
