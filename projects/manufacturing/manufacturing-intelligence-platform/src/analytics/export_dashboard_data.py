"""Export analytics datasets as dashboard-ready CSV files."""

import csv
from pathlib import Path

from .analysis import (
    get_defect_analysis,
    get_downtime_causes,
    get_monthly_trends,
    get_product_family_kpis,
)
from .kpis import get_engine, get_kpi_summary, get_machine_kpis


DEFAULT_OUTPUT_DIRECTORY = Path(__file__).resolve().parents[2] / "outputs" / "analytics"


def write_csv(file_path, rows):
    """Write a list of dictionaries to a CSV file."""
    if not rows:
        return False

    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return True


def export_dashboard_data(engine, output_directory=DEFAULT_OUTPUT_DIRECTORY):
    """Export each analytics dataset and return the files created."""
    datasets = {
        "plant_kpi_summary.csv": [get_kpi_summary(engine)],
        "machine_kpis.csv": get_machine_kpis(engine),
        "product_family_kpis.csv": get_product_family_kpis(engine),
        "quality_defects.csv": get_defect_analysis(engine),
        "downtime_causes.csv": get_downtime_causes(engine),
        "monthly_kpi_trends.csv": get_monthly_trends(engine),
    }

    created_files = []
    for file_name, rows in datasets.items():
        file_path = output_directory / file_name
        if write_csv(file_path, rows):
            created_files.append(file_path)

    return created_files


def main():
    """Export all dashboard datasets and display their paths."""
    created_files = export_dashboard_data(get_engine())
    print("\nDASHBOARD EXPORTS")
    print("=" * 30)
    for file_path in created_files:
        print(file_path)


if __name__ == "__main__":
    main()
