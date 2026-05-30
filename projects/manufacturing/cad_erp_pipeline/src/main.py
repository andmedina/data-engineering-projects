"""Run the CAD-to-ERP engineering data pipeline with observability."""

import time

from extract import extract_all_sources
from load import load_dataframes_to_postgres, save_processed_data
from report import generate_data_quality_report
from transform import transform_all_sources
from validate import (
    validate_inventory_data,
    validate_parts_data,
    validate_supplier_relationships,
)


def log_step(step: str) -> None:
    """Print a formatted pipeline stage header."""
    print(f"\n[STEP] {step}")


def log_rows(label: str, df) -> None:
    """Log the number of rows in a dataframe."""
    print(f"{label}: {len(df)} rows")


def main() -> None:
    """
    Execute the full CAD-to-ERP ETL pipeline.

    Steps:
    1. Extract source data
    2. Validate engineering and inventory data
    3. Transform into analytics-ready structures
    4. Save processed CSV outputs
    5. Load data into PostgreSQL
    6. Print execution summary with runtime metrics
    """
    start_time = time.time()

    log_step("EXTRACT")
    source_dataframes = extract_all_sources()

    log_rows("parts (source)", source_dataframes["parts"])
    log_rows("assemblies (source)", source_dataframes["assemblies"])
    log_rows("suppliers (source)", source_dataframes["suppliers"])
    log_rows("inventory (source)", source_dataframes["inventory"])

    log_step("VALIDATION")

    validate_parts_data(source_dataframes["parts"])
    validate_inventory_data(source_dataframes["inventory"])
    validate_supplier_relationships(
        source_dataframes["parts"],
        source_dataframes["suppliers"],
    )

    print("Validation checks passed")

    log_step("TRANSFORMATION")
    transformed_dataframes = transform_all_sources(source_dataframes)

    log_rows("parts (processed)", transformed_dataframes["parts"])
    log_rows("suppliers (processed)", transformed_dataframes["suppliers"])
    log_rows("inventory (processed)", transformed_dataframes["inventory"])
    log_rows("bom (processed)", transformed_dataframes["bom"])

    log_step("SAVE TO CSV")
    save_processed_data(transformed_dataframes)

    log_step("DATA QUALITY REPORT")
    generate_data_quality_report(transformed_dataframes)
    print("Data quality report saved to logs/data_quality_report.txt")

    log_step("LOAD TO POSTGRESQL")
    load_dataframes_to_postgres(transformed_dataframes)

    end_time = time.time()

    print("\n================ PIPELINE SUMMARY ================")
    print(f"Total runtime: {round(end_time - start_time, 2)} seconds")
    print("Status: SUCCESS")
    print("==================================================\n")


if __name__ == "__main__":
    main()
