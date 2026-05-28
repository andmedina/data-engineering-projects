"""Run the CAD-to-ERP engineering data pipeline."""

from extract import extract_all_sources
from load import load_dataframes_to_postgres, save_processed_data
from transform import transform_all_sources
from validate import (
    validate_inventory_data,
    validate_parts_data,
    validate_supplier_relationships,
)


def main() -> None:
    """Execute the full ETL pipeline."""
    source_dataframes = extract_all_sources()

    validate_parts_data(source_dataframes["parts"])
    validate_inventory_data(source_dataframes["inventory"])

    validate_supplier_relationships(
        source_dataframes["parts"],
        source_dataframes["suppliers"],
    )

    transformed_dataframes = transform_all_sources(source_dataframes)

    save_processed_data(transformed_dataframes)
    load_dataframes_to_postgres(transformed_dataframes)

    print("CAD-to-ERP pipeline executed successfully.")


if __name__ == "__main__":
    main()
