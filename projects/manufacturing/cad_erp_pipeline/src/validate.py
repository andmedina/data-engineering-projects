"""Validation checks for engineering and manufacturing source data."""

import pandas as pd


VALID_ENGINEERING_STATUSES: set[str] = {"released", "in_review", "obsolete"}


def validate_required_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    dataset_name: str,
) -> None:
    """Validate that a dataset contains all required columns."""
    missing_columns: set[str] = required_columns - set(dataframe.columns)

    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: {missing_columns}"
        )


def validate_no_missing_values(
    dataframe: pd.DataFrame,
    columns: list[str],
    dataset_name: str,
) -> None:
    """Validate that selected columns do not contain missing values."""
    missing_counts_by_column: dict[str, int] = {
        column: int(dataframe[column].isna().sum())
        for column in columns
    }

    filtered_missing_counts: dict[str, int] = {
        column: count
        for column, count in missing_counts_by_column.items()
        if count > 0
    }

    if filtered_missing_counts:
        raise ValueError(
            f"{dataset_name} contains missing values: "
            f"{filtered_missing_counts}"
        )

def validate_parts_data(parts_df: pd.DataFrame) -> None:
    """Validate CAD part metadata."""
    required_columns: set[str] = {
        "part_number",
        "part_name",
        "revision",
        "material",
        "weight_kg",
        "cad_system",
        "engineering_status",
        "supplier_id",
    }

    validate_required_columns(parts_df, required_columns, "parts")
    validate_no_missing_values(parts_df, list(required_columns), "parts")

    invalid_statuses: pd.Series = ~parts_df["engineering_status"].isin(
        VALID_ENGINEERING_STATUSES
    )

    if invalid_statuses.any():
        raise ValueError("parts contains invalid engineering_status values")

    if parts_df["part_number"].duplicated().any():
        raise ValueError("parts contains duplicate part_number values")

    if (parts_df["weight_kg"] <= 0).any():
        raise ValueError("parts contains non-positive weight_kg values")


def validate_inventory_data(inventory_df: pd.DataFrame) -> None:
    """Validate inventory data."""
    required_columns: set[str] = {
        "part_number",
        "stock_quantity",
        "reorder_level",
        "warehouse_location",
        "last_updated",
    }

    validate_required_columns(inventory_df, required_columns, "inventory")
    validate_no_missing_values(inventory_df, list(required_columns), "inventory")

    if (inventory_df["stock_quantity"] < 0).any():
        raise ValueError("inventory contains negative stock_quantity values")

    if (inventory_df["reorder_level"] < 0).any():
        raise ValueError("inventory contains negative reorder_level values")


def validate_supplier_relationships(
    parts_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
) -> None:
    """Validate that every part supplier exists in supplier data."""
    missing_suppliers: set[str] = set(parts_df["supplier_id"]) - set(
        suppliers_df["supplier_id"]
    )

    if missing_suppliers:
        raise ValueError(
            f"parts references missing supplier_id values: {missing_suppliers}"
        )
