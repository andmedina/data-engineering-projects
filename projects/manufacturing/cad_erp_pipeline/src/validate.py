"""Validation checks for engineering and manufacturing raw data."""

from collections.abc import Callable
from typing import Any

import pandas as pd


VALID_ENGINEERING_STATUSES: set[str] = {"released", "in_review", "obsolete"}

PART_COLUMNS = {
    "part_number", "part_name", "revision", "material", "weight_kg",
    "cad_system", "engineering_status", "supplier_id",
}
SUPPLIER_COLUMNS = {
    "supplier_id", "supplier_name", "country", "supplier_type",
}
INVENTORY_COLUMNS = {
    "part_number", "stock_quantity", "reorder_level",
    "warehouse_location", "last_updated",
}
BOM_COLUMNS = {
    "assembly_id", "assembly_name", "assembly_revision",
    "part_number", "quantity",
}
ASSEMBLY_COLUMNS = {"assembly_id", "assembly_name", "revision"}


def validate_required_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    dataset_name: str,
) -> None:
    """Validate that a dataset contains all required columns."""
    missing_columns = required_columns - set(dataframe.columns)
    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            f"{sorted(missing_columns)}"
        )


def validate_no_missing_values(
    dataframe: pd.DataFrame,
    columns: set[str],
    dataset_name: str,
) -> None:
    """Validate that required columns do not contain missing values."""
    missing_counts = {
        column: int(dataframe[column].isna().sum())
        for column in sorted(columns)
        if dataframe[column].isna().any()
    }
    if missing_counts:
        raise ValueError(
            f"{dataset_name} contains missing values: {missing_counts}"
        )


def validate_unique_identifier(
    dataframe: pd.DataFrame,
    columns: list[str],
    dataset_name: str,
) -> None:
    """Validate that identifier columns uniquely identify each row."""
    if dataframe.duplicated(subset=columns).any():
        identifier = ", ".join(columns)
        raise ValueError(f"{dataset_name} contains duplicate {identifier} values")


def validate_parts_data(parts_df: pd.DataFrame) -> None:
    """Validate CAD part metadata."""
    validate_required_columns(parts_df, PART_COLUMNS, "parts")
    validate_no_missing_values(parts_df, PART_COLUMNS, "parts")
    validate_unique_identifier(parts_df, ["part_number"], "parts")

    if (~parts_df["engineering_status"].isin(VALID_ENGINEERING_STATUSES)).any():
        raise ValueError("parts contains invalid engineering_status values")
    if (parts_df["weight_kg"] <= 0).any():
        raise ValueError("parts contains non-positive weight_kg values")


def validate_suppliers_data(suppliers_df: pd.DataFrame) -> None:
    """Validate supplier master data."""
    validate_required_columns(suppliers_df, SUPPLIER_COLUMNS, "suppliers")
    validate_no_missing_values(suppliers_df, SUPPLIER_COLUMNS, "suppliers")
    validate_unique_identifier(suppliers_df, ["supplier_id"], "suppliers")


def validate_inventory_data(inventory_df: pd.DataFrame) -> None:
    """Validate inventory data."""
    validate_required_columns(inventory_df, INVENTORY_COLUMNS, "inventory")
    validate_no_missing_values(inventory_df, INVENTORY_COLUMNS, "inventory")
    validate_unique_identifier(inventory_df, ["part_number"], "inventory")

    if (inventory_df[["stock_quantity", "reorder_level"]] < 0).any().any():
        raise ValueError("inventory contains negative quantity values")


def validate_assemblies_data(assemblies_df: pd.DataFrame) -> None:
    """Validate scaled assembly metadata."""
    validate_required_columns(assemblies_df, ASSEMBLY_COLUMNS, "assemblies")
    validate_no_missing_values(assemblies_df, ASSEMBLY_COLUMNS, "assemblies")
    validate_unique_identifier(assemblies_df, ["assembly_id"], "assemblies")


def validate_bom_data(bom_df: pd.DataFrame) -> None:
    """Validate relational BOM rows and positive component quantities."""
    validate_required_columns(bom_df, BOM_COLUMNS, "bom")
    validate_no_missing_values(bom_df, BOM_COLUMNS, "bom")
    validate_unique_identifier(
        bom_df, ["assembly_id", "part_number"], "bom"
    )
    if (bom_df["quantity"] <= 0).any():
        raise ValueError("bom contains non-positive quantity values")


def validate_references(
    values: pd.Series,
    valid_values: pd.Series,
    relationship_name: str,
) -> None:
    """Validate a foreign-key-like relationship between DataFrames."""
    missing_values = set(values) - set(valid_values)
    if missing_values:
        raise ValueError(
            f"{relationship_name} references missing values: "
            f"{sorted(missing_values)}"
        )


def validate_supplier_relationships(
    parts_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
) -> None:
    """Validate that every part supplier exists in supplier data."""
    validate_references(
        parts_df["supplier_id"], suppliers_df["supplier_id"],
        "parts.supplier_id",
    )


def validate_bom_part_relationships(
    bom_df: pd.DataFrame,
    parts_df: pd.DataFrame,
) -> None:
    """Validate that every BOM component exists in part data."""
    validate_references(
        bom_df["part_number"], parts_df["part_number"], "bom.part_number"
    )


def validate_inventory_part_relationships(
    inventory_df: pd.DataFrame,
    parts_df: pd.DataFrame,
) -> None:
    """Validate that every inventory record references an existing part."""
    validate_references(
        inventory_df["part_number"],
        parts_df["part_number"],
        "inventory.part_number",
    )


def run_validation_check(
    name: str,
    check: Callable[..., None],
    *args: Any,
) -> dict[str, object]:
    """Run one validation and capture its pass/fail result."""
    try:
        check(*args)
    except (KeyError, TypeError, ValueError) as error:
        return {"check": name, "passed": False, "message": str(error)}
    return {"check": name, "passed": True, "message": "Passed"}


def validate_all_sources(
    dataframes: dict[str, pd.DataFrame],
) -> list[dict[str, object]]:
    """Run all raw-layer validations and return auditable results."""
    checks = [
        ("parts", validate_parts_data, dataframes["parts"]),
        ("suppliers", validate_suppliers_data, dataframes["suppliers"]),
        ("inventory", validate_inventory_data, dataframes["inventory"]),
        ("assemblies", validate_assemblies_data, dataframes["assemblies"]),
        ("bom", validate_bom_data, dataframes["bom"]),
        (
            "supplier references", validate_supplier_relationships,
            dataframes["parts"], dataframes["suppliers"],
        ),
        (
            "bom part references", validate_bom_part_relationships,
            dataframes["bom"], dataframes["parts"],
        ),
        (
            "inventory part references", validate_inventory_part_relationships,
            dataframes["inventory"], dataframes["parts"],
        ),
    ]
    return [
        run_validation_check(name, check, *args)
        for name, check, *args in checks
    ]


def raise_for_validation_failures(
    validation_results: list[dict[str, object]],
) -> None:
    """Stop the pipeline when one or more validation checks failed."""
    failures = [
        str(result["message"])
        for result in validation_results
        if not result["passed"]
    ]
    if failures:
        raise ValueError("Validation failed: " + "; ".join(failures))
