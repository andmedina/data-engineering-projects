"""Transform engineering and manufacturing data into analytics-ready tables."""

import pandas as pd


def transform_parts(parts_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize CAD part metadata."""
    transformed_df: pd.DataFrame = parts_df.copy()

    transformed_df["part_number"] = transformed_df["part_number"].str.upper()
    transformed_df["revision"] = transformed_df["revision"].str.upper()
    transformed_df["engineering_status"] = (
        transformed_df["engineering_status"].str.lower()
    )
    transformed_df["material"] = transformed_df["material"].str.strip()

    return transformed_df


def transform_inventory(inventory_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize inventory data."""
    transformed_df: pd.DataFrame = inventory_df.copy()

    transformed_df["part_number"] = transformed_df["part_number"].str.upper()
    transformed_df["warehouse_location"] = (
        transformed_df["warehouse_location"].str.upper()
    )
    transformed_df["last_updated"] = pd.to_datetime(
        transformed_df["last_updated"]
    )

    transformed_df["below_reorder_level"] = (
        transformed_df["stock_quantity"] < transformed_df["reorder_level"]
    )

    return transformed_df


def transform_suppliers(suppliers_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize supplier data."""
    transformed_df: pd.DataFrame = suppliers_df.copy()

    transformed_df["supplier_id"] = transformed_df["supplier_id"].str.upper()
    transformed_df["country"] = transformed_df["country"].str.upper()
    transformed_df["supplier_type"] = transformed_df["supplier_type"].str.lower()

    return transformed_df


def flatten_assembly_bom(assemblies_df: pd.DataFrame) -> pd.DataFrame:
    """Flatten nested assembly BOM data into relational rows."""
    bom_rows: list[dict[str, object]] = []

    for _, assembly in assemblies_df.iterrows():
        for part in assembly["parts"]:
            bom_rows.append(
                {
                    "assembly_id": assembly["assembly_id"],
                    "assembly_name": assembly["assembly_name"],
                    "assembly_revision": assembly["revision"],
                    "part_number": part["part_number"],
                    "quantity": part["quantity"],
                }
            )

    return pd.DataFrame(bom_rows)


def transform_all_sources(
    source_dataframes: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Transform all extracted source datasets."""
    parts_df: pd.DataFrame = transform_parts(source_dataframes["parts"])
    suppliers_df: pd.DataFrame = transform_suppliers(
        source_dataframes["suppliers"]
    )
    inventory_df: pd.DataFrame = transform_inventory(
        source_dataframes["inventory"]
    )
    bom_df: pd.DataFrame = flatten_assembly_bom(
        source_dataframes["assemblies"]
    )

    return {
        "parts": parts_df,
        "suppliers": suppliers_df,
        "inventory": inventory_df,
        "bom": bom_df,
    }
