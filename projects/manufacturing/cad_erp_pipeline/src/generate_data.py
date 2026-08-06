"""Generate larger synthetic raw datasets from source engineering exports."""

import json
import random
from pathlib import Path
from typing import Any

import pandas as pd

from .config import (
    ASSEMBLIES_RAW_PATH,
    BOM_RAW_PATH,
    INVENTORY_RAW_PATH,
    PARTS_RAW_PATH,
    RAW_DATA_DIR,
    SOURCE_DATA_DIR,
    SUPPLIERS_RAW_PATH,
)

DEFAULT_RANDOM_SEED = 42


def load_seed_json(file_path: Path) -> pd.DataFrame:
    """Load a JSON seed export into a DataFrame."""
    with file_path.open("r", encoding="utf-8") as file:
        return pd.DataFrame(json.load(file))


def extract_seed_data() -> dict[str, pd.DataFrame]:
    """Read the small source exports used only for synthetic scaling."""
    return {
        "parts": load_seed_json(SOURCE_DATA_DIR / "cad_parts_export.json"),
        "suppliers": pd.read_csv(SOURCE_DATA_DIR / "suppliers_export.csv"),
    }


def generate_scaled_parts(
    seed_parts_df: pd.DataFrame,
    total_parts: int = 250,
    rng: random.Random | None = None,
) -> pd.DataFrame:
    """Generate scaled CAD-style part records from seed part metadata."""
    materials: list[str] = seed_parts_df["material"].unique().tolist()
    cad_systems: list[str] = seed_parts_df["cad_system"].unique().tolist()
    statuses: list[str] = ["released", "in_review", "obsolete"]

    records: list[dict[str, Any]] = []
    randomizer = rng or random.Random(DEFAULT_RANDOM_SEED)

    for index in range(1, total_parts + 1):
        seed_row = seed_parts_df.iloc[randomizer.randrange(len(seed_parts_df))]

        records.append(
            {
                "part_number": f"KSD-AER-{1000 + index}",
                "part_name": f"{seed_row['part_name']} Variant {index}",
                "revision": randomizer.choice(["A", "B", "C", "D"]),
                "material": randomizer.choice(materials),
                "weight_kg": round(randomizer.uniform(0.5, 35.0), 2),
                "cad_system": randomizer.choice(cad_systems),
                "engineering_status": randomizer.choice(statuses),
                "supplier_id": seed_row["supplier_id"],
            }
        )

    return pd.DataFrame(records)


def generate_scaled_inventory(
    parts_df: pd.DataFrame,
    rng: random.Random | None = None,
) -> pd.DataFrame:
    """Generate inventory records for each synthetic part."""
    records: list[dict[str, Any]] = []
    randomizer = rng or random.Random(DEFAULT_RANDOM_SEED)

    for _, part in parts_df.iterrows():
        records.append(
            {
                "part_number": part["part_number"],
                "stock_quantity": randomizer.randint(0, 150),
                "reorder_level": randomizer.randint(5, 30),
                "warehouse_location": randomizer.choice(["WH-A", "WH-B", "WH-C"]),
                "last_updated": "2026-05-01",
            }
        )

    return pd.DataFrame(records)


def generate_scaled_bom(
    parts_df: pd.DataFrame,
    total_assemblies: int = 40,
    rng: random.Random | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate assembly and BOM relationship records."""
    assemblies: list[dict[str, Any]] = []
    bom_rows: list[dict[str, Any]] = []

    part_numbers: list[str] = parts_df["part_number"].tolist()
    randomizer = rng or random.Random(DEFAULT_RANDOM_SEED)

    for index in range(1, total_assemblies + 1):
        assembly_id = f"ASM-{index:03d}"
        assemblies.append(
            {
                "assembly_id": assembly_id,
                "assembly_name": f"Aerospace Assembly {index}",
                "revision": randomizer.choice(["A", "B", "C"]),
            }
        )

        selected_parts = randomizer.sample(
            part_numbers, randomizer.randint(3, 8)
        )

        for part_number in selected_parts:
            bom_rows.append(
                {
                    "assembly_id": assembly_id,
                    "assembly_name": f"Aerospace Assembly {index}",
                    "assembly_revision": assemblies[-1]["revision"],
                    "part_number": part_number,
                    "quantity": randomizer.randint(1, 12),
                }
            )

    return pd.DataFrame(assemblies), pd.DataFrame(bom_rows)


def save_raw_scaled_data(random_seed: int = DEFAULT_RANDOM_SEED) -> None:
    """Create larger raw CSV datasets from source engineering exports."""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    source_dataframes = extract_seed_data()
    rng = random.Random(random_seed)

    parts_df = generate_scaled_parts(source_dataframes["parts"], rng=rng)
    suppliers_df = source_dataframes["suppliers"].copy()
    inventory_df = generate_scaled_inventory(parts_df, rng=rng)
    assemblies_df, bom_df = generate_scaled_bom(parts_df, rng=rng)

    parts_df.to_csv(PARTS_RAW_PATH, index=False)
    suppliers_df.to_csv(SUPPLIERS_RAW_PATH, index=False)
    inventory_df.to_csv(INVENTORY_RAW_PATH, index=False)
    assemblies_df.to_csv(ASSEMBLIES_RAW_PATH, index=False)
    bom_df.to_csv(BOM_RAW_PATH, index=False)


if __name__ == "__main__":
    save_raw_scaled_data()
    print("Scaled raw manufacturing datasets generated successfully.")
