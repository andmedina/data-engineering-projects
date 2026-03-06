"""
Transform raw Ensembl gene metadata into a clean dataframe.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import PROCESSED_CSV_PATH


def transform_gene_data(raw_data: list[dict]) -> pd.DataFrame:
    """
    Convert raw Ensembl API responses into a structured dataframe.
    """
    records = []

    for gene in raw_data:
        records.append(
            {
                "gene_symbol": gene.get("display_name"),
                "ensembl_id": gene.get("id"),
                "chromosome": gene.get("seq_region_name"),
                "start_position": gene.get("start"),
                "end_position": gene.get("end"),
                "strand": gene.get("strand"),
                "description": gene.get("description"),
                "species": gene.get("species"),
            }
        )

    dataframe = pd.DataFrame(records)
    return dataframe


def save_processed_data(
    dataframe: pd.DataFrame,
    output_path: str = PROCESSED_CSV_PATH,
) -> None:
    """
    Save transformed data to CSV.
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_file, index=False)