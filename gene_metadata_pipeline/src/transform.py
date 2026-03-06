"""
Transform raw Ensembl gene metadata into a clean dataframe.
"""

from __future__ import annotations

import pandas as pd


def transform_gene_data(raw_data: list[dict]) -> pd.DataFrame:
    """
    Convert raw Ensembl API responses into a structured dataframe.

    Parameters
    ----------
    raw_data : list[dict]
        Raw JSON responses from the Ensembl API.

    Returns
    -------
    pd.DataFrame
        Clean dataframe containing gene metadata.
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