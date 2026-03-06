#!/usr/bin/env python3
"""
Gene Metadata ETL Pipeline.

Run the full pipeline:
- Extract gene metadata from Ensembl
- Transform raw JSON into a clean dataframe
- Load results into SQLite
"""

from __future__ import annotations

from src.extract import extract_all_genes
from src.transform import transform_gene_data
from src.load import load_to_sqlite


def main() -> None:
    """Run the ETL pipeline."""
    raw_data = extract_all_genes()
    transformed_df = transform_gene_data(raw_data)
    load_to_sqlite(transformed_df)

    print("Pipeline completed successfully.")
    print(transformed_df)


if __name__ == "__main__":
    main()
