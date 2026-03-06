#!/usr/bin/env python3
"""
Gene Metadata ETL Pipeline.

Run the full pipeline:
- Extract gene metadata from Ensembl
- Save raw JSON
- Transform raw JSON into a clean dataframe
- Save processed CSV
- Load results into SQLite
- Log pipeline progress
"""

from __future__ import annotations

import logging
from pathlib import Path

from src.config import LOG_FILE
from src.extract import extract_all_genes, save_raw_data
from src.transform import transform_gene_data, save_processed_data
from src.load import load_to_sqlite


def setup_logger(log_file: str) -> logging.Logger:
    """
    Configure pipeline logger.
    """
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("gene_metadata_pipeline")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def main() -> None:
    """Run the ETL pipeline."""
    logger = setup_logger(LOG_FILE)

    try:
        logger.info("ETL pipeline started")

        logger.info("Extract phase started")
        raw_data = extract_all_genes()
        save_raw_data(raw_data)
        logger.info("Extract phase completed")

        logger.info("Transform phase started")
        transformed_df = transform_gene_data(raw_data)
        save_processed_data(transformed_df)
        logger.info("Transform phase completed")

        logger.info("Load phase started")
        load_to_sqlite(transformed_df)
        logger.info("Load phase completed")

        logger.info("ETL pipeline completed successfully")
        print(transformed_df)

    except Exception as exc:
        logger.exception("ETL pipeline failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
