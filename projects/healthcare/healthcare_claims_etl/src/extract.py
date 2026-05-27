"""Extraction functions for healthcare claims ETL pipeline."""

import pandas as pd


RAW_DATA_DIR = "data/raw"


def extract_raw_data() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame
]:
    """Extract raw healthcare CSV files into pandas DataFrames."""
    patients = pd.read_csv(
        f"{RAW_DATA_DIR}/patients.csv"
    )

    providers = pd.read_csv(
        f"{RAW_DATA_DIR}/providers.csv"
    )

    claims = pd.read_csv(
        f"{RAW_DATA_DIR}/claims.csv"
    )

    return patients, providers, claims
