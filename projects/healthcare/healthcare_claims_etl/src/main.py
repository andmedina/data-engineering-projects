"""Main entry point for healthcare claims ETL pipeline."""

import pandas as pd

from validate import validate_claims_data


RAW_DATA_DIR = "data/raw"


def load_raw_data():
    """Load raw CSV files into pandas DataFrames."""
    patients = pd.read_csv(f"{RAW_DATA_DIR}/patients.csv")
    providers = pd.read_csv(f"{RAW_DATA_DIR}/providers.csv")
    claims = pd.read_csv(f"{RAW_DATA_DIR}/claims.csv")

    return patients, providers, claims


def main():
    """Run the healthcare claims ETL validation workflow."""
    patients, providers, claims = load_raw_data()

    validate_claims_data(
        claims=claims,
        patients=patients,
        providers=providers
    )

    print("Healthcare claims data validation completed successfully.")


if __name__ == "__main__":
    main()
