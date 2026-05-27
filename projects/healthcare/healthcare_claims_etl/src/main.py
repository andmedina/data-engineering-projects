"""Main entry point for healthcare claims ETL pipeline."""

import pandas as pd

from extract import extract_raw_data
from load import load_processed_data
from transform import (
    transform_claims,
    transform_patients,
    transform_providers
)
from validate import validate_claims_data


PROCESSED_DATA_DIR = "data/processed"


def save_processed_data(
    patients: pd.DataFrame,
    providers: pd.DataFrame,
    claims: pd.DataFrame
) -> None:
    """Save processed datasets as CSV files."""
    patients.to_csv(
        f"{PROCESSED_DATA_DIR}/patients_processed.csv",
        index=False
    )

    providers.to_csv(
        f"{PROCESSED_DATA_DIR}/providers_processed.csv",
        index=False
    )

    claims.to_csv(
        f"{PROCESSED_DATA_DIR}/claims_processed.csv",
        index=False
    )


def main() -> None:
    """Run the healthcare claims ETL workflow."""
    patients, providers, claims = extract_raw_data()

    validate_claims_data(
        claims=claims,
        patients=patients,
        providers=providers
    )

    transformed_patients = transform_patients(patients)
    transformed_providers = transform_providers(providers)
    transformed_claims = transform_claims(claims)

    save_processed_data(
        patients=transformed_patients,
        providers=transformed_providers,
        claims=transformed_claims
    )

    load_processed_data(
        patients=transformed_patients,
        providers=transformed_providers,
        claims=transformed_claims
    )

    print("Healthcare claims ETL pipeline completed successfully.")


if __name__ == "__main__":
    main()
