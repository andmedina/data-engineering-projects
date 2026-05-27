"""Transformation functions for healthcare claims ETL pipeline."""

import pandas as pd


def transform_claims(
    claims: pd.DataFrame
) -> pd.DataFrame:
    """Transform raw claims data into analytics-ready format."""
    transformed_claims = claims.copy()

    transformed_claims["claim_date"] = pd.to_datetime(
        transformed_claims["claim_date"],
        errors="coerce"
    )

    transformed_claims["claim_year"] = transformed_claims[
        "claim_date"
    ].dt.year

    transformed_claims["claim_month"] = transformed_claims[
        "claim_date"
    ].dt.month

    transformed_claims["claim_amount"] = transformed_claims[
        "claim_amount"
    ].round(2)

    transformed_claims["insurance_plan"] = transformed_claims[
        "insurance_plan"
    ].str.strip()

    transformed_claims["claim_status"] = transformed_claims[
        "claim_status"
    ].str.strip()

    return transformed_claims


def transform_patients(
    patients: pd.DataFrame
) -> pd.DataFrame:
    """Transform raw patient data."""
    transformed_patients = patients.copy()

    transformed_patients["gender"] = transformed_patients[
        "gender"
    ].str.strip()

    transformed_patients["state"] = transformed_patients[
        "state"
    ].str.upper()

    return transformed_patients


def transform_providers(
    providers: pd.DataFrame
) -> pd.DataFrame:
    """Transform raw provider data."""
    transformed_providers = providers.copy()

    transformed_providers["provider_name"] = transformed_providers[
        "provider_name"
    ].str.strip()

    transformed_providers["specialty"] = transformed_providers[
        "specialty"
    ].str.strip()

    transformed_providers["state"] = transformed_providers[
        "state"
    ].str.upper()

    return transformed_providers
