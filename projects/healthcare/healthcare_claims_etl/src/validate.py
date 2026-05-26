"""Validation functions for healthcare claims ETL pipeline."""

VALID_CLAIM_STATUSES = [
    "Approved",
    "Denied",
    "Pending"
]


def validate_required_columns(dataframe, required_columns, dataset_name):
    """Validate that all required columns exist."""
    missing_columns = [
        column for column in required_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            f"{missing_columns}"
        )


def validate_no_nulls(dataframe, columns, dataset_name):
    """Validate that selected columns do not contain null values."""
    for column in columns:
        null_count = dataframe[column].isnull().sum()

        if null_count > 0:
            raise ValueError(
                f"{dataset_name}.{column} contains "
                f"{null_count} null values"
            )


def validate_claim_amounts(claims):
    """Validate claim amounts are positive."""
    invalid_count = (claims["claim_amount"] <= 0).sum()

    if invalid_count > 0:
        raise ValueError(
            f"claims.claim_amount contains "
            f"{invalid_count} invalid values"
        )


def validate_claim_statuses(claims):
    """Validate claim statuses are within the accepted set."""
    invalid_statuses = claims[
        ~claims["claim_status"].isin(VALID_CLAIM_STATUSES)
    ]

    if not invalid_statuses.empty:
        raise ValueError(
            "claims.claim_status contains invalid values"
        )


def validate_foreign_keys(claims, patients, providers):
    """Validate that claims reference valid patients and providers."""
    invalid_patients = claims[
        ~claims["patient_id"].isin(patients["patient_id"])
    ]

    invalid_providers = claims[
        ~claims["provider_id"].isin(providers["provider_id"])
    ]

    if not invalid_patients.empty:
        raise ValueError(
            f"claims contains {len(invalid_patients)} "
            "invalid patient_id references"
        )

    if not invalid_providers.empty:
        raise ValueError(
            f"claims contains {len(invalid_providers)} "
            "invalid provider_id references"
        )


def validate_claims_data(claims, patients, providers):
    """Run all validation checks for claims data."""
    validate_required_columns(
        claims,
        [
            "claim_id",
            "patient_id",
            "provider_id",
            "diagnosis_code",
            "procedure_code",
            "claim_date",
            "claim_amount",
            "insurance_plan",
            "claim_status"
        ],
        "claims"
    )

    validate_no_nulls(
        claims,
        [
            "claim_id",
            "patient_id",
            "provider_id",
            "claim_date",
            "claim_amount"
        ],
        "claims"
    )

    validate_claim_amounts(claims)
    validate_claim_statuses(claims)
    validate_foreign_keys(claims, patients, providers)
