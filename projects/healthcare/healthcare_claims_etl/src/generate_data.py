"""Generate synthetic healthcare claims datasets."""

import os
import random
from datetime import datetime, timedelta

import pandas as pd


RAW_DATA_DIR = "data/raw"

STATES = [
    "CA",
    "TX",
    "NY",
    "FL",
    "NC",
    "WA",
    "IL",
    "AZ"
]

GENDERS = [
    "Male",
    "Female"
]

SPECIALTIES = [
    "Primary Care",
    "Cardiology",
    "Oncology",
    "Orthopedics",
    "Emergency Medicine",
    "Radiology"
]

DIAGNOSIS_CODES = [
    "E11",
    "I10",
    "J45",
    "M54",
    "C50",
    "N18"
]

PROCEDURE_CODES = [
    "99213",
    "93000",
    "80053",
    "71046",
    "72148",
    "36415"
]

INSURANCE_PLANS = [
    "Medicare",
    "Medicaid",
    "Blue Cross",
    "Aetna",
    "UnitedHealthcare"
]

CLAIM_STATUSES = [
    "Approved",
    "Denied",
    "Pending"
]


def generate_patients(
    count: int = 500
) -> pd.DataFrame:
    """Generate synthetic patient records."""
    patients = []

    for index in range(1, count + 1):
        patients.append({
            "patient_id": f"P{index:05d}",
            "age": random.randint(1, 90),
            "gender": random.choice(GENDERS),
            "state": random.choice(STATES)
        })

    return pd.DataFrame(patients)


def generate_providers(
    count: int = 50
) -> pd.DataFrame:
    """Generate synthetic provider records."""
    providers = []

    for index in range(1, count + 1):
        providers.append({
            "provider_id": f"PR{index:04d}",
            "provider_name": f"Provider {index}",
            "specialty": random.choice(SPECIALTIES),
            "state": random.choice(STATES)
        })

    return pd.DataFrame(providers)


def generate_claims(
    count: int = 3000,
    patient_count: int = 500,
    provider_count: int = 50
) -> pd.DataFrame:
    """Generate synthetic healthcare claims data."""
    claims = []

    start_date = datetime(2023, 1, 1)

    for index in range(1, count + 1):
        claim_date = (
            start_date +
            timedelta(days=random.randint(0, 730))
        )

        claims.append({
            "claim_id": f"C{index:06d}",
            "patient_id": (
                f"P{random.randint(1, patient_count):05d}"
            ),
            "provider_id": (
                f"PR{random.randint(1, provider_count):04d}"
            ),
            "diagnosis_code": random.choice(
                DIAGNOSIS_CODES
            ),
            "procedure_code": random.choice(
                PROCEDURE_CODES
            ),
            "claim_date": claim_date.date(),
            "claim_amount": round(
                random.uniform(50, 5000),
                2
            ),
            "insurance_plan": random.choice(
                INSURANCE_PLANS
            ),
            "claim_status": random.choice(
                CLAIM_STATUSES
            )
        })

    return pd.DataFrame(claims)


def main() -> None:
    """Generate all synthetic healthcare datasets."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    patients = generate_patients()
    providers = generate_providers()
    claims = generate_claims()

    patients.to_csv(
        f"{RAW_DATA_DIR}/patients.csv",
        index=False
    )

    providers.to_csv(
        f"{RAW_DATA_DIR}/providers.csv",
        index=False
    )

    claims.to_csv(
        f"{RAW_DATA_DIR}/claims.csv",
        index=False
    )

    print(
        "Synthetic healthcare claims data "
        "generated successfully."
    )


if __name__ == "__main__":
    main()
