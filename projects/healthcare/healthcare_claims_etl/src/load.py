"""Load functions for healthcare claims ETL pipeline."""

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


DATABASE_URL = "postgresql+psycopg2://localhost/healthcare_claims_etl"
SCHEMA_PATH = Path("sql/schema.sql")

# pyright: ignore[reportReturnType]
def create_database_engine() -> Engine:
    """Create a SQLAlchemy database engine."""
    return create_engine(DATABASE_URL)


def execute_schema(engine: Engine) -> None:
    """Execute database schema SQL."""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

    with engine.begin() as connection:
        connection.execute(text(schema_sql))


def load_dataframe(
    dataframe: pd.DataFrame,
    table_name: str,
    engine: Engine
) -> None:
    """Load a DataFrame into PostgreSQL."""
    dataframe.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False
    )


def load_processed_data(
    patients: pd.DataFrame,
    providers: pd.DataFrame,
    claims: pd.DataFrame
) -> None:
    """Load processed datasets into PostgreSQL."""
    engine = create_database_engine()

    execute_schema(engine)

    load_dataframe(patients, "patients", engine)
    load_dataframe(providers, "providers", engine)
    load_dataframe(claims, "claims", engine)
