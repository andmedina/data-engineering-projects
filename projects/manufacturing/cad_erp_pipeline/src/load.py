"""Load transformed engineering and manufacturing data outputs."""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from config import DATABASE_URL, PROCESSED_DATA_DIR


def ensure_processed_data_directory() -> None:
    """Create the processed data directory if it does not already exist."""
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


def save_processed_data(dataframes: dict[str, pd.DataFrame]) -> None:
    """Save transformed datasets as CSV files."""
    ensure_processed_data_directory()

    for dataset_name, dataframe in dataframes.items():
        output_path = PROCESSED_DATA_DIR / f"{dataset_name}_processed.csv"
        dataframe.to_csv(output_path, index=False)


def get_database_engine() -> Engine:
    """Create and return a PostgreSQL database engine."""
    return create_engine(DATABASE_URL)


def load_dataframes_to_postgres(dataframes: dict[str, pd.DataFrame]) -> None:
    """Load transformed datasets into PostgreSQL tables."""
    engine: Engine = get_database_engine()

    load_order: list[str] = ["suppliers", "parts", "inventory", "bom"]

    for table_name in load_order:
        dataframe = dataframes[table_name]

        dataframe.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
        )
