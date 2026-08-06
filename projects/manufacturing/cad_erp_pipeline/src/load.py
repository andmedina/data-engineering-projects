"""Load transformed engineering and manufacturing data outputs."""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from .config import DATABASE_URL, PROCESSED_DATA_DIR


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


def refresh_postgres_tables(connection: Connection) -> None:
    """
    Clear existing PostgreSQL records before loading fresh pipeline outputs.

    This project uses a full-refresh loading pattern so the pipeline can be
    rerun safely during local development and portfolio demonstrations.

    In production, this would typically be replaced with staging tables,
    incremental loads, or UPSERT logic.
    """
    connection.execute(
        text(
            """
            TRUNCATE TABLE
                bom,
                inventory,
                parts,
                suppliers;
            """
        )
    )


def load_dataframes_to_postgres(dataframes: dict[str, pd.DataFrame]) -> None:
    """Load transformed datasets into PostgreSQL tables."""
    engine: Engine = get_database_engine()

    load_order: list[str] = ["suppliers", "parts", "inventory", "bom"]

    with engine.begin() as connection:
        refresh_postgres_tables(connection)

        for table_name in load_order:
            dataframe = dataframes[table_name]

            dataframe.to_sql(
                table_name,
                connection,
                if_exists="append",
                index=False,
            )

            print(f"Loaded {len(dataframe)} rows into {table_name}")


# Production-oriented alternative:
#
# In a production system, the pipeline would usually avoid truncating final
# tables directly. A more robust pattern would be:
#
# 1. Load incoming data into staging tables
# 2. Validate staging data
# 3. Merge staging records into final tables using UPSERT logic
# 4. Track pipeline run metadata in an audit table
#
# Example PostgreSQL UPSERT pattern:
#
# INSERT INTO suppliers (supplier_id, supplier_name, country, supplier_type)
# VALUES (...)
# ON CONFLICT (supplier_id)
# DO UPDATE SET
#     supplier_name = EXCLUDED.supplier_name,
#     country = EXCLUDED.country,
#     supplier_type = EXCLUDED.supplier_type;
