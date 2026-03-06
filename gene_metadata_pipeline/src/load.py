"""
Load transformed gene metadata into a SQLite database.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from .config import DATABASE_NAME, TABLE_NAME


def load_to_sqlite(df: pd.DataFrame) -> None:
    """
    Load the transformed dataframe into a SQLite database.
    """
    db_path = Path(DATABASE_NAME)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(db_path)
    df.to_sql(TABLE_NAME, connection, if_exists="replace", index=False)
    connection.close()