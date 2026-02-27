#!/usr/bin/env python3
"""
Database connection basics (SQLite + pandas).

This script demonstrates:
- Creating/connecting to a SQLite database
- Loading a CSV into a SQL table
- Running a few basic SELECT queries
- Appending a new row and re-checking counts

Expected input file (in same directory):
- INSTRUCTOR.csv

Outputs:
- STAFF.db (SQLite database file)
- Console prints of queries/results
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


DB_PATH = Path("STAFF.db")
CSV_PATH = Path("INSTRUCTOR.csv")
TABLE_NAME = "INSTRUCTOR"
COLUMNS = ["ID", "FNAME", "LNAME", "CITY", "CCODE"]


def load_instructor_csv(csv_path: Path, columns: list[str]) -> pd.DataFrame:
    """
    Load the instructor CSV into a DataFrame with known column names.

    Parameters
    ----------
    csv_path : Path
        Path to the CSV file.
    columns : list[str]
        Column names to apply to the CSV.

    Returns
    -------
    pd.DataFrame
        Loaded instructor data.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing required file: {csv_path}")

    df = pd.read_csv(csv_path, names=columns)
    return df


def write_table(df: pd.DataFrame, conn: sqlite3.Connection, table_name: str) -> None:
    """
    Write the DataFrame into SQLite, replacing the table if it exists.

    Parameters
    ----------
    df : pd.DataFrame
        Data to write.
    conn : sqlite3.Connection
        Open SQLite connection.
    table_name : str
        Target table name.
    """
    df.to_sql(table_name, conn, if_exists="replace", index=False)


def run_query(conn: sqlite3.Connection, query: str) -> pd.DataFrame:
    """
    Execute a SQL query and return the results as a DataFrame.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open SQLite connection.
    query : str
        SQL query to execute.

    Returns
    -------
    pd.DataFrame
        Query results.
    """
    return pd.read_sql(query, conn)


def append_row(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Append a single example row to the given table.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open SQLite connection.
    table_name : str
        Target table name.
    """
    new_row = pd.DataFrame(
        {
            "ID": [100],
            "FNAME": ["John"],
            "LNAME": ["Doe"],
            "CITY": ["Paris"],
            "CCODE": ["FR"],
        }
    )
    new_row.to_sql(table_name, conn, if_exists="append", index=False)


def main() -> None:
    """Run the demo workflow end-to-end."""
    df = load_instructor_csv(CSV_PATH, COLUMNS)

    with sqlite3.connect(DB_PATH) as conn:
        write_table(df, conn, TABLE_NAME)
        print("Table is ready")

        queries = [
            f"SELECT * FROM {TABLE_NAME}",
            f"SELECT FNAME FROM {TABLE_NAME}",
            f"SELECT COUNT(*) AS row_count FROM {TABLE_NAME}",
        ]

        for query in queries:
            print("\n" + query)
            print(run_query(conn, query))

        append_row(conn, TABLE_NAME)
        print("\nData appended successfully")

        count_query = f"SELECT COUNT(*) AS row_count FROM {TABLE_NAME}"
        print("\n" + count_query)
        print(run_query(conn, count_query))


if __name__ == "__main__":
    main()
