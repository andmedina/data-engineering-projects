#!/usr/bin/env python3
"""
Top Movies Web Scrape ETL.

Extract:
- Scrape the "100 Most Highly-Ranked Films" table from an archived webpage.

Transform:
- Keep only the top 50 rows
- Convert Year to integer
- Filter films released in the 2000s (2000-2009)

Load:
- Save full top-50 dataset to CSV
- Save full top-50 dataset to SQLite
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup


URL = (
    "https://web.archive.org/web/20230902185655/"
    "https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
)
DB_NAME = "Movies.db"
TABLE_NAME = "Top_50"
CSV_PATH = "top_50_films.csv"
LOG_FILE = "etl_log.txt"


def log_progress(message: str, log_file: str = LOG_FILE) -> None:
    """Append a timestamped message to a log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as file_handle:
        file_handle.write(f"{timestamp} : {message}\n")


def fetch_html(url: str, timeout: int = 30) -> str:
    """Fetch HTML from a URL with a browser-like User-Agent."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.text


def extract_top_50(html: str) -> pd.DataFrame:
    """
    Extract the top 50 rows from the first wikitable on the page.

    Returns
    -------
    pd.DataFrame
        Columns: Average Rank, Film, Year
    """
    soup = BeautifulSoup(html, "html.parser")

    table = soup.select_one("table.wikitable")
    if table is None:
        raise ValueError("Could not find table.wikitable on the page.")

    rows = table.select("tbody tr")
    output_rows: List[Dict[str, str]] = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        output_rows.append(
            {
                "Average Rank": cols[0].get_text(strip=True),
                "Film": cols[1].get_text(strip=True),
                "Year": cols[2].get_text(strip=True),
            }
        )

        if len(output_rows) >= 50:
            break

    dataframe = pd.DataFrame(output_rows, columns=["Average Rank", "Film", "Year"])
    if dataframe.empty:
        raise ValueError("No movie rows extracted. Page layout may have changed.")

    return dataframe


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean types and produce a 2000s-filtered subset.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (top_50_clean, films_2000s)
    """
    dataframe = df.copy()

    dataframe["Year"] = (
        dataframe["Year"]
        .astype(str)
        .str.extract(r"(\d{4})", expand=False)
    )
    dataframe["Year"] = pd.to_numeric(dataframe["Year"], errors="raise").astype(int)

    films_2000s = dataframe[(dataframe["Year"] >= 2000) & (dataframe["Year"] <= 2009)]
    return dataframe, films_2000s


def load_to_csv(df: pd.DataFrame, path: str) -> None:
    """Write the dataframe to CSV."""
    df.to_csv(path, index=False)


def load_to_sqlite(df: pd.DataFrame, db_name: str, table_name: str) -> None:
    """Write the dataframe to SQLite."""
    with sqlite3.connect(db_name) as connection:
        df.to_sql(table_name, connection, if_exists="replace", index=False)


def main() -> None:
    """Run the ETL pipeline."""
    log_progress("ETL started.")

    try:
        log_progress("Fetching HTML...")
        html = fetch_html(URL)

        log_progress("Extracting top 50...")
        extracted = extract_top_50(html)

        log_progress("Transforming data...")
        top_50, films_2000s = transform(extracted)

        print("\nTop 50 films:")
        print(top_50)

        print("\nFilms released in the 2000s (2000-2009):")
        print(films_2000s)

        log_progress("Saving CSV...")
        load_to_csv(top_50, CSV_PATH)

        log_progress("Saving SQLite DB...")
        load_to_sqlite(top_50, DB_NAME, TABLE_NAME)

        log_progress("ETL completed successfully.")

    except (requests.RequestException, ValueError, sqlite3.Error) as exc:
        log_progress(f"ERROR: {type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    main()
