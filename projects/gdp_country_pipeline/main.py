#!/usr/bin/env python3
"""
GDP Country ETL Pipeline.

This pipeline:
- Fetches GDP data from an archived Wikipedia page
- Extracts country and GDP (USD millions)
- Cleans and converts GDP to USD billions
- Saves results to CSV
- Loads results into SQLite
- Executes a sample SQL query
- Logs execution steps
"""
import sqlite3
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup


URL = (
    "https://web.archive.org/web/20230902185326/"
    "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
)

CSV_OUT = "Countries_by_GDP.csv"
DB_OUT = "World_Economies.db"
TABLE_NAME = "Countries_by_GDP"
LOG_FILE = "etl_project_log.txt"


def log_progress(message: str, log_file: str = LOG_FILE) -> None:
    """Append a timestamped message to the ETL log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as file_handle:
        file_handle.write(f"{timestamp} : {message}\n")


def fetch_html(url: str) -> str:
    """Fetch HTML content from the specified URL."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def extract(url: str) -> pd.DataFrame:
    """
    Extract country and GDP data from the Wikipedia page.

    Returns
    -------
    pd.DataFrame
        DataFrame containing Country and GDP_USD_millions.
    """
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    target_table = soup.select_one("table.wikitable tbody")
    if target_table is None:
        raise ValueError("Could not find a wikitable on the page.")

    rows = target_table.find_all("tr")
    output_rows = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        country = cols[0].get_text(strip=True)
        gdp_raw = cols[2].get_text(strip=True)

        if not country or "—" in gdp_raw:
            continue

        output_rows.append(
            {"Country": country, "GDP_USD_millions": gdp_raw}
        )

    dataframe = pd.DataFrame(
        output_rows, columns=["Country", "GDP_USD_millions"]
    )

    if dataframe.empty:
        raise ValueError("No rows extracted. Page layout may have changed.")

    return dataframe


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert GDP from USD millions (string) to USD billions (float).
    """
    dataframe = df.copy()

    series = dataframe["GDP_USD_millions"].astype(str)
    series = series.str.replace(",", "", regex=False)
    series = series.str.extract(r"(\d+(?:\.\d+)?)", expand=False)

    dataframe["GDP_USD_millions"] = pd.to_numeric(series, errors="raise")
    dataframe["GDP_USD_billions"] = (
        dataframe["GDP_USD_millions"] / 1000
    ).round(2)

    dataframe = dataframe.drop(columns=["GDP_USD_millions"])
    dataframe = dataframe.sort_values(
        "GDP_USD_billions", ascending=False
    ).reset_index(drop=True)

    return dataframe


def load_to_csv(df: pd.DataFrame, path: str) -> None:
    """Save dataframe to CSV file."""
    df.to_csv(path, index=False)


def load_to_db(df: pd.DataFrame, db_path: str, table: str) -> None:
    """Save dataframe to SQLite database."""
    with sqlite3.connect(db_path) as connection:
        df.to_sql(table, connection, if_exists="replace", index=False)


def run_query(query: str, db_path: str) -> pd.DataFrame:
    """Execute SQL query and return results."""
    with sqlite3.connect(db_path) as connection:
        return pd.read_sql(query, connection)


def main() -> None:
    """Execute full ETL pipeline."""
    log_progress("ETL process started.")

    try:
        log_progress("Extracting data from URL...")
        dataframe = extract(URL)

        log_progress("Transforming data...")
        dataframe = transform(dataframe)

        log_progress("Saving data to CSV...")
        load_to_csv(dataframe, CSV_OUT)

        log_progress("Saving data to SQLite database...")
        load_to_db(dataframe, DB_OUT, TABLE_NAME)

        query = (
            f"SELECT * FROM {TABLE_NAME} "
            "WHERE GDP_USD_billions >= 100 "
            "ORDER BY GDP_USD_billions DESC"
        )

        result = run_query(query, DB_OUT)
        print(result)

        log_progress("ETL process completed successfully.")

    except (requests.RequestException, ValueError, sqlite3.Error) as exc:
        log_progress(f"ERROR: {type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    main()
