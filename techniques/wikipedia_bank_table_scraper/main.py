#!/usr/bin/env python3
"""
Banks and GDP ETL Exercise.

This script demonstrates two approaches for scraping tables from Wikipedia:
1. Pandas `read_html`
2. BeautifulSoup + cleaning citations

It outputs cleaned DataFrames.
"""
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup

URL_BANKS = "https://en.wikipedia.org/wiki/List_of_largest_banks"
URL_GDP = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"


def remove_citations(text: str) -> str:
    """Remove citation denotations like [1], [2], etc. from a string."""
    return re.sub(r"\[.*?\]", "", text) if isinstance(text, str) else text


def fetch_html(url: str, timeout: int = 30) -> str:
    """Fetch HTML content from the specified URL with a timeout."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.text


def extract_tables_pandas(url: str) -> pd.DataFrame:
    """Fetch HTML with headers, parse, and pick the table that matches expected columns."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    tables = pd.read_html(response.text)
    for table in tables:
        cols = {str(c).lower() for c in table.columns}
        if "bank name" in cols or "bank" in cols:
            return table

    raise ValueError("No matching bank table found on the page.")


def extract_table_bs(url: str) -> pd.DataFrame:
    """
    Extract the first wikitable from a webpage using BeautifulSoup.

    Cleans citations and returns as a DataFrame.

    Parameters
    ----------
    url : str
        The webpage URL.

    Returns
    -------
    pd.DataFrame
        Cleaned table as a pandas DataFrame.
    """
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    tables = soup.select("table.wikitable")
    if not tables:
        raise ValueError("No wikitable found on page.")

    # Convert first table to DataFrame
    df = pd.read_html(str(tables[0]))[0]

    # Remove citations from all cells
    df = df.applymap(remove_citations) #type: ignore

    # Handle multiindex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = pd.MultiIndex.from_tuples(
            [(remove_citations(col[0]), remove_citations(col[1])) for col in df.columns]
        )

    return df


def main() -> None:
    """Run ETL extraction examples for banks and GDP tables."""
    print("Extracting largest banks table via pandas...")
    df_banks = extract_tables_pandas(URL_BANKS)
    print(df_banks.head())

    print("\nExtracting GDP table via pandas + BeautifulSoup + cleaning...")
    df_gdp = extract_table_bs(URL_GDP)
    print(df_gdp.head())


if __name__ == "__main__":
    main()
