#!/usr/bin/env python3
"""
Multi-format ETL (CSV/JSON/XML) - Car price rounding.

This pipeline:
- Downloads a ZIP dataset
- Extracts CSV, JSON, and XML files from the ZIP
- Concatenates them into one DataFrame
- Rounds `price` to 2 decimal places
- Writes a transformed CSV
- Logs each pipeline phase
"""

from __future__ import annotations

import glob
import os
import urllib.request
import zipfile
from datetime import datetime

import pandas as pd


DATA_URL = (
    "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
    "IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/"
    "Lab%20-%20Extract%20Transform%20Load/data/datasource.zip"
)
ZIP_NAME = "source.zip"
EXTRACT_DIR = "extracted_files"

LOG_FILE = "log_file.txt"
TARGET_FILE = "transformed_data.csv"

EXPECTED_COLUMNS = ["car_model", "year_of_manufacture", "price", "fuel"]


def log_progress(message: str, log_file: str = LOG_FILE) -> None:
    """Append a timestamped log line to `log_file`."""
    timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as file_handle:
        file_handle.write(f"{timestamp},{message}\n")


def download_and_unzip(url: str, zip_name: str, extract_dir: str) -> None:
    """
    Download a zip file from `url` and extract its contents to `extract_dir`.

    Raises
    ------
    urllib.error.URLError
        If the download fails.
    zipfile.BadZipFile
        If the file is not a valid zip.
    """
    os.makedirs(extract_dir, exist_ok=True)
    urllib.request.urlretrieve(url, zip_name)

    with zipfile.ZipFile(zip_name, "r") as zip_ref:
        zip_ref.extractall(extract_dir)


def extract_from_csv(path: str) -> pd.DataFrame:
    """Read a CSV file into a DataFrame."""
    return pd.read_csv(path)


def extract_from_json(path: str) -> pd.DataFrame:
    """Read a newline-delimited JSON file into a DataFrame."""
    return pd.read_json(path, lines=True)


def extract_from_xml(path: str) -> pd.DataFrame:
    """Read an XML file into a DataFrame."""
    return pd.read_xml(path)


def extract(directory: str) -> pd.DataFrame:
    """
    Extract and concatenate all CSV/JSON/XML files found in `directory`.

    Returns
    -------
    pd.DataFrame
        Combined dataframe containing (at minimum) EXPECTED_COLUMNS.
    """
    extracted_data = pd.DataFrame(columns=EXPECTED_COLUMNS)

    for csv_path in glob.glob(os.path.join(directory, "*.csv")):
        extracted_data = pd.concat(
            [extracted_data, extract_from_csv(csv_path)],
            ignore_index=True,
        )

    for json_path in glob.glob(os.path.join(directory, "*.json")):
        extracted_data = pd.concat(
            [extracted_data, extract_from_json(json_path)],
            ignore_index=True,
        )

    for xml_path in glob.glob(os.path.join(directory, "*.xml")):
        extracted_data = pd.concat(
            [extracted_data, extract_from_xml(xml_path)],
            ignore_index=True,
        )

    return extracted_data


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform extracted data.

    Currently:
    - Coerces `price` to numeric
    - Rounds `price` to 2 decimals

    Raises
    ------
    ValueError
        If `price` column is missing.
    """
    if "price" not in df.columns:
        raise ValueError("Missing required column: 'price'.")

    dataframe = df.copy()
    dataframe["price"] = pd.to_numeric(dataframe["price"], errors="raise").round(2)
    return dataframe


def load_data(path: str, df: pd.DataFrame) -> None:
    """Write `df` to CSV at `path`."""
    df.to_csv(path, index=False)


def main() -> None:
    """Run the ETL pipeline end-to-end."""
    log_progress("ETL Job Started")

    try:
        log_progress("Download phase Started")
        download_and_unzip(DATA_URL, ZIP_NAME, EXTRACT_DIR)
        log_progress("Download phase Ended")

        log_progress("Extract phase Started")
        extracted = extract(EXTRACT_DIR)
        log_progress("Extract phase Ended")

        log_progress("Transform phase Started")
        transformed = transform(extracted)
        log_progress("Transform phase Ended")

        log_progress("Load phase Started")
        load_data(TARGET_FILE, transformed)
        log_progress("Load phase Ended")

        log_progress("ETL Job Ended")

        print("Transformed Data")
        print(transformed)

    except Exception as exc:  # pylint: disable=broad-exception-caught
        log_progress(f"ETL Job Failed: {type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    main()
