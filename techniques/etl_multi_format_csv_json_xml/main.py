#!/usr/bin/env python3
"""
Pipeline:
- Download ZIP source data
- Extract CSV, JSON, and XML records
- Transform height and weight to metric units
- Load transformed data to CSV
- Log each phase of execution
"""

import glob
import logging
import os
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd


SOURCE_URL = (
    "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
    "IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/"
    "Lab%20-%20Extract%20Transform%20Load/data/source.zip"
)

DEFAULT_ZIP_NAME = "source.zip"
DEFAULT_EXTRACT_DIR = "extracted_files"
DEFAULT_OUTPUT_CSV = "transformed_data.csv"
DEFAULT_LOG_FILE = "log_file.txt"


def setup_logger(log_path: str) -> logging.Logger:
    """
    Configure and return a logger for the ETL pipeline.

    Parameters
    ----------
    log_path : str
        Path to the log file where ETL progress will be recorded.

    Returns
    -------
    logging.Logger
        Configured logger instance with file and console handlers.
    """
    logger = logging.getLogger("etl_project_1")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s,%(message)s", "%Y-%m-%d-%H:%M:%S")

        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def download_and_unzip(url: str, zip_name: str, extract_dir: str) -> None:
    """
    Download a ZIP file from a URL and extract its contents.

    Parameters
    ----------
    url : str
        Source URL of the ZIP file.
    zip_name : str
        Local filename for the downloaded ZIP.
    extract_dir : str
        Directory where the ZIP contents will be extracted.

    Returns
    -------
    None
    """
    os.makedirs(extract_dir, exist_ok=True)
    urllib.request.urlretrieve(url, zip_name)

    with zipfile.ZipFile(zip_name, "r") as zip_ref:
        zip_ref.extractall(extract_dir)


def extract_from_csv(file_path: str) -> pd.DataFrame:
    """
    Extract data from a CSV file into a DataFrame.
    """
    return pd.read_csv(file_path)


def extract_from_json(file_path: str) -> pd.DataFrame:
    """
    Extract data from a newline-delimited JSON file into a DataFrame.
    """
    return pd.read_json(file_path, lines=True)

def require_text(parent: ET.Element, tag: str) -> str:
    """
    Retrieve required text content from an XML child element.

    Parameters
    ----------
    parent : xml.etree.ElementTree.Element
        Parent XML element.
    tag : str
        Name of the child tag to retrieve.

    Returns
    -------
    str
        Stripped text content of the child element.

    Raises
    ------
    ValueError
        If the child element is missing or contains no text.
    """
    child = parent.find(tag)
    if child is None or child.text is None:
        raise ValueError(f"Missing required <{tag}> element.")
    return child.text.strip()

def extract_from_xml(file_path: str) -> pd.DataFrame:
    """
    Extract structured data from an XML file.

    Each XML record is expected to contain:
    - name
    - height
    - weight

    Parameters
    ----------
    file_path : str
        Path to the XML file.

    Returns
    -------
    pd.DataFrame
        DataFrame containing extracted records.

    Raises
    ------
    ValueError
        If required XML elements are missing.
    """
    rows = []
    tree = ET.parse(file_path)
    root = tree.getroot()

    for person in root:
        rows.append(
            {
                "name": require_text(person, "name"),
                "height": float(require_text(person, "height")),
                "weight": float(require_text(person, "weight")),
            }
        )

    return pd.DataFrame(rows)


def extract(directory: str) -> pd.DataFrame:
    """
    Extract data from all CSV, JSON, and XML files in a directory.

    Parameters
    ----------
    directory : str
        Directory containing source files.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame containing extracted data.
    """
    extracted = pd.DataFrame(columns=["name", "height", "weight"])

    for csvfile in glob.glob(os.path.join(directory, "*.csv")):
        extracted = pd.concat([extracted, extract_from_csv(csvfile)], ignore_index=True)

    for jsonfile in glob.glob(os.path.join(directory, "*.json")):
        extracted = pd.concat([extracted, extract_from_json(jsonfile)], ignore_index=True)

    for xmlfile in glob.glob(os.path.join(directory, "*.xml")):
        extracted = pd.concat([extracted, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform height and weight to metric units.
    Validates numeric integrity before conversion.
    """
    df = df.copy()

    for col in ("height", "weight"):
        numeric = pd.to_numeric(df[col], errors="coerce")

        if not isinstance(numeric, pd.Series):
            raise TypeError("Expected pandas Series from pd.to_numeric")

        bad_mask = numeric.isna() & df[col].notna()

        if bad_mask.any():
            bad_examples = df.loc[bad_mask, col].head(10).tolist()
            raise ValueError(
                f"Non-numeric values found in '{col}'. Example values: {bad_examples}"
            )

        df[col] = numeric.astype("float64")

    df["height"] = (df["height"] * 0.0254).round(2)
    df["weight"] = (df["weight"] * 0.45359237).round(2)

    return df


def load_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Save a DataFrame to a CSV file.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to write.
    output_path : str
        Destination file path.

    Returns
    -------
    None
    """
    df.to_csv(output_path, index=False)


def main() -> None:
    """
    Execute the full ETL pipeline:
    - Download data
    - Extract multi-format sources
    - Transform to metric units
    - Load to CSV
    - Log progress
    """
    logger = setup_logger(DEFAULT_LOG_FILE)

    try:
        logger.info("ETL Job Started")

        logger.info("Download phase Started")
        download_and_unzip(SOURCE_URL, DEFAULT_ZIP_NAME, DEFAULT_EXTRACT_DIR)
        logger.info("Download phase Ended")

        logger.info("Extract phase Started")
        extracted = extract(DEFAULT_EXTRACT_DIR)
        logger.info("Extract phase Ended")

        logger.info("Transform phase Started")
        transformed = transform(extracted)
        logger.info("Transform phase Ended")

        logger.info("Load phase Started")
        load_to_csv(transformed, DEFAULT_OUTPUT_CSV)
        logger.info("Load phase Ended")

        print("Transformed Data")
        print(transformed)

        logger.info("ETL Job Ended")

    except Exception as exc:
        logger.info("ETL Job Failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
