"""
Extract gene metadata from the Ensembl REST API.
"""

from __future__ import annotations

import json
from pathlib import Path

import requests

from .config import ENSEMBL_API_BASE, GENE_LOOKUP_ENDPOINT, GENE_LIST, HEADERS, RAW_JSON_PATH


def fetch_gene_metadata(gene_symbol: str) -> dict:
    """
    Fetch metadata for a single gene symbol from Ensembl.
    """
    endpoint = GENE_LOOKUP_ENDPOINT.format(gene=gene_symbol)
    url = f"{ENSEMBL_API_BASE}{endpoint}"

    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_all_genes() -> list[dict]:
    """
    Fetch metadata for all configured genes.
    """
    results = []

    for gene in GENE_LIST:
        gene_data = fetch_gene_metadata(gene)
        results.append(gene_data)

    return results


def save_raw_data(raw_data: list[dict], output_path: str = RAW_JSON_PATH) -> None:
    """
    Save raw API responses to JSON.
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as file_handle:
        json.dump(raw_data, file_handle, indent=2)
