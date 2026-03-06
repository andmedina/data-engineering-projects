"""
Extract gene metadata from the Ensembl REST API.
"""

from __future__ import annotations

import requests

from .config import ENSEMBL_API_BASE, GENE_LOOKUP_ENDPOINT, GENE_LIST, HEADERS


def fetch_gene_metadata(gene_symbol: str) -> dict:
    """
    Fetch metadata for a single gene symbol from Ensembl.

    Parameters
    ----------
    gene_symbol : str
        Human gene symbol, e.g. BRCA1.

    Returns
    -------
    dict
        JSON response parsed into a Python dictionary.
    """
    endpoint = GENE_LOOKUP_ENDPOINT.format(gene=gene_symbol)
    url = f"{ENSEMBL_API_BASE}{endpoint}"

    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()

    return response.json()


def extract_all_genes() -> list[dict]:
    """
    Fetch metadata for all configured genes.

    Returns
    -------
    list[dict]
        List of gene metadata dictionaries.
    """
    results = []

    for gene in GENE_LIST:
        gene_data = fetch_gene_metadata(gene)
        results.append(gene_data)

    return results
