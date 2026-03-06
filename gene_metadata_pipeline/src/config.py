"""
Configuration settings for the Gene Metadata ETL Pipeline.
"""

# Ensembl REST API base endpoint
ENSEMBL_API_BASE = "https://rest.ensembl.org"

# Endpoint template for gene lookup by symbol
GENE_LOOKUP_ENDPOINT = "/lookup/symbol/homo_sapiens/{gene}"

# Genes we will pull metadata for
GENE_LIST = [
    "BRCA1",
    "BRCA2",
    "TP53",
    "EGFR",
    "MYC",
    "CFTR",
    "APOE"
]

# Database configuration
DATABASE_NAME = "gene_metadata.db"
TABLE_NAME = "genes"

# Logging
LOG_FILE = "logs/etl_log.txt"

# Request headers required by Ensembl API
HEADERS = {
    "Content-Type": "application/json"
}
