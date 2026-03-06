"""
Configuration settings for the Gene Metadata ETL Pipeline.
"""

ENSEMBL_API_BASE = "https://rest.ensembl.org"
GENE_LOOKUP_ENDPOINT = "/lookup/symbol/homo_sapiens/{gene}"

GENE_LIST = [
    "BRCA1",
    "BRCA2",
    "TP53",
    "EGFR",
    "MYC",
    "CFTR",
    "APOE",
]

DATABASE_NAME = "gene_metadata.db"
TABLE_NAME = "genes"

RAW_JSON_PATH = "data/raw/gene_metadata_raw.json"
PROCESSED_CSV_PATH = "data/processed/gene_metadata_processed.csv"
LOG_FILE = "logs/etl_log.txt"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}