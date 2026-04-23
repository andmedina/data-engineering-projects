# Gene Metadata ETL Pipeline

This project is a bioinformatics-focused ETL pipeline that extracts human gene metadata from the Ensembl REST API, transforms raw JSON into a structured tabular format, and loads the results into a SQLite database.

## Overview

The pipeline uses a selected list of human genes and retrieves metadata including:

- Gene symbol
- Ensembl gene ID
- Chromosome
- Start position
- End position
- Strand
- Description
- Species

This project demonstrates a full ETL workflow using Python, REST APIs, JSON processing, Pandas, SQLite, and logging.

## ETL Workflow

### Extract
- Calls the Ensembl REST API for each gene symbol
- Collects raw JSON metadata
- Saves raw results to `data/raw/gene_metadata_raw.json`

### Transform
- Parses nested JSON responses
- Selects relevant gene metadata fields
- Converts data into a structured tabular format using Pandas
- Performs basic validation and cleaning

### Load
- Loads transformed data into a SQLite database
- Stores structured records for downstream querying and analysis

## Tech Stack

- Python
- Pandas
- SQLite
- REST API
- JSON
- Logging

## Project Structure

```text
gene_metadata_pipeline/
├── src/
├── sql/
├── main.py
├── requirements.txt
└── README.md
