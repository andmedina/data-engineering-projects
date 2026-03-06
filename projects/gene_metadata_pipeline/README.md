# Gene Metadata ETL Pipeline

This project is a bioinformatics-focused ETL pipeline that extracts human gene metadata from the Ensembl REST API, transforms the raw JSON into a structured tabular format, and loads the results into a SQLite database.

## Overview

The pipeline uses a list of selected human genes and retrieves metadata such as:

- gene symbol
- Ensembl gene ID
- chromosome
- start position
- end position
- strand
- description
- species

The project demonstrates a full ETL workflow using Python, APIs, JSON processing, Pandas, SQLite, and logging.

---

## ETL Workflow

### Extract
- Calls the Ensembl REST API for each gene symbol
- Collects raw JSON metadata
- Saves raw results to:

```text
data/raw/gene_metadata_raw.json