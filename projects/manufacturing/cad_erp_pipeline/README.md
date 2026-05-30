# CAD-to-ERP Engineering Data Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white">
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white">
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white">
  <img src="https://img.shields.io/badge/ETL_Pipeline-Engineering_Data-blue?style=for-the-badge">
</p>

## Overview

This project simulates a real-world aerospace manufacturing and engineering data pipeline that integrates CAD/PLM-style engineering metadata with ERP and inventory-style operational datasets.

The pipeline extracts engineering source exports, generates scalable synthetic manufacturing datasets, validates manufacturing data quality, transforms nested Bill of Materials (BOM) structures into relational tables, loads transformed datasets into PostgreSQL, generates automated data quality reports, and produces analytics-ready datasets for downstream reporting and operational workflows.

This project was designed to simulate engineering/manufacturing data environments commonly found in:
- aerospace manufacturing
- defense contractors
- CAD/PLM systems
- ERP systems
- manufacturing analytics platforms

---

# Pipeline Architecture

```mermaid
flowchart LR
    A[Source Engineering Exports] --> B[Synthetic Scaling Layer]
    B --> C[Raw Operational Datasets]
    C --> D[Extract Layer]
    D --> E[Validation Layer]
    E --> F[Transformation Layer]
    F --> G[Processed Analytics Tables]
```

---

# Engineering Data Workflow

```mermaid
flowchart TD
    A[CAD Metadata JSON] --> B[Engineering Parts Table]
    C[Assembly BOM JSON] --> D[Flattened Relational BOM Table]
    E[Supplier CSV] --> F[Supplier Dimension Table]
    G[Inventory CSV] --> H[Inventory Analytics Table]

    B --> I[Manufacturing Analytics]
    D --> I
    F --> I
    H --> I
```

---

# Key Features

- Extracts engineering and manufacturing source exports
- Generates scalable synthetic aerospace manufacturing datasets
- Processes JSON and CSV operational datasets
- Validates engineering metadata and supplier relationships
- Flattens nested BOM structures into relational tables
- Produces analytics-ready manufacturing datasets
- Simulates CAD-to-ERP engineering workflows
- Demonstrates modular ETL pipeline architecture
- Implements inventory business-rule transformations
- Simulates operational-scale manufacturing environments
- Loads transformed datasets into PostgreSQL
- Generates automated data quality reports
- Provides SQL analytics queries for operational reporting
- Implements pipeline observability and execution metrics

---

# Synthetic Operational Scaling

The project includes a synthetic scaling layer that expands seed engineering exports into larger operational manufacturing datasets.

This allows the pipeline to simulate:
- large aerospace part inventories
- multi-assembly manufacturing relationships
- operational warehouse inventory tracking
- ERP-style manufacturing datasets
- scalable relational manufacturing workflows

Example scaling:
- hundreds of engineering parts
- dozens of assemblies
- large relational BOM relationships
- expanded inventory operations

This simulates how engineering metadata may evolve into larger operational manufacturing datasets inside real enterprise environments.

---

# Example Engineering Transformation

One major transformation performed by the pipeline is converting nested engineering BOM structures into relational manufacturing tables.

## Source Engineering BOM Structure

```json
{
  "assembly_id": "ASM-001",
  "parts": [
    {
      "part_number": "KSD-AER-1001",
      "quantity": 1
    }
  ]
}
```

## Flattened Relational Output

| assembly_id | part_number | quantity |
|---|---|---|
| ASM-001 | KSD-AER-1001 | 1 |

This transformation is highly relevant to:
- ERP systems
- manufacturing databases
- engineering analytics
- aerospace assembly tracking
- relational SQL workflows

---

# Inventory Business Logic

The pipeline derives operational analytics fields from raw inventory exports.

Example:

```python
below_reorder_level = stock_quantity < reorder_level
```

This simulates real-world ERP inventory monitoring and procurement alert workflows.

---

# Project Structure

```text
cad_erp_pipeline/
├── data
│   ├── source
│   ├── raw
│   └── processed
├── images
├── logs
├── sql
├── src
│   ├── config.py
│   ├── extract.py
│   ├── generate_data.py
│   ├── load.py
│   ├── main.py
│   ├── transform.py
│   └── validate.py
├── README.md
└── requirements.txt
```

---

# Source Data Files

| File | Purpose |
|---|---|
| `cad_parts_export.json` | Simulated CAD/PLM engineering metadata export |
| `assembly_bom_export.json` | Nested engineering Bill of Materials (BOM) structures |
| `suppliers_export.csv` | ERP supplier and procurement data |
| `inventory_export.csv` | Inventory and warehouse operational data |

---

# Raw Operational Outputs

The synthetic scaling layer generates larger operational manufacturing datasets inside:

```text
data/raw/
```

Generated datasets include:
- large engineering parts datasets
- expanded assembly relationships
- operational inventory records
- relational manufacturing BOM tables

These datasets simulate operational-scale aerospace manufacturing environments.

---

# Processed Outputs

| Output File | Description |
|---|---|
| `parts_processed.csv` | Cleaned and standardized engineering part metadata with normalized revisions, materials, and engineering status values |
| `bom_processed.csv` | Flattened relational Bill of Materials (BOM) table generated from nested assembly structures for SQL-based analysis |
| `suppliers_processed.csv` | Standardized supplier master dataset with normalized supplier identifiers and procurement attributes |
| `inventory_processed.csv` | Inventory analytics dataset containing warehouse inventory metrics and derived business logic such as reorder-level monitoring |
| `data_quality_report.txt` | Automated data quality report summarizing row counts, missing values, duplicate records, and overall pipeline quality metrics |
| PostgreSQL Tables | Transformed datasets loaded into PostgreSQL for downstream reporting, analytics, and operational workflows |


---

---

# Data Quality Reporting

The pipeline automatically generates a data quality report during execution.

Generated report:

```text
logs/data_quality_report.txt
```

The report summarizes:

- dataset row counts
- column counts
- missing values
- duplicate rows
- overall pipeline quality metrics

Example output:

```text
Overall Summary
--------------------
Datasets Evaluated: 4
Total Rows Processed: 17
Total Missing Values: 0
Total Duplicate Rows: 0

Status: PASSED
```

This simulates enterprise data quality monitoring workflows commonly used in manufacturing and ERP environments.

---

# Analytics Queries

The project includes example SQL analytics queries located in:

```text
sql/analytics_queries.sql
```

Example business use cases:

- Inventory shortage monitoring
- Supplier dependency analysis
- Assembly complexity reporting
- Engineering review tracking
- Inventory exposure analysis
- Manufacturing operational reporting

Example analytics questions answered by the project:

- Which parts are below reorder level?
- Which suppliers support the most engineering components?
- Which assemblies contain the largest number of parts?
- Which warehouses hold the most inventory?
- Which engineering parts are currently under review?

These queries demonstrate how transformed ERP-style datasets can support downstream reporting and operational decision-making workflows.

---

# Pipeline Execution Example

The pipeline provides execution observability throughout the ETL lifecycle, including extraction metrics, validation status, transformation statistics, data quality reporting, PostgreSQL load metrics, and runtime tracking.

Example pipeline execution:

![Pipeline Execution](images/pipeline_execution.png)

Key execution metrics include:

- source dataset row counts
- transformed dataset row counts
- validation status
- data quality report generation
- PostgreSQL load statistics
- total pipeline runtime

This observability layer helps simulate production-style monitoring and troubleshooting workflows commonly used in enterprise data engineering environments.

---

# Technologies Used

- Python
- Pandas
- PostgreSQL
- SQL
- JSON
- CSV
- ETL Pipelines
- Data Validation
- Relational Modeling

---

# Data Validation Checks

The pipeline validates:
- required columns
- missing values
- duplicate part numbers
- invalid engineering statuses
- negative inventory values
- missing supplier relationships

---

# Data Sources

This project currently uses synthetic engineering and manufacturing metadata to simulate aerospace and CAD-to-ERP operational workflows.

In production environments, similar data is commonly sourced from:
- CAD/PLM platforms
- ERP systems
- manufacturing databases
- engineering BOM exports
- supplier and inventory systems

Example public datasets and references:
- https://www.kaggle.com/datasets/shivamb/machine-predictive-maintenance-classification
- https://www.kaggle.com/datasets/amirmotefaker/supply-chain-dataset

---

# Future Improvements

- Apache Airflow orchestration
- Automated pipeline scheduling
- ERP API integrations
- Engineering revision history tracking
- Supplier lead-time analytics
- Inventory forecasting
- Manufacturing KPI dashboards
- Real-time manufacturing event streaming