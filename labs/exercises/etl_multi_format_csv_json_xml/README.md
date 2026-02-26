# Multi-Format ETL Pipeline

## Overview

This project demonstrates a structured ETL pipeline that:

- Downloads a ZIP archive containing structured data files
- Extracts records from CSV, JSON, and XML formats
- Transforms height (inches → meters)
- Transforms weight (pounds → kilograms)
- Outputs a consolidated transformed dataset
- Logs each phase of execution

---

## Data Source

The pipeline downloads sample structured data from an external training dataset.
The dataset contains:

- `name`
- `height` (inches)
- `weight` (pounds)

These records are provided in multiple formats (CSV, JSON, XML) to simulate
real-world multi-source ingestion scenarios.

---

## How to Run

```bash
python main.py