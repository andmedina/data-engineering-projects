# GDP Country ETL Pipeline

A small end-to-end ETL pipeline that pulls country GDP data from an archived Wikipedia page (IMF table), cleans it, converts GDP from **USD millions → USD billions**, and saves the results to both **CSV** and **SQLite**.

## What it does

- Fetches HTML from an archived Wikipedia snapshot
- Extracts `Country` + `GDP_USD_millions`
- Cleans numeric GDP values and converts to `GDP_USD_billions`
- Writes output to:
  - `Countries_by_GDP.csv`
  - `World_Economies.db` (table: `Countries_by_GDP`)
- Runs a sample SQL query (`GDP_USD_billions >= 100`)
- Logs steps to `etl_project_log.txt`

## How to run

```bash
python main.py