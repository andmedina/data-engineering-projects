# Data Engineering Portfolio

<p align="center">
  <img src="https://img.shields.io/badge/Data_Engineering-Portfolio-blue?style=for-the-badge">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white">
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white">
  <img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white">
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white">
  <img src="https://img.shields.io/badge/SQL-025E8C?style=for-the-badge">
</p>

This repository contains data engineering projects focused on ETL development, data pipelines, API ingestion, web scraping, database workflows, analytics engineering, and domain-specific data systems using Python and SQL.

Projects span healthcare, bioinformatics, and engineering/manufacturing data workflows.

Projects are organized into:
- **Projects** — larger end-to-end pipeline implementations
- **Techniques** — focused demonstrations of core data engineering concepts and workflows

---


# Projects

Projects combine multiple data engineering concepts into realistic, multi-stage workflows.

## 🏥 Healthcare

| Project | Description |
|----|----|
| [healthcare_claims_etl](./projects/healthcare/healthcare_claims_etl/) | End-to-end healthcare claims ETL pipeline using Python, PostgreSQL, Airflow, and analytics-ready transformations |
| `healthcare_streaming_pipeline` | Real-time patient vitals streaming pipeline using Kafka and event-driven processing |

---

## 🧬 Bioinformatics

| Project | Description |
|----|----|
| [gene_metadata_pipeline](./projects/gene_metadata_pipeline/) | Bioinformatics ETL pipeline extracting genomic metadata from the Ensembl API and loading structured results into SQLite |
| `genomics_data_pipeline` | Pipeline for processing biological sequence data into analytics-ready relational datasets |

---

## ✈️ Engineering & Manufacturing Data Systems

| Project | Description |
|----|----|
| `cad_erp_pipeline` | Engineering metadata ETL pipeline simulating CAD-to-ERP workflows for manufacturing environments using Python, PostgreSQL, and structured BOM processing |
| `engineering_bom_analytics` | Relational manufacturing dataset modeling assemblies, part revisions, suppliers, and inventory analytics for engineering reporting workflows |

---

## ⚙️ General Data Engineering

| Project | Description |
|----|----|
| [gdp_country_pipeline](./projects/gdp_country_pipeline/) | ETL pipeline extracting GDP data and storing transformed results in SQLite |
| [shell_etl_psswd_to_sqlite](./projects/shell_etl_psswd_to_sqlite/) | Shell-based ETL pipeline using Unix data processing utilities |
| [top_movies_webscrape_etl](./projects/top_movies_webscrape_etl/) | Web scraping pipeline storing structured movie ranking data in SQLite |

---

# Techniques

Techniques demonstrate focused concepts commonly used in data engineering workflows.

| Technique | Description |
|----|----|
| `database_connection_basics` | Loading CSV data into SQLite using Python |
| `etl_multi_format_csv_json_xml` | Processing CSV, JSON, and XML datasets into standardized formats |
| `html_parsing_beautifulsoup` | HTML parsing and structured extraction using BeautifulSoup |
| `multi_format_price_etl` | Multi-format ETL pipeline for price normalization |
| `requests_http_basics` | HTTP request handling and response inspection using Python |
| `rest_api_data_fetching` | Retrieving and processing structured data from REST APIs |
| `sqlite_2nf_normalization_demo` | Demonstration of relational database normalization to Second Normal Form |
| `wikipedia_bank_table_scraper` | Structured table extraction from Wikipedia pages |
| `wikipedia_html_parsing` | HTML extraction and parsing workflows for semi-structured web data |

---

# Technologies Used

- Python
- SQL
- PostgreSQL
- SQLite
- Pandas
- Apache Airflow
- BeautifulSoup
- Requests
- Shell scripting
- REST APIs
- JSON / XML / CSV processing

---

# Purpose

This repository demonstrates practical data engineering workflows including:

- Designing reproducible ETL pipelines
- Building structured data ingestion workflows
- Extracting data from APIs and web sources
- Transforming and validating structured datasets
- Loading data into relational databases
- Automating pipeline execution
- Working with healthcare, bioinformatics, and engineering datasets
- Modeling real-world operational data systems
- Organizing analytics-ready infrastructure and reporting pipelines
