# Data Engineering Portfolio

This repository contains hands-on work in **data engineering fundamentals**, including ETL pipelines, web scraping, API ingestion, data transformation, and database workflows using Python and SQL.

The repository is organized into **Projects** (multi-step pipelines) and **Labs** (focused technical exercises).

---

# Projects

Projects combine multiple data engineering skills into practical pipelines.

| Project | Description |
|----|----|
| `gene_metadata_pipeline` | **Bioinformatics ETL pipeline** that extracts human gene metadata from the Ensembl REST API, transforms JSON responses into structured tabular data, and loads results into SQLite |
| `gdp_country_pipeline` | ETL pipeline that extracts GDP data, transforms values, and loads results into SQLite |
| `shell_etl_psswd_to_sqlite` | Demonstrates ETL using shell tools (`cut`, `tr`, `sed`) and loads results into SQLite |
| `top_movies_webscrape_etl` | Web scraping pipeline that extracts movie rankings, transforms data, and stores results in SQLite |

---

# Labs

Labs demonstrate specific techniques used in data engineering workflows.

| Lab | Description |
|----|----|
| `database_connection_basics` | Load CSV data into SQLite using Python |
| `etl_multi_format_csv_json_xml` | Extract data from CSV, JSON, and XML formats and transform to metric units |
| `html_parsing_beautifulsoup` | Explore HTML parsing with BeautifulSoup |
| `multi_format_price_etl` | Simple ETL pipeline for price normalization |
| `requests_http_basics` | Make HTTP requests and inspect responses using Python |
| `rest_api_data_fetching` | Retrieve structured data from REST APIs |
| `sqlite_2nf_normalization_demo` | Demonstration of database normalization to Second Normal Form (2NF) |
| `wikipedia_bank_table_scraper` | Scrape structured tables from Wikipedia |
| `wikipedia_html_parsing` | Extract and analyze HTML content from Wikipedia pages |

---

# Technologies Used

- Python
- Pandas
- BeautifulSoup
- Requests
- SQLite
- Shell scripting
- REST APIs
- XML / JSON / CSV processing

---

# Purpose

This repository demonstrates core **data engineering skills and workflows**, including:

- Building reproducible ETL pipelines
- Extracting data from web sources and APIs
- Transforming and validating structured data
- Loading results into relational databases
- Automating data workflows with scripts
- Working with scientific datasets such as genomic metadata