# Top Movies Web Scrape ETL

Scrapes an archived webpage containing the “100 Most Highly-Ranked Films” table and builds a small ETL pipeline.

## What it does
**Extract**
- Downloads HTML from a Web Archive snapshot
- Extracts the first `wikitable` and keeps the top 50 rows

**Transform**
- Cleans `Year` into an integer
- Prints a filtered subset of films released in the 2000s (2000–2009)

**Load**
- Writes the top 50 dataset to `top_50_films.csv`
- Loads the top 50 dataset into SQLite (`Movies.db`) as table `Top_50`
- Logs progress to `etl_log.txt`

## How to run
From this folder:

```bash
python main.py