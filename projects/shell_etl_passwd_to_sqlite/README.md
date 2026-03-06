# Shell ETL Demo: /etc/passwd → SQLite

This project demonstrates a simple **ETL pipeline using classic Unix shell tools**.  
It extracts data from the system `/etc/passwd` file, transforms it into CSV format, and loads it into a SQLite database.

The project includes **two versions of the same pipeline**:

- **Notebook (`.ipynb`)** – step-by-step demonstration of the ETL process
- **Python script (`main.py`)** – automated version that runs the pipeline from the terminal

---

# ETL Pipeline

## Extract
Extract selected fields from `/etc/passwd`:

- username
- user ID
- home directory

Example command used:

```bash
cut -d ":" -f1,3,6 /etc/passwd