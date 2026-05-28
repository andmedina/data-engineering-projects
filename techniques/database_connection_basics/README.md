# SQLite Database Access with Python

This lab demonstrates how to interact with a SQLite database using **Python** and **pandas**.

## Objectives

- Create/connect to a SQLite database
- Load data from a CSV file into a database table
- Execute SQL queries using Python
- Append new records to an existing table

## Technologies Used

- Python
- pandas
- sqlite3 (built-in Python module)

## Project Structure
connect_to_db/
├── main.py
├── INSTRUCTOR.csv
└── STAFF.db   (generated after running)

## How It Works

The script performs the following steps:

1. Loads instructor data from `INSTRUCTOR.csv`
2. Creates a SQLite database (`STAFF.db`)
3. Writes data into table `INSTRUCTOR`
4. Executes example SQL queries:
   - Select all rows
   - Select specific columns
   - Count records
5. Appends a new instructor row
6. Verifies updated row count

## Run the Script

```bash
python main.py