import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

def extract(url):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    response = requests.get(url).text
    data = BeautifulSoup(response, 'html.parser')
    table = data.find_all('tbody')
    rows = table[2].find_all('tr')
    df = pd.DataFrame(columns=['Country', 'GDP_USD_millions'])
    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0 and row.find('a') and '—' not in row.text:
            country = cols[0].text.strip()
            gdp = cols[2].text.strip()
            df1 = pd.DataFrame({'Country': [country], 'GDP_USD_millions': [gdp]})
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    df['GDP_USD_millions'] = df['GDP_USD_millions'].str.replace(',', '').astype(float)
    df['GDP_USD_millions'] = (df['GDP_USD_millions'] / 1000).round(2)
    df = df.rename(columns={'GDP_USD_millions': 'GDP_USD_billions'})
    return df

def load_to_csv(df, path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(path, index=False)

def load_to_db(df, sql_connection, table):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message, log_file):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d-%H:%M:%S'  # Year-Month-Day-Hour-Minute-Second 
    now = datetime.now()  # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file, "a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def main():
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    filename = 'Countries_by_GDP.csv'
    tablename = 'Countries_by_GDP'
    database_name = 'World_Economies.db'
    log_file = 'etl_project_log.txt'
    

    log_progress("ETL process started.", log_file)
    
    try:
        # Extract data
        log_progress("Extracting data from URL...", log_file)
        df = extract(url)

        # Transform data
        log_progress("Transforming data...", log_file)
        df = transform(df)

        # Load data to CSV
        log_progress("Saving data to CSV...", log_file)
        load_to_csv(df, filename)

        # Load data to SQLite database
        log_progress("Saving data to SQLite database...", log_file)
        sql_connection = sqlite3.connect(database_name)
        load_to_db(df, sql_connection, tablename)
        
        #Check for countries with gdp greater than or equal to 100 billion
        query_statement = f"SELECT * from {tablename} WHERE GDP_USD_billions >= 100"
        run_query(query_statement, sql_connection)
        
        sql_connection.close()

        log_progress("ETL process completed successfully.", log_file)

    except Exception as e:
        error_message = "Error occurred during ETL process: " + str(e)
        log_progress(error_message, log_file)

if __name__ == "__main__":
    main()
