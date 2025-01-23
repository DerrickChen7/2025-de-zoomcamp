import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url    
    csv_name = 'output.csv.gz'

    os.system(f'wget {url} -O {csv_name}')

    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        engine.connect()
        print("Connection to the database was successful!")
    except Exception as e:
        print("Error connecting to the database:", e)
        return


    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    while True:
        t_start = time()

        df = next(df_iter)

        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

        t_end = time()
        print('inserted 100k rows in', t_end - t_start, 'seconds')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to PostgreSQL')

    parser.add_argument('--user', required=True, help='Postgres username')
    parser.add_argument('--password', required=True, help='Postgres password')
    parser.add_argument('--host', required=True, help='Postgres host')
    parser.add_argument('--port', required=True, help='Postgres port')
    parser.add_argument('--db', required=True, help='Postgres database name')
    parser.add_argument('--table_name', required=True, help='Name of the table where data will be ingested')
    parser.add_argument('--url', required=True, help='URL of the CSV file')

    args = parser.parse_args()
    main(args)

