import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    print(table_name, csv_file, execution_date)
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        engine.connect()
        print("Connection to the database was successful!")
    except Exception as e:
        print("Error connecting to the database:", e)
        return

    t_start = time()
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    t_end = time()
    print('inserted first 100k rows in', t_end - t_start, 'seconds')

    while True:
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break


        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

        t_end = time()
        print('inserted 100k rows in', t_end - t_start, 'seconds')
