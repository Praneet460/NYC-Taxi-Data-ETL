## import modules
from sqlalchemy import create_engine
import pandas as pd
# built-ins
from time import time
import argparse
import os

## main function
def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = params.csv_name + '.csv'
    iterations = int(params.iterations)

    # download csv file
    try:
        os.system(f"wget {url} -O {csv_name}")
    except Exception as e:
        print("CSV file download has failed.")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # load chunk data
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000,  compression='gzip')
    df = next(df_iter)
    
    # set timestamp datatype
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # CREATE/ REPLACE IF EXISTS TABLE WITH COLUMNS
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # INSERT ROWS IN CREATED TABLE
    df.to_sql(name=table_name, con=engine, if_exists='append')

    i = 0
    while i < iterations:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print("inserted another chunk..., took %.3f second" % (t_end - t_start))
        i = i+1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV Data to Postgres") 

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name to write data')
    parser.add_argument('--url', help='url of the csv file to fetch')
    parser.add_argument('--csv_name', help='csv_name of file to fetch')
    parser.add_argument('--iterations', help='number of data push iterations')

    args = parser.parse_args()

    main(args)
