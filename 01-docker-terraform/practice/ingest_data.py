from time import time
import os
import argparse
import pandas as pd
from sqlalchemy import create_engine, table


def main(args):

    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table_name = args.table_name
    url = args.url

    csv_name = "output.csv"

    os.system(f"curl -L {url} -o {csv_name}.gz")

    os.system(f"gunzip -k {csv_name}.gz ")
    
    engine = create_engine(url=f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df = pd.read_csv(
        csv_name,
        nrows=1,     
        dtype={"store_and_fwd_flag": "string"},
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    )

    df.head(0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")


    df_iter = pd.read_csv(
        csv_name,
        dtype={"store_and_fwd_flag": "string"},
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        iterator=True, 
        chunksize=100000
    )


    for df in df_iter:
        t_start = time()

        df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

    print("All chunks inserted")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user, password, port, host, csv_url, database_name, table_name
    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="passworD name for postgres")
    parser.add_argument("--host", help="host name for postgres")
    parser.add_argument("--port", help="port name for postgres")
    parser.add_argument("--db", help="db name for postgres")
    parser.add_argument("--table-name", help="name of the table we will ingest the csv data")
    parser.add_argument("--url", help="url of the csv file")

    args = parser.parse_args()

    main(args)