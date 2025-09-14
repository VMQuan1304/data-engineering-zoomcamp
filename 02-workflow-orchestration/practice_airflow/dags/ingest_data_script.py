
from time import time
import pandas as pd
from sqlalchemy.engine.create import create_engine


def ingest_func(user, password, host, port, db, table_name, csv_name):

    engine = create_engine(url=f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df = pd.read_csv(
        csv_name,
        nrows=1,     
        dtype={"store_and_fwd_flag": "string"},
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    )

    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

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

