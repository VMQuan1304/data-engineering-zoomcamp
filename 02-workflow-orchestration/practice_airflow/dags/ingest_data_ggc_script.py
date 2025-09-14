import pandas as pd


def convert_csv_to_parquet_callable(csv_name, parquet_name):

    df = pd.read_csv(csv_name, index_col=False)

    print(f"Loaded {len(df)} rows from {csv_name}")

    df.to_parquet(parquet_name, engine='pyarrow', index=False)


# convert_csv_to_parquet_callable("output.csv", "output.parquet")