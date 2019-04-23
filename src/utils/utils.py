import pandas as pd
import os
import re


def parquet_dir_to_pandas_df(dir_path):
    # expects parquet file to end with .parquet extension, expects a single parquet file in directory
    p = re.compile(r'^.*parquet$')
    files = os.listdir(dir_path)
    df_list = []
    for file in files:
        if p.search(file):
            df = pd.read_parquet(path=os.path.join(dir_path, file), engine='fastparquet')
            df_list.append(df)
    return pd.concat(df_list)


def line_cnt(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1
