from pyspark.sql import SparkSession
import pandas as pd
import os


def normalize_path(path):
    path = os.path.join(path)
    return os.path.normpath(path)


def from_xlsx(session, path, **kwargs):
    path = normalize_path(path)
    df_pandas = pd.read_excel(path, **kwargs)
    return session.createDataFrame(df_pandas)

