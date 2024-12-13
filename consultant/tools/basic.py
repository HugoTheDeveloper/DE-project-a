import pandas as pd
import os


def normalize_path(*args):
    path = os.path.join(*args)
    return os.path.normpath(path)


def from_xlsx(session, path, **kwargs):
    path = normalize_path(path)
    df_pandas = pd.read_excel(path, **kwargs)
    return session.createDataFrame(df_pandas)
