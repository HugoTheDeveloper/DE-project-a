import pandas as pd
import os


def normalize_path(*args):
    path = os.path.join(*args)
    return os.path.normpath(path)


def from_xlsx(session, path, **kwargs):
    path = normalize_path(path)
    df_pandas = pd.read_excel(path, **kwargs)
    return session.createDataFrame(df_pandas)


def get_jdbc_url_for_gp():
    GP_HOST = os.getenv('GP_HOST')
    GP_PORT = os.getenv('GP_PORT')
    GP_DB_NAME = os.getenv('GP_DB_NAME')
    return f"jdbc:postgresql://{GP_HOST}:{GP_PORT}/{GP_DB_NAME}"


def get_properies_for_gp():
    DB_USER = os.getenv('DB_USER')
    DB_PASS = os.getenv('DB_PASS')
    return {
        "user": DB_USER, 
        "password": DB_PASS,
        "driver": "org.postgresql.Driver"
    }