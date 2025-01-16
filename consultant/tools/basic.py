# import pandas as pd
import os
import datetime
import requests


def normalize_path(*args):
    path = os.path.join(*args)
    return os.path.normpath(path)


# def from_xlsx(session, path, **kwargs):
#     path = normalize_path(path)
#     df_pandas = pd.read_excel(path, **kwargs)
#     return session.createDataFrame(df_pandas)


# connectors to external sources
# ______________________________________________________________________________________________

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


def get_hdfs_url(file_name, today=False, *dir_list):
    HOME_DIR = os.getenv('HOME_DIR')
    HDFS_HOST = os.getenv('HDFS_HOST')

    if today:
        name, extension = os.path.splitext(file_name)
        file_name = f"{name}_{datetime.date.today()}{extension}"
        
    path = [HOME_DIR, *dir_list, file_name]
    hdfs_path = normalize_path(*path)

    return f"hdfs://{HDFS_HOST}/{hdfs_path}"

# requests to API
# ______________________________________________________________________________________________

def fetch_json_from_server(host, port='', path_list=[], query_list=[], secured_connection=False):
    con_type = 'http' if not secured_connection else 'https'
    port = f':{port}' if port else ''
    path = '/'.join(path_list)
    query = '&'.join(query_list)
    prepared_url = f'{con_type}://{host}{port}/{path}?{query}'
    try:
        response = requests.get(prepared_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f'Raised exception {e.__class__.__name__}')
        return 
    return response.json()


def get_cbr_currency_rate_xml(date=""):
    prepared_query = f'date_req={date}' if date else ''
    return fetch_json_from_server('www.cbr.ru', query_list=[prepared_query],
                      path_list=['scripts', 'XML_daily.asp'], 
                      secured_connection=True)


def get_cbr_currency_rate_json(date=""):
    prepared_query = f'date_req={date}' if date else ''
    return fetch_json_from_server('www.cbr-xml-daily.ru', query_list=[prepared_query],
                      path_list=['daily_json.js'], 
                      secured_connection=True)


def get_moex_security_list():
    return fetch_json_from_server(
        host='iss.moex.com',
        path_list=['iss', 'securities.json'],
        secured_connection=True
    )
