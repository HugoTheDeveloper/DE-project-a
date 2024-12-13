import requests
# import datetime


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

# def get_cbr_currency_rate_xml(date=""):
#     prepared_query = f'date_req={date}' if date else ''
#     return fetch_json_from_server('www.cbr.ru', query_list=[prepared_query],
#                       path_list=['scripts', 'XML_daily.asp'], 
#                       secured_connection=True)

def get_cbr_currency_rate_json(date=""):
    prepared_query = f'date_req={date}' if date else ''
    return fetch_json_from_server('www.cbr-xml-daily.ru', query_list=[prepared_query],
                      path_list=['daily_json.js'], 
                      secured_connection=True)

# def download_cbr_currency_to(path_to_dir, date=""):
#     currency = get_cbr_currency_rate(date)
#     today = datetime.date.today()
#     path = f"{path_to_dir}/cbr_currency_{today}.xml"
#     with open(path, 'wb') as f:
#         try:
#             f.write(currency)
#             print("Data was written to {path} successfully!")
#         except Exception as e:
#             print(f'Raised exception {e.__class__.__name__}')
