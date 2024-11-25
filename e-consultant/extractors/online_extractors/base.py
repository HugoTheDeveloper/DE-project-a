import requests


def try_to_get(host, port='', path_list=[], query_list=[], secured_connection=False):
    con_type = 'http' if not secured_connection else 'https'
    port = f':{port}' if port else ''
    path = '/'.join(path_list)
    query = '&'.join(query_list)
    prepared_url = f'{con_type}://{host}{port}/{path}?{query}'
    print(prepared_url)
    print(prepared_url == url)
    try:
        response = requests.get(prepared_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f'Raised exception {e.__class__.__name__}')
        return 
    return response.content