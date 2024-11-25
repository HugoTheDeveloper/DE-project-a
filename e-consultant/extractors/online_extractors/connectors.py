from base import try_to_get


def get_cbr_currency_rate(date):
    prepared_query = f'date_req={date}'
    return try_to_get('www.cbr.ru', query_list=[prepared_query],
                      path_list=['scripts', 'XML_daily.asp'], 
                      secured_connection=True)
