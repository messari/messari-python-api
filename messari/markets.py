from typing import Dict, Union, List

import pandas as pd
from pandas import DataFrame

from messari.utils import check_http_errors

BASE_URL = 'https://data.messari.io/api/v1/markets'


def get_all_markets(page: int = 1, limit: int = 20, to_dataframe: bool = True) -> Union[List[Dict], DataFrame]:
    """Get the list of all exchanges and pairs that our WebSocket-based market real-time market data API supports.

    Parameters
    ----------
        page: int
            Page number starting at 1. Increment value to paginate through results.
        limit: int
            Limit of assets to return. Default is 20, max value is 500.
        to_dataframe: bool
            Return data as DataFrame or list of dictionaries. Default is set to DataFrame.

    Returns
    -------
        list, DataFrame
            List of dictionaries or pandas DataFrame of markets indexed by exchange slug.
    """
    payload = {'page': page, 'limit': limit}
    response_data = check_http_errors(BASE_URL, payload=payload)
    if to_dataframe:
        return pd.DataFrame(response_data['data']).set_index('exchange_slug')
    return response_data['data']
