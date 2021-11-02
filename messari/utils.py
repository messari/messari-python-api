from collections.abc import MutableMapping
from string import Template
from typing import Dict
from typing import List, Union

import pandas as pd
import requests
from pandas import DataFrame

from messari import session

from messari import MESSARI_API_KEY

import logging

"""
Inconsistent API usage between metrics and profile end points

works: https://data.messari.io/api/v1/assets/BTC/metrics?fields=id,symbol,marketcap
doesn't work: https://data.messari.io/api/v1/assets/BTC/metrics?fields=id,symbol,metrics/markecap

works: https://data.messari.io/api/v2/assets/BTC/profile?fields=id,symbol,profile/general
doesn't work: https://data.messari.io/api/v2/assets/BTC/profile?fields=id,symbol,general
"""


def convert_flatten(response_json: Union[Dict, MutableMapping], parent_key: str = '', sep: str = '_') -> Dict:
    """Collapse JSON response to one single dictionary.

     :param response_json: dict, MutableMapping
        JSON response from API call.
     :param parent_key: str
        Key from original JSON
     :param sep: str
        Delimiter for new keys
     :return Collapsed JSON.ass
     """
    items = []
    for k, v in response_json.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(convert_flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def generate_urls(base_url: Template, asset_slugs: List) -> List:
    """Create list of urls given multiple asset_slugs.

    :param base_url: Template
        Template url string.
    :param asset_slugs: list
        Single asset slug string or list of asset slugs (i.e. BTC).
    :return List of string urls.
    """
    urls = [base_url.substitute(asset_key=asset_key) for asset_key in asset_slugs]
    return urls


def retrieve_data(base_url: Union[str, Template], payload: Dict, asset_slugs: List) -> Dict:
    """Retrieve data from API.

    :param base_url: str, Template
        Template url API string.
    :param payload: dict
        Dictionary of query parameters.
    :param asset_slugs: list
        Single asset slug string or list of asset slugs (i.e. BTC).
    :return Parsed JSON.
    """
    response_data = {}
    urls = generate_urls(base_url, asset_slugs)
    for url, asset in zip(urls, asset_slugs):
        response_json = check_http_errors(url, payload=payload)
        response_data[asset] = convert_flatten(response_json['data'])
    return response_data


def check_http_errors(url: str, payload: Dict) -> Union[Dict, None]:
    """ Checks for HTTP errors when requesting data.

    :param url: str
        URL API string.
    :param payload: dict
        Dictionary of query parameters.
    :return: JSON with requested data
    :raises SystemError if HTTP error occurs
    """
    try:
        response = session.get(url, params=payload, headers={
            'x-messari-api-key': MESSARI_API_KEY})  # API call is what makes the process slow
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        raise SystemError(e)


def validate_input(asset_input: Union[str, List]):
    """Checks if input is list.

    :param asset_input: str, list
        Single asset slug string or list of asset slugs (i.e. BTC).
    :return List of asset slugs.
    :raises ValueError if input is neither a string or list
    """
    if isinstance(asset_input, str):
        return [asset_input]
    elif isinstance(asset_input, list):
        return asset_input
    else:
        raise ValueError('Input should be of type string or list')


def unpack_list_of_dicts(list_of_dicts: List) -> Dict:
    """Unpack list of dictionaries to dictionary of dictionaries.

    The keys of the object are assets and the value is the associated asset data.

    :param list_of_dicts: list
        List of python dictionaries
    :return Dictionary of dictionaries
    """
    return {asset_data['slug']: asset_data for asset_data in list_of_dicts}


def validate_asset_fields_list_order(asset_fields: List, field: str) -> List:
    """Validates fields list order is arranged correctly when constructing url.

    :param asset_fields: list
        List of asset fields
    :param field: str
        String of asset field
    :return Arranged list of metrics
    """
    if asset_fields[-1] == field:
        return asset_fields
    else:
        asset_fields.append(asset_fields.pop(asset_fields.index(field)))
        return asset_fields


def find_and_update_asset_field(asset_fields: List, field: str, updated_field: str) -> List:
    """Find a updates fields list to concatenate drill metric or profile drill down in url.

    :param asset_fields: list
        List of asset fields.
    :param field: str
        String of asset field to replace.
    :param updated_field: str
        String to update asset field.
    :return Updated asset field list.
    """
    field_idx = asset_fields.index(field)
    asset_fields[field_idx] = updated_field
    return asset_fields


def fields_payload(asset_fields: Union[str, List], asset_metric: str = None, asset_profile_metric: str = None):
    """Returns payload with fields parameter.

    :param asset_fields: str, list
        List of asset fields.
    :param asset_metric: str
        Single metric string to filter metric data.
    :param asset_profile_metric: str
        Single profile metric string to filter profile data.
    :return String of fields query parameter.
    """
    asset_fields = validate_input(asset_fields)
    if 'slug' not in asset_fields:
        asset_fields = asset_fields + ['slug']
    if asset_metric:
        if 'metrics' not in asset_fields:
            asset_fields = asset_fields + ['metrics']
        # Ensure that metric is the last value in asset fields to successfully concatenate url
        asset_fields = validate_asset_fields_list_order(asset_fields, 'metrics')
        # Update metric in asset fields to include drill down asset metric
        asset_fields = find_and_update_asset_field(asset_fields, 'metrics',
                                                   '/'.join(['metrics', asset_metric]))
    if asset_profile_metric:
        if 'profile' not in asset_fields:
            asset_fields = asset_fields + ['profile']
        # Ensure that metric is the last value in asset fields to successfully concatenate url
        asset_fields = validate_asset_fields_list_order(asset_fields, 'profile')
        # Update metric in asset fields to include drill down asset metric
        asset_fields = find_and_update_asset_field(asset_fields,
                                                   'profile', '/'.join(['profile', asset_profile_metric]))

    return ','.join(asset_fields)


def timeseries_to_dataframe(response: Dict) -> DataFrame:
    """Convert timeseries data to pandas dataframe

    :param response: dict
        Dictionary of asset time series data keyed by symbol
    :return: pandas dataframe
    """
    data_df = pd.DataFrame()
    df_list, key_list = [], []
    for key, value in response.items():
        data_df = pd.DataFrame()
        key_list.append(key)
        if isinstance(value['values'], list):
            values_df = pd.DataFrame.from_records(value['values'],
                                                  columns=[f'{name}' for name in value['parameters_columns']])
            values_df.set_index(f'timestamp', inplace=True)
            values_df.index = pd.to_datetime(values_df.index, unit='ms', origin='unix')  # noqa
            df_list.append(values_df)
        else:
            logging.warning(f'Missing timeseries data for {key}')
            continue
    # Create multindex DataFrame using list of dataframes & keys
    metric_data_df = pd.concat(df_list, keys=key_list, axis=1)
    return metric_data_df


# Token Terminal API utility functions
# Need to add tests
def token_terminal_request(url: str, api_key: str) -> Dict:
    """
    Function to make HTTPs requests to Token Terminal's API
    :param url: str
        Endpoint url
    :param api_key: str
        API key
    :return: Data in JSON format
    """
    key = api_key
    headers = {"Authorization": f"Bearer {key}"}
    r = requests.get(url, headers=headers)
    return r.json()


def response_to_df(resp):
    """
    Transforms Token Terminal's JSON response to pandas DataFrame

    :param resp: dict
        API JSON response
    :return: pandas DataFrame
    """
    df = pd.DataFrame(resp)
    df.set_index('datetime', inplace=True)
    df.index = pd.to_datetime(df.index, format='%Y-%m-%dT%H:%M:%S').date # noqa
    return df
