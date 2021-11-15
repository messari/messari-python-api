from collections.abc import MutableMapping
from string import Template
from typing import List, Union, Dict

import pandas as pd
import requests
import datetime
import os
import json
import logging

# Local imports
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

DATETIME_FORMAT = "%Y-%m-%d"
def validate_datetime(datetime_input: Union[str, datetime.datetime]) -> Union[datetime.datetime, None]:
    """Checks if input is datetime.datetime.

    :param datetime_input: str, datetime.datetime
        Single string "YYYY-MM-DD" or datetime.datetime
    :return datetime.datetime.
    :raises ValueError if input is neither a string (formatted correctly) or datetime.datetime
    """
    if isinstance(datetime_input, str):
        # NOTE Chosing to return just date component of datetime.datetime
        return datetime.datetime.strptime(datetime_input, DATETIME_FORMAT).date()
    elif isinstance(datetime_input, datetime.datetime):
        # NOTE Chosing to return just date component of datetime.datetime
        return datetime_input.date()
    else:
        raise ValueError("Input should be of type string 'YYYY-MM-DD' or datetime.datetime")

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

def time_filter_df(df_in: pd.DataFrame, start_date: str=None, end_date: str=None, sort=True) -> pd.DataFrame:
    """Convert filter timeseries indexed DataFrame

    :param start_date: str
        Optional starting date for filter
    :param end_date: str
        Optional end date for filter
    :param sort: bool
        Optionally override default sorting of output DataFrame
    :return: pandas DataFrame
    """

    filtered_df = df_in
    if start_date:
        start = validate_datetime(start_date)
        filtered_df = filtered_df[start:]
        pass

    if end_date:
        end = validate_datetime(end_date)
        filtered_df = filtered_df[:end]
        pass

    # Sort ascending
    if sort:
        filtered_df.sort_index(inplace=True)

    return filtered_df


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

def get_taxonomy_dict(filename: str) -> Dict:
    current_path = os.path.dirname(__file__)
    if os.path.exists(os.path.join(current_path, f"../{filename}")): # this file is being called from an install
        json_path = os.path.join(current_path, f"../{filename}")
        taxonomy_dict = json.load(open(json_path, "r"))
        # TODO check this below path
    elif os.path.exists(os.path.join(current_path, f"../json/{filename}")): # this file is being called from the project dir
        json_path = os.path.join(current_path, f"../json/{filename}")
        taxonomy_dict = json.load(open(json_path, "r"))
    else: # Can't find .json mapping file, default to empty
        taxonomy_dict = {}
    return taxonomy_dict
