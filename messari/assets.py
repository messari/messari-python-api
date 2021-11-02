from string import Template
from typing import Dict
from typing import List
from typing import Union

import pandas as pd
from pandas import DataFrame

from messari.utils import fields_payload, convert_flatten
from messari.utils import check_http_errors
from messari.utils import retrieve_data
from messari.utils import unpack_list_of_dicts
from messari.utils import validate_input


BASE_URL_V1 = 'https://data.messari.io/api/v1/assets'
BASE_URL_V2 = 'https://data.messari.io/api/v2/assets'


def get_all_assets(page: int = 1, limit: int = 20, asset_fields: Union[str, List] = None,
                   asset_metric: str = None, asset_profile_metric: str = None,
                   to_dataframe: bool = None) -> Union[Dict, DataFrame]:
    """Get the paginated list of all assets including metrics and profile.

    Data is return only in JSON format when an asset profile is provided due to the high number
    of text fields. The keys of the object are assets and the value is the associated asset data.

    The function can return a pandas DataFrame only when asset metric data is requested.

    Parameters
    ----------
        page: int
            Page number starting at 1. Increment value to paginate through results.
        limit: int
            Limit of assets to return. Default is 20, max value is 500.
        asset_fields: str, list
            Single filter string or list of fields to filter data.
            Available fields include:
                - id
                - name
                - slug
                - metrics
                - profile
        asset_metric: str
            Single metric string to filter metric data.
            Available metrics include:
                - market_data
                - marketcap
                - supply
                - blockchain_stats_24_hours
                - market_data_liquidity
                - all_time_high
                - cycle_low
                - token_sale_stats
                - staking_stats
                - mining_stats
                - developer_activity
                - roi_data
                - roi_by_year
                - risk_metrics
                - misc_data
                - lend_rates
                - borrow_rates
                - loan_data
                - reddit
                - on_chain_data
                - exchange_flows
                - alert_messages
        asset_profile_metric: str
            Single profile metric string to filter profile data.
            Available metrics include:
                - general
                - contributors
                - advisors
                - investors
                - ecosystem
                - economics
                - technology
                - governance
                - metadata
        to_dataframe: bool
            Return data as pandas DataFrame or JSON. Default is set to JSON.

    Returns
    -------
        dict, DataFrame
            Dictionary or pandas DataFrame of asset data.
    """
    payload = {'page': page, 'limit': limit}
    if asset_fields:
        payload['fields'] = fields_payload(asset_fields=asset_fields, asset_metric=asset_metric,
                                           asset_profile_metric=asset_profile_metric)
    # DataFrame can only be returned if asset metric is provided or if metrics is the only asset field
    if to_dataframe:
        # DataFrame can't be returned because profile data has been requested.
        if asset_profile_metric:
            raise ValueError('Profile data can only be returned as JSON. '
                             'Only asset metric data can be returned as DataFrame.')

        # DataFrame can be returned because only metrics has been requested
        if asset_metric and not asset_fields:
            asset_fields = ['metrics']
            payload['fields'] = fields_payload(asset_fields=asset_fields, asset_metric=asset_metric)
        # DataFrame can be returned because only metrics has been requested
        elif asset_fields and all(elem == 'metrics' for elem in asset_fields):
            # If asset metric is supplied, filter data based on metric
            if asset_metric:
                payload['fields'] = fields_payload(asset_fields=asset_fields, asset_metric=asset_metric)
            # Else return all metrics
            else:
                payload['fields'] = fields_payload(asset_fields=asset_fields)
        else:
            raise ValueError('Only asset metrics can be returned as DataFrame. Make sure only metrics is specified in '
                             'asset fields.')
        response_data = check_http_errors(BASE_URL_V2, payload)
        response_data = unpack_list_of_dicts(response_data['data'])
        for key, value in response_data.items():
            response_data[key] = convert_flatten(value)
        return pd.DataFrame.from_dict(response_data, orient='index')
    response_data = check_http_errors(BASE_URL_V2, payload=payload)
    return unpack_list_of_dicts(response_data['data'])


def get_asset(asset_slugs: Union[str, List], asset_fields: Union[str, List] = None, to_dataframe: bool = True) -> \
        Union[Dict, DataFrame]:
    """Get basic metadata for an asset.

    Parameters
    ----------
    asset_slugs: str, list
        Single asset slug string or list of asset slugs (i.e. bitcoin).
    asset_fields: str, list
        Single filter string or list of fields to filter data.
        Available fields include:
            - id
            - symbol
            - name
            - slug
    to_dataframe: bool
        Return data as DataFrame or JSON. Default is set to DataFrame.

    Returns
    -------
    dict, DataFrame
        Dictionary or pandas DataFrame with asset metadata.
    """
    asset_slugs = validate_input(asset_slugs)
    payload = {}
    if asset_fields:
        payload['fields'] = fields_payload(asset_fields=asset_fields)
    base_url_template = Template(f'{BASE_URL_V1}/$asset_key')
    response_data = retrieve_data(base_url_template, payload, asset_slugs)
    if to_dataframe:
        return pd.DataFrame.from_dict(response_data, orient='index')
    return response_data


def get_asset_profile(asset_slugs: Union[str, List], asset_profile_metric: str = None) -> Dict:
    """Get all the qualitative information for an asset.

    Data is return only in JSON format due to high number of text fields. The keys of the object
    are assets and the value is the associated asset data.

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin).
        asset_profile_metric: str
            Single profile metric string to filter profile data.
            Available metrics include:
                - general
                - contributors
                - advisors
                - investors
                - ecosystem
                - economics
                - technology
                - governance
                - metadata

    Returns
    -------
        dict
            Dictionary with asset profile data
    """
    asset_slugs = validate_input(asset_slugs)
    payload = {}
    if asset_profile_metric:
        payload['fields'] = fields_payload(asset_fields='id', asset_profile_metric=asset_profile_metric)
    base_url_template = Template(f'{BASE_URL_V2}/$asset_key/profile')
    response_data = retrieve_data(base_url_template, payload, asset_slugs)
    return response_data


def get_asset_metrics(asset_slugs: Union[str, List], asset_metric: str = None, to_dataframe: bool = True) -> \
        Union[Dict, DataFrame]:
    """Get all the quantitative metrics for an asset.

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin).
        asset_metric: str
            Single metric string to filter metric data.
            Available metrics include:
                - market_data
                - marketcap
                - supply
                - blockchain_stats_24_hours
                - market_data_liquidity
                - all_time_high
                - cycle_low
                - token_sale_stats
                - staking_stats
                - mining_stats
                - developer_activity
                - roi_data
                - roi_by_year
                - risk_metrics
                - misc_data
                - lend_rates
                - borrow_rates
                - loan_data
                - reddit
                - on_chain_data
                - exchange_flows
                - alert_messages
        to_dataframe: bool
            Return data as DataFrame or JSON. Default is set to DataFrame.

    Returns
    -------
        dict, DataFrame
            Dictionary or pandas DataFrame with asset metric data.
    """
    asset_slugs = validate_input(asset_slugs)
    payload = {}
    if asset_metric:
        # Using fields payload function will work once API is fixed. See inconsistent API usage example note.
        # payload['fields'] = fields_payload(asset_fields='id', asset_metric=asset_metric)
        payload['fields'] = f'id,symbol,{asset_metric}'
    base_url_template = Template(f'{BASE_URL_V1}/$asset_key/metrics')
    response_data = retrieve_data(base_url_template, payload, asset_slugs)
    if to_dataframe:
        return pd.DataFrame.from_dict(response_data, orient='index')
    return response_data


def get_asset_market_data(asset_slugs: Union[str, List], to_dataframe: bool = True) -> Union[Dict, DataFrame]:
    """Get the latest market data for an asset.

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin).
        to_dataframe: bool
            Return data as DataFrame or JSON. Default is set to DataFrame.

    Returns
    -------
        dict, DataFrame
            Dictionary or pandas DataFrame with asset market data.
    """
    return get_asset_metrics(asset_slugs=asset_slugs, asset_metric='market_data', to_dataframe=to_dataframe)
