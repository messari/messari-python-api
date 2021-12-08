"""This module is dedicated to helpers for the Messari class"""


import logging
from typing import Union, List, Dict
import pandas as pd

from messari.utils import validate_input, validate_asset_fields_list_order, find_and_update_asset_field


def fields_payload(asset_fields: Union[str, List],
                   asset_metric: str = None, asset_profile_metric: str = None):
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
        asset_fields = find_and_update_asset_field(asset_fields, 'profile',
                                                   '/'.join(['profile', asset_profile_metric]))

    return ','.join(asset_fields)


def timeseries_to_dataframe(response: Dict) -> pd.DataFrame:
    """Convert timeseries data to pandas dataframe

    :param response: dict
        Dictionary of asset time series data keyed by symbol
    :return: pandas dataframe
    """
    df_list, key_list = [], []
    for key, value in response.items():
        key_list.append(key)
        if isinstance(value['values'], list):
            df_columns=[f'{name}' for name in value['parameters_columns']]
            values_df = pd.DataFrame.from_records(value['values'], columns=df_columns)
            values_df.set_index('timestamp', inplace=True)
            values_df.index = pd.to_datetime(values_df.index, unit='ms', origin='unix')  # noqa
            df_list.append(values_df)
        else:
            logging.warning('Missing timeseries data for %s', key)
            continue
    # Create multindex DataFrame using list of dataframes & keys
    metric_data_df = pd.concat(df_list, keys=key_list, axis=1)
    return metric_data_df
