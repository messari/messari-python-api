from typing import Dict

import pytest
from pandas import DataFrame

from messari.timeseries import get_metric_timeseries


# TODO parametrize get_asset_metrics with all metric to check for correct output

@pytest.fixture
def asset_data():
    asset_keys = ['BTC', 'ETH', 'UNI']
    asset_metric = 'price'
    return asset_keys, asset_metric


def test_get_metric_timeseries_dictionary_output(asset_data):
    """Test dictionary output from get metric timeseries"""
    asset_keys, asset_metric = asset_data
    assert isinstance(get_metric_timeseries(asset_keys, asset_metric, to_dataframe=False), Dict)


def test_get_metric_timeseries_dataframe_output(asset_data):
    """Test dictionary output from get metric timeseries"""
    asset_keys, asset_metric = asset_data
    assert isinstance(get_metric_timeseries(asset_keys, asset_metric), DataFrame)
