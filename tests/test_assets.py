from typing import Dict

import pytest
from pandas import DataFrame

from messari.assets import get_all_assets, get_asset_profile, get_asset_metrics, get_asset_market_data
from messari.assets import get_asset


# TODO parametrize get_asset_metrics with all metric to check for correct output


@pytest.fixture
def asset_data():
    asset_keys = ['bitcoin', 'ethereum']
    asset_fields = ['id', 'symbol', 'metrics']
    asset_metric = 'marketcap'
    asset_profile_metric = 'general'
    return asset_keys, asset_fields, asset_metric, asset_profile_metric


def test_get_all_assets_output():
    """Test dictionary output of get asset function"""
    assert isinstance(get_all_assets(), Dict)


def test_get_all_assets_output_dictionary():
    """Test asset keys exist in returned object"""
    assert 'bitcoin' in get_all_assets()
    assert 'ethereum' in get_all_assets()


def test_get_all_assets_df_true_multiple_asset_fields():
    """Test ValueError output when metrics is not the only asset field"""
    asset_fields = ['profile', 'metrics']
    with pytest.raises(ValueError):
        get_all_assets(asset_fields=asset_fields, to_dataframe=True)


def test_get_all_assets_df_true_multiple_asset_fields_and_metric():
    """Test ValueError output when given multiple asset fields and asset metric"""
    asset_fields = ['id', 'profile', 'metrics']
    asset_metric = 'marketcap'
    with pytest.raises(ValueError):
        get_all_assets(asset_fields=asset_fields, asset_metric=asset_metric, to_dataframe=True)


def test_get_all_assets_df_true_asset_field_and_asset_metric():
    """Test dataframe output when given asset fields and asset metric"""
    asset_fields = ['metrics']
    asset_metric = 'marketcap'
    assert (get_all_assets(asset_fields=asset_fields, asset_metric=asset_metric, to_dataframe=True), DataFrame)


def test_get_all_assets_df_true_with_asset_metric():
    """Test dataframe ouput when given only asset metric"""
    asset_metric = 'marketcap'
    assert isinstance(get_all_assets(asset_metric=asset_metric, to_dataframe=True), DataFrame)


def test_get_all_assets_df_true_with_asset_fields():
    """Test dataframe output when metrics is the only value of asset fields"""
    asset_fields = ['metrics']
    assert isinstance(get_all_assets(asset_fields=asset_fields, to_dataframe=True), DataFrame)


def test_get_all_assets_df_true_with_asset_profile_metric():
    """Test ValueError when given asset profile metric"""
    asset_profile_metric = 'general'
    asset_metric = 'marketcap'
    with pytest.raises(ValueError):
        get_all_assets(asset_metric=asset_metric, asset_profile_metric=asset_profile_metric, to_dataframe=True)


def test_get_all_assets_df_true_with_asset_fields_wrong_value():
    """Test ValueError when asset fields contains other values"""
    asset_fields = ['profile']
    with pytest.raises(ValueError):
        get_all_assets(asset_fields=asset_fields, to_dataframe=True)


def test_get_asset_dictionary_output(asset_data):
    """Test dictionary object output from get asset"""
    asset_fields = ['id', 'symbol']
    asset_keys, _, _, _ = asset_data
    assert isinstance(get_asset(asset_keys, asset_fields, to_dataframe=False), Dict)


def test_get_asset_dataframe_output(asset_data):
    """Test dataframe object output from get asset"""
    asset_fields = ['id', 'symbol']
    asset_keys, _, _, _ = asset_data
    assert isinstance(get_asset(asset_keys, asset_fields), DataFrame)


def test_get_asset_profile_dictionary_output(asset_data):
    """Test dictionary object output from get asset profile"""
    asset_keys, _, _, asset_profile_metric = asset_data
    assert isinstance(get_asset_profile(asset_keys, asset_profile_metric), Dict)


def test_get_asset_metric_dictionary_object(asset_data):
    """Test dictionary object output from get asset metrics"""
    asset_keys, _, asset_metric, _ = asset_data
    assert isinstance(get_asset_metrics(asset_keys, asset_metric, to_dataframe=False), Dict)


def test_get_asset_metric_dataframe_object(asset_data):
    """Test dataframe object output from get asset metrics"""
    asset_keys, _, asset_metric, _ = asset_data
    assert isinstance(get_asset_metrics(asset_keys, asset_metric), DataFrame)


def test_get_asset_market_data_dictionary_object(asset_data):
    """Test dictionary object output from get asset market data"""
    asset_keys, _, _, _ = asset_data
    assert isinstance(get_asset_market_data(asset_keys, to_dataframe=False), Dict)


def test_get_asset_market_data_dataframe_object(asset_data):
    """Test dictionary object output from get asset market data"""
    asset_keys, _, _, _ = asset_data
    assert isinstance(get_asset_market_data(asset_keys), DataFrame)
