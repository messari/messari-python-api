from string import Template
from typing import List, Dict

import pytest
from pandas import DataFrame
from pandas._libs.tslibs.timestamps import Timestamp # noqa

from messari.utils import convert_flatten, unpack_list_of_dicts, validate_asset_fields_list_order, \
    find_and_update_asset_field, fields_payload, check_http_errors, timeseries_to_dataframe
from messari.utils import generate_urls
from messari.utils import retrieve_data
from messari.utils import validate_input


@pytest.fixture
def base_url():
    base_url = "https://data.messari.io/api/v1/assets"
    return Template('{}/$asset_key'.format(base_url))


@pytest.fixture
def asset_keys():
    asset_keys = ['BTC', 'ETH']
    return asset_keys


@pytest.fixture
def response_keys():
    return ['status', 'data']


@pytest.fixture
def response_keys():
    return ['status', 'data']


@pytest.fixture
def fields_data():
    asset_fields = ['id', 'symbol']
    metric = 'marketcap'
    profile_metric = 'general'
    return asset_fields, metric, profile_metric


@pytest.fixture
def list_of_dictionaries():
    test_dictionaries = [{'slug': 'bitcoin', 'data': '12345'},
                         {'slug': 'ethereum', 'data': '56789'}]
    return test_dictionaries


@pytest.fixture
def timeseries_test_data():
    test_data = {'BTC': {'values': [[1612702800000, 38853.613012806745, 39159.36109133666,
                                     38423.826881965455, 38673.37604354767, 377110650.0026134],
                                    [1612706400000, 38657.92960500524, 38727.79739631503,
                                     38203.828677655576, 38245.42836490441, 407334093.74781823],
                                    [1612710000000, 38317.37195213928, 38420.40368800111,
                                     37589.69716661886, 37945.58142427618, 696880128.0200763]],
                         'parameters_columns': ["timestamp", "open", "high", "low", "close", "volume"]},
                 'ETH': {'values': [[1612702800000, 1618.7466285386643, 1630.2596772919906,
                                     1581.2043124912527, 1587.2864078647651, 421478492.69593227],
                                    [1612706400000, 1589.9896737306258, 1593.4166752212038,
                                     1563.741845109996, 1572.52804973436, 443310698.3793288],
                                    [1612710000000, 1572.846538052633, 1578.2104530266033,
                                     1525.6938716626223, 1539.3280445092105, 666689350.3926613]],
                         'parameters_columns': ["timestamp", "open", "high", "low", "close", "volume"]}}
    return test_data


def test_convert_flatten():
    """Test convert flatten utility function"""
    test_data = {'a': 1,
                 'c': {'a': 2,
                       'b': {'x': 5,
                             'y': 10}},
                 'd': [1, 2, 3]}

    flattened_data = {'a': 1,
                      'c_a': 2,
                      'c_b_x': 5,
                      'c_b_y': 10,
                      'd': [1, 2, 3]}
    assert convert_flatten(test_data) == flattened_data


def test_generate_urls_output_type(base_url, asset_keys):
    """Test url generator utility function output type"""
    assert isinstance(generate_urls(base_url, asset_keys), List)


def test_generate_urls_output(base_url, asset_keys):
    """Test url generator utility function output"""
    assert generate_urls(base_url, asset_keys) == ['https://data.messari.io/api/v1/assets/BTC',
                                                   'https://data.messari.io/api/v1/assets/ETH']


def test_retrieve_data_output_type(base_url, asset_keys):
    """Test data retriever output type"""
    payload = {}
    assert isinstance(retrieve_data(base_url=base_url, payload=payload, asset_slugs=asset_keys), Dict)


def test_retrieve_data_http_errors(asset_keys):
    """Test HTTP error handler"""
    payload = {}
    bad_url = Template('https://data.messari.io/api/v1/ASSET_FIELDS/$asset_key')  # Bad URL request
    with pytest.raises(SystemError):
        retrieve_data(bad_url, payload, asset_keys)


def test_validate_input_string():
    """Test validate output when given string input"""
    asset_key = 'bitcoin'
    assert isinstance(validate_input(asset_key), List)


def test_validate_input_list(asset_keys):
    """Test validate output when given list input"""
    assert isinstance(validate_input(asset_keys), List)


@pytest.mark.parametrize('test_data', [set('BTC'), tuple('BTC')])
def test_validate_input_value_error(test_data):
    """Test ValueError when input is neither list or str"""
    with pytest.raises(ValueError):
        validate_input(test_data)


def test_unpack_list_of_dicts(list_of_dictionaries):
    """Test output of unpack list of dictionaries"""
    assert isinstance(unpack_list_of_dicts(list_of_dictionaries), Dict)


def test_unpack_list_of_dicts_keys(list_of_dictionaries):
    """Test keys of output object of unpack list of dictionaries"""
    assert 'bitcoin' in unpack_list_of_dicts(list_of_dictionaries)
    assert 'ethereum' in unpack_list_of_dicts(list_of_dictionaries)


def test_validate_asset_fields_list_order():
    """Test ordering of asset fields list"""
    unordered_list = ['id', 'metrics', 'symbol']
    assert validate_asset_fields_list_order(unordered_list, 'metrics')[-1] == 'metrics'


def test_validate_asset_fields_list_order_output():
    unordered_list = ['id', 'metrics', 'symbol']
    assert isinstance(validate_asset_fields_list_order(unordered_list, 'metrics'), List)


def test_find_and_update_asset_field_output():
    asset_fields = ['id', 'symbol', 'metrics']
    metric = 'metrics'
    updated_metric = 'metric/marketcap'
    assert isinstance(find_and_update_asset_field(asset_fields, metric, updated_metric), List)


def test_find_and_update_asset_field():
    """Test updating asset field with / when given asset metric or profile metric"""
    asset_fields = ['id', 'symbol', 'metrics']
    metric = 'metrics'
    updated_metric = 'metrics/marketcap'
    assert find_and_update_asset_field(asset_fields, metric, updated_metric) == ['id', 'symbol', 'metrics/marketcap']


def test_fields_payload_asset_fields(fields_data):
    """Test concatenation of asset fields list"""
    asset_fields, _, _ = fields_data
    assert fields_payload(asset_fields) == 'id,symbol,slug'


def test_fields_payload_asset_fields_and_asset_metric(fields_data):
    """Test concatenation of asset fields list and asset metric"""
    asset_fields, metric, _ = fields_data
    assert fields_payload(asset_fields, metric) == 'id,symbol,slug,metrics/marketcap'


def test_fields_payload_asset_fields_and_asset_profile_metric(fields_data):
    """Test concatenation of asset fields list and asset profile metric"""
    asset_fields, _, profile_metric = fields_data
    assert fields_payload(asset_fields, asset_profile_metric=profile_metric) == 'id,symbol,slug,profile/general'


def test_fields_payload_asset_fields_and_asset_metric_and_asset_profile_metric(fields_data):
    """Test concatenation of asset fields list and asset metric and profile metric"""
    asset_fields, metric, profile_metric = fields_data
    assert fields_payload(asset_fields, asset_metric=metric,
                          asset_profile_metric=profile_metric) == 'id,symbol,slug,metrics/marketcap,profile/general'


def test_check_http_errors():
    """Test HTTP error handler"""
    payload = {}
    bad_url = 'https://data.messari.io/api/v1/ASSET_FIELDS/$asset_key'  # Bad URL request
    with pytest.raises(SystemError):
        check_http_errors(bad_url, payload)


def test_timeseries_to_dataframe(timeseries_test_data):
    """Test timeseries data to dataframe"""
    timeseries_data = timeseries_test_data
    assert isinstance(timeseries_to_dataframe(timeseries_data), DataFrame)


def test_timeseries_to_dataframe_index(timeseries_test_data):
    """Test Timestap object in timeseries data index"""
    timeseries_data = timeseries_test_data
    timeseries_df = timeseries_to_dataframe(timeseries_data)
    assert isinstance(timeseries_df.index[0], Timestamp)
