from typing import List

from pandas import DataFrame

from messari.markets import get_all_markets


def test_get_all_markets_output_dictionary():
    """Test dictionary output of get all markets function"""
    assert isinstance(get_all_markets(to_dataframe=False), List)


def test_get_all_markets_output_dataframe():
    """Test dataframe output of get all markets function"""
    assert isinstance(get_all_markets(), DataFrame)
