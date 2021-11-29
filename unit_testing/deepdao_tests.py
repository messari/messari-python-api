import unittest
from messari.deepdao import DeepDAO
from typing import List
#import os
#import sys
import pandas as pd

class TestDeepDAO(unittest.TestCase):
    """This is a unit testing class for testing the DeepDAO class"""

    def test_init(self):
        """Test initializing DeepDAO class"""
        dd = DeepDAO()
        self.assertIsInstance(dd, DeepDAO)


    def test_get_organizations(self):
        """Test getting project ids"""
        dd = DeepDAO()
        organizations = dd.get_organizations()
        self.assertIsInstance(organizations, pd.DataFrame)

    # TODO, fix all below

    def test_get_all_protocol_data(self):
        """Test getting all protocold data"""
        dd = DeepDAO()
        all_protocol_data = tt.get_all_protocol_data()
        self.assertIsInstance(all_protocol_data, pd.DataFrame)

    def test_protocol_data(self):
        """Test getting protocol data"""
        dd = DeepDAO()
        protocols = ["uniswap"]
        start_date = "2021-10-01"
        end_date = "2021-10-10"
        protocol_data = tt.get_protocol_data(protocols, start_date=start_date, end_date=end_date)
        self.assertIsInstance(protocol_data, pd.DataFrame)

        protocols = ["uniswap", "compound"]
        protocols_data = tt.get_protocol_data(protocols, start_date=start_date, end_date=end_date)
        self.assertIsInstance(protocols_data, pd.DataFrame)
        self.assertIsInstance(protocols_data["compound"], pd.DataFrame)


    def test_historic_metric_data(self):
        """Test getting current protocol tvl"""
        dd = DeepDAO()
        metric =  "market_cap"
        start_date = "2021-10-01"
        end_date = "2021-10-10"
        protocols = ["uniswap", "compound"]
        historical_mktcap = tt.get_historical_metric_data(protocols, metric=metric,
                                                          start_date=start_date, end_date=end_date)
        self.assertIsInstance(historical_mktcap, pd.DataFrame)

if __name__ == "__main__":
    unittest.main()
