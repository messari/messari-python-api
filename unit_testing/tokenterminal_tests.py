import unittest
from messari.tokenterminal import TokenTerminal
from typing import List
import os
import sys
import pandas as pd

API_KEY= os.getenv("TOKEN_TERMINAL_API_KEY")
if API_KEY is None:
    print("Please define TOKEN_TERMINAL_API_KEY in your runtime enviornment")
    sys.exit()

class TestDeFiLlama(unittest.TestCase):
    """This is a unit testing class for testing the TokenTerminal class"""

    def test_init(self):
        """Test initializing TokenTerminal class"""
        tt = TokenTerminal(api_key=API_KEY)
        self.assertIsInstance(tt, TokenTerminal)

    def test_get_project_ids(self):
        """Test getting project ids"""
        tt = TokenTerminal(api_key=API_KEY)
        project_ids = tt.get_project_ids()
        self.assertIsInstance(project_ids, List)

    def test_get_all_protocol_data(self):
        """Test getting all protocold data"""
        tt = TokenTerminal(api_key=API_KEY)
        all_protocol_data = tt.get_all_protocol_data()
        self.assertIsInstance(all_protocol_data, pd.DataFrame)

    def test_protocol_data(self):
        """Test getting protocol data"""
        tt = TokenTerminal(api_key=API_KEY)
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
        tt = TokenTerminal(api_key=API_KEY)
        metric =  "market_cap"
        start_date = "2021-10-01"
        end_date = "2021-10-10"
        protocols = ["uniswap", "compound"]
        historical_mktcap = tt.get_historical_metric_data(protocols, metric=metric,
                                                          start_date=start_date, end_date=end_date)
        self.assertIsInstance(historical_mktcap, pd.DataFrame)

if __name__ == "__main__":
    unittest.main()
