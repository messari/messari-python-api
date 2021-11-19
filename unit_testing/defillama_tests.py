import unittest
from messari.defillama import DeFiLlama
import pandas as pd

class TestDeFiLlama(unittest.TestCase):
    """This is a unit testing class for testing the DeFiLlama class"""

    def test_init(self):
        """Test initializing DeFiLlama class"""
        dl = DeFiLlama()
        self.assertIsInstance(dl, DeFiLlama)

    def test_get_protocol_tvl(self):
        """Test getting protocol tvl"""
        dl = DeFiLlama()
        tvl = dl.get_protocol_tvl_timeseries(["aave", "compound"],
                                             start_date="2021-10-01", end_date="2021-10-10")
        self.assertIsInstance(tvl, pd.DataFrame)

    def test_global_tvl(self):
        """Test getting global tvl"""
        dl = DeFiLlama()
        global_tvl = dl.get_global_tvl_timeseries(start_date="2021-10-01", end_date="2021-10-10")
        self.assertIsInstance(global_tvl, pd.DataFrame)

    def test_chain_tvl(self):
        """Test getting chain tvl"""
        dl = DeFiLlama()
        chains = ["Avalanche", "Harmony", "Polygon"]
        chain_tvl = dl.get_chain_tvl_timeseries(chains, start_date="2021-10-01",
                                                end_date="2021-10-10")
        self.assertIsInstance(chain_tvl, pd.DataFrame)

    def test_current_tvl(self):
        """Test getting current protocol tvl"""
        dl = DeFiLlama()
        protocols = ["uniswap", "curve", "aave"]
        current_tvl = dl.get_current_tvl(protocols)
        self.assertIsInstance(current_tvl, pd.DataFrame)

    def test_get_protocols(self):
        """Test getting protocol info"""
        dl = DeFiLlama()
        protocols = dl.get_protocols()
        self.assertIsInstance(protocols, pd.DataFrame)

if __name__ == "__main__":
    unittest.main()
