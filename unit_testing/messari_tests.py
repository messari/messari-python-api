import unittest
from messari.messari import Messari
from typing import Dict
import os
import sys
import pandas as pd
import time

API_KEY = os.getenv('MESSARI_API_KEY')
if API_KEY is None:
    print('Please define MESSARI_API_KEY in your runtime enviornment')
    sys.exit()

class TestDeFiLlama(unittest.TestCase):
    """This is a unit testing class for testing the Messari class"""

    def test_init(self):
        """Test initializing Messari class"""
        messari = Messari(API_KEY)
        self.assertIsInstance(messari, Messari)

    def test_get_all_assets(self):
        """Test get all assets"""
        messari = Messari(API_KEY)
        response_data = messari.get_all_assets()
        self.assertIsInstance(response_data, Dict)

        response_data_df = messari.get_all_assets(asset_fields=['metrics'], to_dataframe=True)
        self.assertIsInstance(response_data_df, pd.DataFrame)

        metric = 'mining_stats'
        response_data_df_market_data = messari.get_all_assets(asset_metric=metric,
                                                              to_dataframe=True)
        self.assertIsInstance(response_data_df_market_data, pd.DataFrame)

        #dfs = [] # list to hold metric DataFrames
        #for i in range(1, 5, 1):
        #    df = messari.get_all_assets(page=1, limit=500,
        #                                asset_metric='marketcap', to_dataframe=True)
        #    dfs.append(df)
        #merged_df = pd.concat(dfs)
        #print(f'Number of assets in DataFrame {len(merged_df)}')
        #self.assertIsInstance(merged_df, pd.DataFrame)
        #print('sleeping for 60 sec')
        #for i in range(20):
        #    print(f'sleep {i}/19')
        #    time.sleep(10)

    def test_get_asset(self):
        """Test get asset"""
        messari = Messari(API_KEY)

        assets = ['bitcoin', 'ethereum', 'tether']
        #asset_metadata = messari.get_asset(asset_slugs=assets)
        #asset_metadata.head()

        fields = ['id', 'name']
        #asset_metadata_filtered = messari.get_asset(asset_slugs=assets, asset_fields=fields)
        #asset_metadata_filtered.head()

    def test_get_asset_profile(self):
        """Test get asset profile"""
        messari = Messari(API_KEY)
        assets = ['bitcoin', 'ethereum', 'tether']
        asset_profile_data = messari.get_asset_profile(asset_slugs=assets)
        self.assertIsInstance(asset_profile_data, Dict)
        details = asset_profile_data['bitcoin']['profile_general_overview_project_details']
        self.assertIsInstance(details, str)
        asset = 'Uniswap'
        profile_metric = 'investors'
        #governance_data = messari.get_asset_profile(asset_slugs=asset,
        #                                            asset_profile_metric=profile_metric)
        #self.assertIsInstance(governance_data, Dict)

    def test_get_asset_metric(self):
        """Test get asset metirc"""
        messari = Messari(API_KEY)
        assets = ['bitcoin', 'ethereum', 'tether']
        asset_metric_df = messari.get_asset_metrics(asset_slugs=assets)
        self.assertIsInstance(asset_metric_df, pd.DataFrame)
        #metric = 'marketcap'
        #asset_metric_df_marketcap = messari.get_asset_metrics(asset_slugs=assets,
        #                                                      asset_metric=metric)
        #self.assertIsInstance(asset_metric_df_marketcap, pd.DataFrame)

    def test_get_asset_market_data(self):
        """Test get asset market date"""
        messari = Messari(API_KEY)
        assets = ['bitcoin', 'ethereum', 'tether']
        market_data = messari.get_asset_market_data(asset_slugs=assets)
        self.assertIsInstance(market_data, pd.DataFrame)

    def test_get_all_markets(self):
        """Test get all markets"""
        messari = Messari(API_KEY)
        markets_df = messari.get_all_markets()
        self.assertIsInstance(markets_df, pd.DataFrame)

    def test_get_metric_timeseries(self):
        """Test get metic timeseries"""
        messari = Messari(API_KEY)
        metric = 'price'
        start = '2020-06-01'
        end = '2021-01-01'
        assets = ['bitcoin', 'ethereum', 'tether']
        timeseries_df = messari.get_metric_timeseries(asset_slugs=assets,
                                                      asset_metric=metric, start=start, end=end)
        self.assertIsInstance(timeseries_df, pd.DataFrame)

if __name__ == '__main__':
    unittest.main()
