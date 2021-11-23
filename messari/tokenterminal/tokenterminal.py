"""This module is meant to contain the TokenTerminal class"""

from string import Template
import datetime
from typing import List, Union
import pandas as pd

from messari.dataloader import DataLoader
from messari.utils import get_taxonomy_dict, time_filter_df
from .helpers import response_to_df


BASE_URL = 'https://api.tokenterminal.com/v1/projects'

class TokenTerminal(DataLoader):
    """This class is a wrapper for the Token Terminal API
    """

    def __init__(self, api_key: str):
        tt_api_key = {'Authorization': f'Bearer {api_key}'}
        messari_to_tt_dict = get_taxonomy_dict('messari_to_tt.json')
        DataLoader.__init__(self, api_dict=tt_api_key, taxonomy_dict=messari_to_tt_dict)

    def get_project_ids(self):
        """
        Returns all the project ids available in Token Terminal

        Returns
        -------
            List
                List of token ids.
        """
        url = BASE_URL
        data = self.get_response(url, headers=self.api_dict)
        return [x['project_id'] for x in data]

    def get_all_protocol_data(self, to_dataframe=True):
        """
        Returns an overview of latest data for all projects, ranging from metadata
        such as launch dates, logos brand colors and Twitter followers to more
        fundamental metrics such as Revenue, GMV, TVL and P/S ratios.

        The data is updated every 10 minutes.

        Parameters
        ----------
            to_dataframe: bool
                Return data as pandas DataFrame or JSON. Default is set to JSON.
        Returns
        -------
            dict, DataFrame
                Dictionary or pandas DataFrame of asset data.
        """
        url = BASE_URL
        data = self.get_response(url, headers=self.api_dict)
        if to_dataframe:
            df = pd.DataFrame(data)
            df.set_index('project_id', inplace=True)
            df.index.name = None
            df_transposed = df.transpose()
            return df_transposed
        return data

    def get_protocol_data(self, protocol_ids: Union[str, List],
                          start_date: Union[str, datetime.datetime] = None,
                          end_date: Union[str, datetime.datetime] = None) -> pd.DataFrame:
        """
        Returns a time series of the latest data for a given project,
        ranging from metadata such as Twitter followers to more fundamental
        metrics such as Revenue, GMV, TVL and P/S ratios.

        Parameters
        ----------
           protocol_ids: str, list
                String of protocol ID
           start_date: str, datetime.datetime
               Optional start date to set filter for timeseries ("YYYY-MM-DD")
           end_date: str, datetime.datetime
               Optional end date to set filter for timeseries ("YYYY-MM-DD")
           to_dataframe: bool
                Return data as pandas DataFrame or JSON. Default is set to DataFrame.
        Returns
        -------
            dict, DataFrame
                Dictionary or pandas DataFrame of asset data.
        """
        protocols = self.translate(protocol_ids)

        df_list = []
        for protocol in protocols:
            url = f'{BASE_URL}/{protocol}/metrics'
            data = self.get_response(url, headers=self.api_dict)
            df = pd.DataFrame(data)
            df.set_index('datetime', inplace=True)
            df.index = pd.to_datetime(df.index, format='%Y-%m-%dT%H:%M:%S').date  # noqa
            df_list.append(df)

        final_df = pd.concat(df_list, keys=protocols, axis=1)
        final_df = time_filter_df(final_df, start_date=start_date, end_date=end_date)
        return final_df

    def get_historical_metric_data(self, protocol_ids: Union[str, List], metric: str,
                                   start_date: Union[str, datetime.datetime] = None,
                                   end_date: Union[str, datetime.datetime] = None) -> pd.DataFrame:
        """
        Returns the time series of a specified metric for a given list of project.

        Parameters
        ----------
            protocol_ids: str, list
                List of project IDs
            metric: str
                Single metric string to filter data.
                Available metrics include:
                    - price,
                    - market_cap
                    - market_cap_circulating
                    - market_cap_fully_diluted
                    - volume
                    - vol_mc
                    - pe
                    - ps
                    - tvl
                    - gmv
                    - revenue
                    - revenue_supply_side
                    - revenue_protocol
                    - token_incentives
           start_date: str, datetime.datetime
               Optional start date to set filter for timeseries ("YYYY-MM-DD")
           end_date: str, datetime.datetime
               Optional end date to set filter for timeseries ("YYYY-MM-DD")

        Returns
        -------
            DataFrame
                pandas DataFrame with asset metric data.
        """
        url_temp = Template(f'{BASE_URL}/$asset_key/metrics')
        metric_df = pd.DataFrame()
        ids = self.translate(protocol_ids)

        for protocol_id in ids:
            url = url_temp.substitute(asset_key=protocol_id)
            data = self.get_response(url, headers=self.api_dict)
            data_df = response_to_df(data)
            single_metric_df = data_df[metric].to_frame()
            single_metric_df.columns = [protocol_id]
            single_metric_df = time_filter_df(single_metric_df,
                                              start_date=start_date, end_date=end_date)
            metric_df = metric_df.join(single_metric_df, how='outer')
        return metric_df
