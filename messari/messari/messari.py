"""This module is meant to contain the Messari class"""

from string import Template
from typing import Union, List, Dict
import pandas as pd

from messari.dataloader import DataLoader
from messari.utils import validate_input, convert_flatten, unpack_list_of_dicts
from .helpers import fields_payload, timeseries_to_dataframe

BASE_URL = 'https://data.messari.io/api/v1/assets'
BASE_URL_V1 = 'https://data.messari.io/api/v1/assets'
BASE_URL_V2 = 'https://data.messari.io/api/v2/assets'
BASE_URL_MARKETS = 'https://data.messari.io/api/v1/markets'


class Messari(DataLoader):
    """This class is a wrapper around the Messari API
    """
    def __init__(self, api_key=None):
        messari_api_key = {'x-messari-api-key': api_key}
        DataLoader.__init__(self, api_dict=messari_api_key, taxonomy_dict=None)
        # TODO, look into super() for __init__

    #######################
    # markets
    #######################
    def get_all_markets(self, page: int = 1, limit: int = 20, to_dataframe: bool = True) -> Union[
        List[Dict], pd.DataFrame]:
        """Get the list of all exchanges and pairs that our
        WebSocket-based market real-time market data API supports.

        Parameters
        ----------
            page: int
                Page number starting at 1. Increment value to paginate through results.
            limit: int
                Limit of assets to return. Default is 20, max value is 500.
            to_dataframe: bool
                Return data as DataFrame or list of dictionaries. Default is set to DataFrame.

        Returns
        -------
            list, DataFrame
                List of dictionaries or pandas DataFrame of markets indexed by exchange slug.
        """
        payload = {'page': page, 'limit': limit}
        response_data = self.get_response(BASE_URL_MARKETS, params=payload)
        if to_dataframe:
            return pd.DataFrame(response_data['data']).set_index('exchange_slug')
        return response_data['data']

    #######################
    # assets
    #######################
    def get_all_assets(self, page: int = 1, limit: int = 20, asset_fields: Union[str, List] = None,
                       asset_metric: str = None, asset_profile_metric: str = None,
                       to_dataframe: bool = None) -> Union[Dict, pd.DataFrame]:
        """Get the paginated list of all assets including metrics and profile.

        Data is return only in JSON format when an asset profile is provided due
        to the high number of text fields. The keys of the object are assets and
        the value is the associated asset data.

        The function can return a pandas DataFrame only when asset metric data is
        requested.

        Parameters
        ----------
            page: int
                Page number starting at 1. Increment value to paginate through results.
            limit: int
                Limit of assets to return. Default is 20, max value is 500.
            asset_fields: str, list
                Single filter string or list of fields to filter data.
                Available fields include:
                    - id
                    - name
                    - slug
                    - metrics
                    - profile
            asset_metric: str
                Single metric string to filter metric data.
                Available metrics include:
                    - market_data
                    - marketcap
                    - supply
                    - blockchain_stats_24_hours
                    - market_data_liquidity
                    - all_time_high
                    - cycle_low
                    - token_sale_stats
                    - staking_stats
                    - mining_stats
                    - developer_activity
                    - roi_data
                    - roi_by_year
                    - risk_metrics
                    - misc_data
                    - lend_rates
                    - borrow_rates
                    - loan_data
                    - reddit
                    - on_chain_data
                    - exchange_flows
                    - alert_messages
            asset_profile_metric: str
                Single profile metric string to filter profile data.
                Available metrics include:
                    - general
                    - contributors
                    - advisors
                    - investors
                    - ecosystem
                    - economics
                    - technology
                    - governance
                    - metadata
            to_dataframe: bool
                Return data as pandas DataFrame or JSON. Default is set to JSON.

        Returns
        -------
            dict, DataFrame
                Dictionary or pandas DataFrame of asset data.
        """
        payload = {'page': page, 'limit': limit}
        if asset_fields:
            payload['fields'] = fields_payload(asset_fields=asset_fields, asset_metric=asset_metric,
                                               asset_profile_metric=asset_profile_metric)
        # DataFrame returned if asset metric is provided or if metrics is the only asset field
        if to_dataframe:
            # DataFrame can't be returned because profile data has been requested.
            if asset_profile_metric:
                raise ValueError('Profile data can only be returned as JSON. '
                                 'Only asset metric data can be returned as DataFrame.')

            # DataFrame can be returned because only metrics has been requested
            if asset_metric and not asset_fields:
                asset_fields = ['metrics']
                payload['fields'] = fields_payload(asset_fields=asset_fields,
                                                   asset_metric=asset_metric)
            # DataFrame can be returned because only metrics has been requested
            elif asset_fields and all(elem == 'metrics' for elem in asset_fields):
                # If asset metric is supplied, filter data based on metric
                if asset_metric:
                    payload['fields'] = fields_payload(asset_fields=asset_fields,
                                                       asset_metric=asset_metric)
                # Else return all metrics
                else:
                    payload['fields'] = fields_payload(asset_fields=asset_fields)
            else:
                raise ValueError(
                    'Only asset metrics can be returned as DataFrame. Make sure only '
                    'metrics is specified in asset fields.')
            response_data = self.get_response(BASE_URL_V2, params=payload)
            response_data = unpack_list_of_dicts(response_data['data'])
            for key, value in response_data.items():
                response_data[key] = convert_flatten(value)
            return pd.DataFrame.from_dict(response_data, orient='index')
        response_data = self.get_response(BASE_URL_V2, params=payload)
        return unpack_list_of_dicts(response_data['data'])

    def get_asset(self, asset_slugs: Union[str, List], asset_fields: Union[str, List] = None,
                  to_dataframe: bool = True) -> \
            Union[Dict, pd.DataFrame]:
        """Get basic metadata for an asset.

        Parameters
        ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin).
        asset_fields: str, list
            Single filter string or list of fields to filter data.
            Available fields include:
                - id
                - symbol
                - name
                - slug
        to_dataframe: bool
            Return data as DataFrame or JSON. Default is set to DataFrame.

        Returns
        -------
        dict, DataFrame
            Dictionary or pandas DataFrame with asset metadata.
        """
        asset_slugs = validate_input(asset_slugs)
        payload = {}
        if asset_fields:
            payload['fields'] = fields_payload(asset_fields=asset_fields)
        base_url_template = Template(f'{BASE_URL_V1}/$asset_key')

        response_data = {}
        for asset in asset_slugs:
            url = base_url_template.substitute(asset_key=asset)
            response = self.get_response(url, params=payload)
            response_flat = convert_flatten(response['data'])
            response_data[asset] = response_flat

        if to_dataframe:
            return pd.DataFrame.from_dict(response_data, orient='index')
        return response_data

    def get_asset_profile(self, asset_slugs: Union[str, List],
                          asset_profile_metric: str = None) -> Dict:
        """Get all the qualitative information for an asset.

        Data is return only in JSON format due to high number of text fields. The keys of the object
        are assets and the value is the associated asset data.

        Parameters
        ----------
            asset_slugs: str, list
                Single asset slug string or list of asset slugs (i.e. bitcoin).
            asset_profile_metric: str
                Single profile metric string to filter profile data.
                Available metrics include:
                    - general
                    - contributors
                    - advisors
                    - investors
                    - ecosystem
                    - economics
                    - technology
                    - governance
                    - metadata

        Returns
        -------
            dict
                Dictionary with asset profile data
        """
        asset_slugs = validate_input(asset_slugs)
        payload = {}
        if asset_profile_metric:
            payload['fields'] = fields_payload(asset_fields='id',
                                               asset_profile_metric=asset_profile_metric)
        base_url_template = Template(f'{BASE_URL_V2}/$asset_key/profile')
        response_data = {}
        for asset in asset_slugs:
            url = base_url_template.substitute(asset_key=asset)
            response = self.get_response(url, params=payload)
            response_flat = convert_flatten(response['data'])
            response_data[asset] = response_flat
        return response_data

    def get_asset_metrics(self, asset_slugs: Union[str, List],
                          asset_metric: str = None, to_dataframe: bool = True) -> \
                          Union[Dict, pd.DataFrame]:
        """Get all the quantitative metrics for an asset.

        Parameters
        ----------
            asset_slugs: str, list
                Single asset slug string or list of asset slugs (i.e. bitcoin).
            asset_metric: str
                Single metric string to filter metric data.
                Available metrics include:
                    - market_data
                    - marketcap
                    - supply
                    - blockchain_stats_24_hours
                    - market_data_liquidity
                    - all_time_high
                    - cycle_low
                    - token_sale_stats
                    - staking_stats
                    - mining_stats
                    - developer_activity
                    - roi_data
                    - roi_by_year
                    - risk_metrics
                    - misc_data
                    - lend_rates
                    - borrow_rates
                    - loan_data
                    - reddit
                    - on_chain_data
                    - exchange_flows
                    - alert_messages
            to_dataframe: bool
                Return data as DataFrame or JSON. Default is set to DataFrame.

        Returns
        -------
            dict, DataFrame
                Dictionary or pandas DataFrame with asset metric data.
        """
        asset_slugs = validate_input(asset_slugs)
        payload = {}
        if asset_metric:
            # Using fields payload function will work once API is fixed.
            # See inconsistent API usage example note.
            # payload['fields'] = fields_payload(asset_fields='id', asset_metric=asset_metric)
            payload['fields'] = f'id,symbol,{asset_metric}'
        base_url_template = Template(f'{BASE_URL_V1}/$asset_key/metrics')
        response_data = {}
        for asset in asset_slugs:
            url = base_url_template.substitute(asset_key=asset)
            response = self.get_response(url, params=payload)
            response_flat = convert_flatten(response['data'])
            response_data[asset] = response_flat
        if to_dataframe:
            return pd.DataFrame.from_dict(response_data, orient='index')
        return response_data

    def get_asset_market_data(self, asset_slugs: Union[str, List],
                              to_dataframe: bool = True) -> Union[Dict, pd.DataFrame]:
        """Get the latest market data for an asset.

        Parameters
        ----------
            asset_slugs: str, list
                Single asset slug string or list of asset slugs (i.e. bitcoin).
            to_dataframe: bool
                Return data as DataFrame or JSON. Default is set to DataFrame.

        Returns
        -------
            dict, DataFrame
                Dictionary or pandas DataFrame with asset market data.
        """
        return self.get_asset_metrics(asset_slugs=asset_slugs,
                                      asset_metric='market_data', to_dataframe=to_dataframe)

    ##############################
    # timeseries
    ##############################
    def get_metric_timeseries(self, asset_slugs: Union[str, List], asset_metric: str,
                              start: str = None, end: str = None, interval: str = '1d',
                              to_dataframe: bool = True) -> Union[Dict, pd.DataFrame]:
        """Retrieve historical timeseries data for an asset.

        Parameters
        ----------
            asset_slugs: str, list
                Single asset slug string or list of asset slugs (i.e. bitcoin).
            asset_metric: str
                Single metric string to filter timeseries data.
                Available metrics include:

                    - blk.cnt
                        The sum count of blocks created each day.
                    - txn.fee.avg
                        The USD value of the mean fee per transaction.
                    - txn.cnt
                        The sum count of transactions that interval. Transactions represent a
                        bundle of intended actions to alter the ledger initiated by a user
                        (human or machine). Transactions are counted whether they execute or
                        not and whether they result in the transfer of native units.
                    - txn.tsfr.val.adj
                        The sum USD value of all native units transferred removing noise and
                        certain artifacts.
                    - sply.circ
                        The circulating supply acknowledges that tokens may be held by
                        projects/foundations which have no intent to sell down their positions,
                        but which have not locked up supply in a formal contract. Thus, circulating
                        supply does not include known project treasury holdings (which can be
                        significant). Note that an investor must carefully consider both liquid and
                        circulating supplies when evaluating an asset, and the two can vary
                        significantly. A risk of depending entirely on circulating supply is that
                        the number can change dramatically based on discretionary sales from project
                        treasuries.
                    - mcap.circ
                        The circulating marketcap is the price of the asset multiplied by the
                        circulating supply. If no price is found for an asset because no trades
                        occurred, the last price of the last trade is used. After 30 days with no
                        trades, a marketcap of 0 is reported until trading resumes.
                    - reddit.subscribers
                        The number of subscribers on the asset's primary subreddit
                    - iss.rate
                        The percentage of new native units (continuous) issued over that interval,
                        extrapolated to one year (i.e., multiplied by 365), and divided by the
                        current supply at the end of that interval. Also referred to as the annual
                        inflation rate.
                    - mcap.realized
                        The sum USD value based on the USD closing price on the day that a native
                        unit last moved (i.e., last transacted) for all native units.
                    - bitwise.volume
                        It is well known that many exchanges conduct wash trading practices in
                        order to inflate trading volume. They are incentivized to report inflated
                        volumes in order to attract traders. "Bitwise Volume" refers to the total
                        volume over 10 exchanges identified by Bitwise to be free of wash trading
                        activities. They tend to be regulated exchanges. However, that does not
                        necessarily mean that the volume reported by other exchanges is 100% wash
                        trades. As such, the Bitwise Volume underestimates the total volume.
                    - txn.tsfr.val.avg
                        The sum USD value of native units transferred divided by the count of
                        transfers (i.e., the mean "size" in USD of a transfer).
                    - act.addr.cnt
                        The sum count of unique addresses that were active in the network (either
                        as a recipient or originator of a ledger change) that interval. All parties
                        in a ledger change action (recipients and originators) are counted.
                        Individual addresses are not double-counted.
                    - fees.ntv
                        The sum of all fees paid to miners in native units.
                        Fees do not include new issuance.
                    - exch.flow.in.usd.incl
                        The sum USD value sent to exchanges that interval,
                        including exchange to exchange activity.
                    - blk.size.byte
                        The sum of the size (in bytes) of all blocks created each day.
                    - txn.tsfr.val.med
                        The median transfer value in US dollars.
                    - exch.flow.in.ntv.incl
                        The amount of the asset sent to exchanges that interval,
                        including exchange to exchange activity.
                    - exch.flow.out.usd
                        The sum USD value withdrawn from exchanges that interval,
                        excluding exchange to exchange activity.'
                    - txn.vol
                        The sum USD value of all native units transferred
                        (i.e., the aggregate size in USD of all transfers).
                    - fees
                        The sum USD value of all fees paid to miners that interval.
                        Fees do not include new issuance.
                    - exch.flow.out.ntv.incl
                        The amount of the asset withdrawn from exchanges that interval,
                        including exchange to exchange activity.
                    - exch.flow.out.usd.incl
                        The sum USD value withdrawn from exchanges that interval,
                        including exchange to exchange activity.
                    - txn.fee.med
                        The USD value of the median fee per transaction.
                    - min.rev.ntv
                        The sum of all miner revenue, which constitutes fees plus newly issued
                        native units.
                    - exch.sply.usd
                        The sum USD value of all native units held in hot or cold exchange wallets.
                    - diff.avg
                        The mean difficulty of finding a hash that meets the protocol-designated
                        requirement (i.e., the difficulty of finding a new block) that interval.
                        The requirement is unique to each applicable cryptocurrency protocol.
                    - daily.shp
                        The Sharpe ratio (performance of the asset compared to a "risk-free" asset)
                        over a window of time).
                    - txn.tsfr.cnt
                        The sum count of transfers that interval. Transfers represent movements
                        of native units from one ledger entity to another distinct ledger entity.
                        Only transfers that are the result of a transaction and that have a
                        positive (non-zero) value are counted.
                    - exch.flow.in.ntv
                        The amount of the asset sent to exchanges that interval,
                        excluding exchange to exchange activity.
                    - new.iss.usd
                        The sum USD value of new native units issued that interval. Only those
                        native units that are issued by a protocol-mandated continuous emission
                        schedule are included (i.e., units manually released from escrow or
                        otherwise disbursed are not included).
                    - mcap.dom
                        The marketcap dominance is the asset's percentage share of total crypto
                        circulating marketcap.
                    - daily.vol
                        The annualized standard-deviation of daily returns over a window of time.
                    - reddit.active.users
                        The number of active users on the asset's primary subreddit
                    - exch.sply
                        The sum of all native units held in hot or cold exchange wallets
                    - nvt.adj
                        The ratio of the network value (or market capitalization, current supply)
                        divided by the adjusted transfer value. Also referred to as NVT.
                    - exch.flow.out.ntv
                        The amount of the asset withdrawn from exchanges that interval,
                        excluding exchange to exchange activity.
                    - min.rev.usd
                        The sum USD value of all miner revenue, which constitutes fees plus newly
                        issued native units, represented as the US dollar amount earned if all
                        native units were sold at the closing price on the same day.
                    - bitwise.price
                        Volume weighted average price over Bitwise 10 exchanges.
                    - new.iss.ntv
                        The sum of new native units issued that interval. Only those native units
                        that are issued by a protocol-mandated continuous emission schedule are
                        included (i.e., units manually released from escrow or otherwise disbursed
                        are not included).
                    - blk.size.bytes.avg
                        The mean size (in bytes) of all blocks created.
                    - hashrate
                        The mean rate at which miners are solving hashes that interval.
                    - exch.flow.in.usd
                        The sum USD value sent to exchanges that interval,
                        excluding exchange to exchange activity.
                    - price
                        Volume weighted average price computed using Messari Methodology
                    - real.vol
                        It is well known that many exchanges conduct wash trading practices in order
                        to inflate trading volume. They are incentivized to report inflated volumes
                        in order to attract traders. "Real Volume" refers to the total volume on the
                        exchanges that we believe with high level of confidence are free of wash
                        trading activities. However, that does not necessarily mean that the volume
                        reported by other exchanges is 100% wash trades. As such, the Messari "Real
                        Volume" applies a penalty to these exchanges to discount the volume believed
                        to come from wash trading activity. For more information,
                        see our methodology page.

            start: str
                Starting date string for timeseries data. A default start date will
                provided if not specified.
            end: str
                Ending date string for timeseries data. A default end date will
                provided if not specified.
            interval: str
                Interval of timeseries data. Default value is set to 1d.

                For any given interval, at most 2016 points will be returned. For example,
                with interval=5m, the maximum range of the request is 2016 * 5 minutes = 7 days.
                With interval=1h, the maximum range is 2016 * 1 hour = 84 days.
                Exceeding the maximum range will result in an error,
                which can be solved by reducing the date range specified in the request.

                Anything under 1 day requires an enterprise subscription.
                Please email enterprise@messari.io for information.

                Interval options include:
                    - 1m
                    - 5m
                    - 15m
                    - 30m
                    - 1hr
                    - 1d
                    - 1w
            to_dataframe: bool
                Return data as DataFrame or JSON. Default is set to DataFrame.

        Returns
        -------
            dict, DataFrame
                Dictionary or pandas DataFrame of asset data.
        """
        asset_slugs = validate_input(asset_slugs)
        payload = {'interval': interval}
        if start:
            if not end:
                raise ValueError('End date must be provided')
            payload['start'] = start
            payload['end'] = end
        base_url_template = Template(f'{BASE_URL}/$asset_key/metrics/{asset_metric}/time-series')
        response_data = {}
        for asset in asset_slugs:
            url = base_url_template.substitute(asset_key=asset)
            response = self.get_response(url, params=payload)
            response_flat = convert_flatten(response['data'])
            response_data[asset] = response_flat
        if to_dataframe:
            timeseries_df = timeseries_to_dataframe(response_data)
            if asset_metric != 'price':
                col_name = timeseries_df.columns[0][1]
                timeseries_df = timeseries_df.xs(col_name, axis=1, level=1)
            return timeseries_df
        return response_data
