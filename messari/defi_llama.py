# Global Imports
from string import Template
from typing import Union, List, Dict
from numpy import nan as Nan
import requests
import os
import json
import datetime
import pandas as pd

# Local Imports
from messari.utils import validate_input, retrieve_data, validate_datetime, time_filter_df

##########################
# SETUP
##########################
# DeFi Llama URLS
DL_PROTOCOLS_URL = "https://api.llama.fi/protocols"
DL_GLOBAL_TVL_URL = "https://api.llama.fi/charts/"
DL_CURRENT_PROTOCOL_TVL_URL = Template("https://api.llama.fi/tvl/$slug")
DL_CHAIN_TVL_URL = Template("https://api.llama.fi/charts/$chain")
DL_GET_PROTOCOL_TVL_URL = Template("https://api.llama.fi/protocol/$slug")
# TODO handle requests error, ie wrong url

##########################
# HELPERS
##########################
current_path = os.path.dirname(__file__)
if os.path.exists(os.path.join(current_path, "../messari_to_dl.json")): # this file is being called from an install
    json_path = os.path.join(current_path, "../messari_to_dl.json")
    messari_to_dl_dict = json.load(open(json_path, "r"))
elif os.path.exists(os.path.join(current_path, "json/messari_to_dl.json")): # this file is being called from the project dir
    json_path = os.path.join(current_path, "json/messari_to_dl.json")
    messari_to_dl_dict = json.load(open(json_path, "r"))
else: # Can't find .json mapping file, default to empty
    messari_to_dl_dict = {}

def validate_dl_input(asset_slugs: Union[str, List]) -> Union[List, None]:
    """Wrapper around messari.utils.validate_input, validate input & check if it's supported by DeFi Llama

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin)

    Returns
    -------
        List
            list of validated slugs
    """
    slugs = validate_input(asset_slugs)

    # Look through known Messari to DeFi Llama translations
    slugs2=[]
    for slug in slugs:
        if slug in messari_to_dl_dict.keys():
            slugs2.append(messari_to_dl_dict[slug])
        else:
            slugs2.append(slug)

    # NOTE: this can likely be a one liner using sets but i haven't figured that out w/ WARNING print yet
    supported_slugs = []
    for slug in slugs2:
        if slug in DL_SLUGS:
            supported_slugs.append(slug)
        else:
            print(f"WARNING: slug '{slug}' not supported by DeFi Llama")

    return supported_slugs

def format_df(df_in: pd.DataFrame) -> pd.DataFrame:
    """format a typica DF from DL, replace date & drop duplicates

    Parameters
    ----------
        df_in: pd.DataFrame
            input DataFrame

    Returns
    -------
        DataFrame
            formated pandas DataFrame
    """

    # set date to index
    df_new = df_in
    if 'date' in df_in.columns:
        df_new.set_index(f'date', inplace=True)
        df_new.index = pd.to_datetime(df_new.index, unit='s', origin='unix')
        df_new.index = df_new.index.date

    # drop duplicates
    # NOTE: sometimes DeFi Llama has duplicate dates, choosing to just keep the last
    # NOTE: Data for duplicates is not the same
    # TODO: Investigate which data should be kept (currently assuming last is more recent
    df_new = df_new[~df_new.index.duplicated(keep='last')]
    return df_new

##########################
# API Wrappers
##########################
def get_protocol_tvl_timeseries(asset_slugs: Union[str, List], start_date: Union[str, datetime.datetime]=None, end_date: Union[str, datetime.datetime]=None) -> pd.DataFrame:
    """Returns times TVL of a protocol with token amounts as a pandas DataFrame indexed by df[protocol][chain][asset].

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin)

        start_date: str, datetime.datetime
            Optional start date to set filter for tvl timeseries ("YYYY-MM-DD")

        end_date: str, datetime.datetime
            Optional end date to set filter for tvl timeseries ("YYYY-MM-DD")

    Returns
    -------
        DataFrame
            pandas DataFrame of protocol TVL, indexed by df[protocol][chain][asset]
            to look at total tvl across all chains, index with chain='all'
            to look at total tvl across all tokens of a chain, asset='totalLiquidityUSD'
            tokens can be indexed by native amount, asset='tokenName', or by USD amount, asset='tokenName_usd'
    """
    slugs = validate_dl_input(asset_slugs)

    protocols_dict={}
    slug_df_list=[]
    for slug in slugs:
        endpoint_url = DL_GET_PROTOCOL_TVL_URL.substitute(slug=slug)
        protocol = requests.get(endpoint_url).json()


        ###########################
        # This portion is basically grabbing tvl metrics on a per chain basis

        # TODO this is gonna be difficult
        chainTvls = protocol["chainTvls"]
        chains = protocol["chains"]
        chain_list = []
        chain_df_list = []
        for chain in chains:
            chain_list.append(chain)

            # get timeseries
            chainTvl = chainTvls[chain]["tvl"]
            chainTvl_tokens = chainTvls[chain]["tokens"]
            chainTvl_tokensInUsd = chainTvls[chain]["tokensInUsd"]

            # convert tokens & tokensInUsd
            for token in chainTvl_tokens:
                for key, value in token["tokens"].items():
                    token[key] = value
                token.pop("tokens", None)

            for token in chainTvl_tokensInUsd:
                for key, value in token["tokens"].items():
                    token[key] = value
                token.pop("tokens", None)

            # convert to df
            chainTvl_df = pd.DataFrame(chainTvl)
            chainTvl_tokens_df = pd.DataFrame(chainTvl_tokens)
            chainTvl_tokensInUsd_df = pd.DataFrame(chainTvl_tokensInUsd)

            # fix indexes
            chainTvl_df = format_df(chainTvl_df)
            chainTvl_tokens_df = format_df(chainTvl_tokens_df)
            chainTvl_tokensInUsd_df = format_df(chainTvl_tokensInUsd_df)
            chainTvl_tokensInUsd_df = chainTvl_tokensInUsd_df.add_suffix('_usd')


            # concat tokens and tokensInUsd
            joint_tokens_df = pd.concat([chainTvl_tokens_df, chainTvl_tokensInUsd_df], axis=1)
            # Join total chain TVL w/ token TVL
            chain_df = chainTvl_df.join(joint_tokens_df)
            chain_df_list.append(chain_df)


        ###########################
        # This portion is basically grabbing tvl metrics for all chains combined


        ######################################
        # Get protocol token balances

        ## tokens in native amount
        tokens = protocol["tokens"]
        for token in tokens:
            for key, value in token["tokens"].items():
                token[key] = value
            token.pop("tokens", None)
        tokens_df = pd.DataFrame(tokens)
        tokens_df = format_df(tokens_df)

        ## tokens in USD
        tokensInUsd = protocol["tokensInUsd"]
        for token in tokensInUsd:
            for key, value in token["tokens"].items():
                token[key] = value
            token.pop("tokens", None)
        tokensInUsd_df = pd.DataFrame(tokensInUsd)
        tokensInUsd_df = format_df(tokensInUsd_df)
        tokensInUsd_df = tokensInUsd_df.add_suffix('_usd')

        # Get total tvl across chains
        tvl = protocol["tvl"]
        total_tvl_df = pd.DataFrame(tvl)
        total_tvl_df = format_df(total_tvl_df)

        # Working
        joint_tokens_df = pd.concat([tokens_df, tokensInUsd_df], axis=1)
        total_df = total_tvl_df.join(joint_tokens_df)

        # Now create multi index
        chain_list.append("all")
        chain_df_list.append(total_df)

        slug_df = pd.concat(chain_df_list, keys=chain_list, axis=1)
        slug_df_list.append(slug_df)

    total_slugs_df = pd.concat(slug_df_list, keys=slugs, axis=1)
    total_slugs_df.sort_index(inplace=True)

    total_slugs_df = time_filter_df(total_slugs_df, start_date=start_date, end_date=end_date)
    return total_slugs_df

def get_global_tvl_timeseries(start_date: Union[str, datetime.datetime]=None, end_date: Union[str, datetime.datetime]=None) -> pd.DataFrame:
    """Returns timeseries TVL from total of all Defi Llama supported protocols

    Parameters
    ----------
        start_date: str, datetime.datetime
            Optional start date to set filter for tvl timeseries ("YYYY-MM-DD")

        end_date: str, datetime.datetime
            Optional end date to set filter for tvl timeseries ("YYYY-MM-DD")

    Returns
    -------
        DataFrame
            DataFrame containing timeseries tvl data for every protocol
    """
    global_tvl = requests.get(DL_GLOBAL_TVL_URL).json()
    global_tvl_df = pd.DataFrame(global_tvl)
    global_tvl_df = format_df(global_tvl_df)
    global_tvl_df = time_filter_df(global_tvl_df, start_date=start_date, end_date=end_date)
    return global_tvl_df

def get_chain_tvl_timeseries(chains_in: Union[str, List], start_date: Union[str, datetime.datetime]=None, end_date: Union[str, datetime.datetime]=None) -> pd.DataFrame:
    """Retrive timeseries TVL for a given chain

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin)

        start_date: str, datetime.datetime
            Optional start date to set filter for tvl timeseries ("YYYY-MM-DD")

        end_date: str, datetime.datetime
            Optional end date to set filter for tvl timeseries ("YYYY-MM-DD")

    Returns
    -------
        DataFrame
            DataFrame containing timeseries tvl data for each chain
    """
    chains = validate_input(chains_in)

    chain_df_list=[]
    for chain in chains:
        endpoint_url = DL_CHAIN_TVL_URL.substitute(chain=chain)
        response = requests.get(endpoint_url).json()
        chain_df = pd.DataFrame(response)
        chain_df = format_df(chain_df)
        chain_df_list.append(chain_df)

    # Join DataFrames from each chain & return
    chains_df = pd.concat(chain_df_list, axis=1)
    chains_df.columns = chains
    chains_df = time_filter_df(chains_df, start_date=start_date, end_date=end_date)
    return chains_df

def get_current_tvl(asset_slugs: Union[str, List]) -> Dict:
    """Retrive current protocol tvl for an asset

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin)

    Returns
    -------
        DataFrame
            Pandas Series for tvl indexed by each slug {slug: tvl, ...}
    """
    slugs = validate_input(asset_slugs)

    tvl_dict={}
    for slug in slugs:
        endpoint_url = DL_CURRENT_PROTOCOL_TVL_URL.substitute(slug=slug)
        tvl = requests.get(endpoint_url).json()
        if type(tvl) == float:
            tvl_dict[slug] = tvl
        else:
            print(f"ERROR: slug={slug}, MESSAGE: {tvl['message']}")

    tvl_series = pd.Series(tvl_dict)
    tvl_df = tvl_series.to_frame("tvl")
    return tvl_df

def get_defi_llama_protocols() -> pd.DataFrame:
    """Returns basic information on all listed protocols, their current TVL and the changes to it in the last hour/day/week

    Returns
    -------
        DataFrame
            DataFrame with one column per DeFi Llama supported protocol
    """
    protocols = requests.get(DL_PROTOCOLS_URL).json()

    protocol_dict={}
    for protocol in protocols:
        protocol_dict[protocol["slug"]] = protocol

    protocols_df = pd.DataFrame(protocol_dict)
    return protocols_df

##########################
# HELPERS
##########################
# annoyingly has to be included in this file?
def get_defi_llama_slugs() -> List[str]:
    protocols = requests.get(DL_PROTOCOLS_URL).json()
    tt = get_defi_llama_protocols()
    slugs_series = tt.loc['slug']
    slugs_list = slugs_series.tolist()
    return slugs_list

# Store DL_SLUGS for global access
DL_SLUGS = get_defi_llama_slugs()


