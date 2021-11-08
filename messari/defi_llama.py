# Global imports
from string import Template
from typing import Union, List, Dict
from numpy import nan as Nan
import requests
import datetime
import pandas as pd

# Local imports
from messari.utils import validate_input, retrieve_data

# URLS
DL_PROTOCOLS_URL = "https://api.llama.fi/protocols"

# TODO handle requests error, ie wrong url

def get_defi_llama_protocols() -> pd.DataFrame:
    """Returns basic information on all listed protocols, their current TVL and the changes to it in the last hour/day/week"""
    protocols = requests.get(DL_PROTOCOLS_URL).json()

    protocol_dict={}
    for protocol in protocols:
        protocol_dict[protocol["slug"]] = protocol

    protocols_df = pd.DataFrame(protocol_dict)
    return protocols_df

##########################
# SETUP
##########################
dl_get_chain_chart_url = "https://api.llama.fi/charts/"

def get_defi_llama_slugs() -> List[str]:
    protocols = requests.get(DL_PROTOCOLS_URL).json()
    #protocols = retrieve_data(DL_PROTOCOLS_URL, {}, {})

    dl_slugs=[]
    for protocol in protocols:
        if "slug" in protocol.keys():
            dl_slugs.append(protocol["slug"])
    return dl_slugs
DL_SLUGS = get_defi_llama_slugs()
#print(DL_SLUGS)

DATETIME_FORMAT = "%Y-%m-%d"
def validate_datetime(datetime_input: Union[str, datetime.datetime]) -> Union[datetime.datetime, None]:
    if isinstance(datetime_input, str):
        # NOTE Chosing to return just date component of datetime.datetime
        return datetime.datetime.strptime(datetime_input, DATETIME_FORMAT).date()
    elif isinstance(datetime_input, datetime.datetime):
        # NOTE Chosing to return just date component of datetime.datetime
        return datetime_input.date()
    else:
        raise ValueError("Input should be of type string 'YYYY-MM-DD' or datetime.datetime")

#print(validate_datetime("2021-10-01"))

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

    # TODO: taxonimy translations
    # messari->dl
    # 

    supported_slugs = []
    for slug in slugs:
        if slug in DL_SLUGS:
            supported_slugs.append(slug)
        else:
            print(f"WARNING: slug '{slug}' not supported by DeFi Llama")


    return supported_slugs


##########################
# HELPERS
##########################
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

def time_filter_df(df_in: pd.DataFrame, start_date: str=None, end_date: str=None, sort=True) -> pd.DataFrame:

    filtered_df = df_in
    if start_date:
        start = validate_datetime(start_date)
        filtered_df = filtered_df[start:]
        pass

    if end_date:
        end = validate_datetime(end_date)
        filtered_df = filtered_df[:end]
        pass

    # Sort ascending
    if sort:
        filtered_df.sort_index(inplace=True)

    return filtered_df


##########################
# API Wrappers
##########################
def get_protocol_tvl_timeseries(asset_slugs: Union[str, List], start_date: Union[str, datetime.datetime]=None, end_date: Union[str, datetime.datetime]=None) -> pd.DataFrame:
    """Returns historical data on the TVL of a protocol along with some basic data on it. The fields `tokensInUsd` and `tokens` are only available for some protocols

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin)

    Returns
    -------
        DataFrame
            pandas DataFrame of protocol TVL
    """
    slugs = validate_dl_input(asset_slugs)

    protocols_dict={}
    slug_df_list=[]
    for slug in slugs:
        dl_get_protocol_url = f"https://api.llama.fi/protocol/{slug}"
        protocol = requests.get(dl_get_protocol_url).json()


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

        #print(pd.DataFrame(chainTvls))

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

#slugs = ["curve", "uniswap"]
#tt = get_defi_llama_protocol(slugs)
#print(tt)

def get_global_tvl_timeseries(start_date: Union[str, datetime.datetime]=None, end_date: Union[str, datetime.datetime]=None) -> pd.DataFrame:
    """Returns timeseries TVL from total of all Defi Llama supported protocols

    Returns
    -------
        DataFrame
            DataFrame containing timeseries tvl data for every protocol
    """
    charts = requests.get(dl_get_chain_chart_url).json()
    df = pd.DataFrame(charts)
    df.set_index(f'date', inplace=True)
    df.index = pd.to_datetime(df.index, unit='s', origin='unix')
    df = time_filter_df(df, start_date=start_date, end_date=end_date)
    return df

#print(get_defi_llama_charts())

def get_chain_tvl_timeseries(chains_in: Union[str, List], start_date: Union[str, datetime.datetime]=None, end_date: Union[str, datetime.datetime]=None) -> pd.DataFrame:
    """Retrive timeseries TVL for a given chain

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin)

    Returns
    -------
        DataFrame
            DataFrame containing timeseries tvl data for each chain
    """
    chains = validate_input(chains_in)

    chain_df_list=[]
    for chain in chains:
        dl_get_charts_url = f"https://api.llama.fi/charts/{chain}"
        response = requests.get(dl_get_charts_url).json()
        chain_df = pd.DataFrame(response)
        chain_df = format_df(chain_df)
        chain_df_list.append(chain_df)

    # Join DataFrames from each chain & return
    chains_df = pd.concat(chain_df_list, keys=chains, axis=1)
    chains_df = time_filter_df(chains_df, start_date=start_date, end_date=end_date) 
    return chains_df

#def get_chain_tvl(chains_in: Union[str, List], start_date: Union[str, datetime.datetime]=None, end_date: Union[str, datetime.datetime]) -> pd.

def get_current_tvl(asset_slugs: Union[str, List]) -> Dict:
    """Retrive current protocol tvl for an asset

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin)

    Returns
    -------
        Dict
            dictionary with tvl for each slug {slug: tvl, ...}
    """
    slugs = validate_input(asset_slugs)

    tvl_dict={}
    for slug in slugs:
        endpoint_url = f"https://api.llama.fi/tvl/{slug}"
        tvl = requests.get(endpoint_url).json()
        if type(tvl) == float:
            tvl_dict[slug] = tvl
        else:
            print(f"ERROR: slug={slug}, MESSAGE: {tvl['message']}")
    return tvl_dict


