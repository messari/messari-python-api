from typing import Union, List, Dict
from messari.utils import validate_input
from numpy import nan as Nan
import requests
import datetime
import pandas as pd


##########################
# DeFi Llama
def get_defi_llama_protocols() -> pd.DataFrame:
    """Returns basic information on all listed protocols, their current TVL and the changes to it in the last hour/day/week"""
    dl_get_protocols_url = "https://api.llama.fi/protocols"
    protocols = requests.get(dl_get_protocols_url).json()

    protocol_dict={}
    for protocol in protocols:
        protocol_dict[protocol["slug"]] = protocol

    protocols_df = pd.DataFrame(protocol_dict)
    return protocols_df

#print(get_defi_llama_protocols())

## NOTE Should probably go in __init__.py
## NOTE this could be used to screen slugs before using them
def get_defi_llama_slugs() -> List[str]:
    dl_get_protocols_url = "https://api.llama.fi/protocols"
    protocols = requests.get(dl_get_protocols_url).json()

    dl_slugs=[]
    for protocol in protocols:
        if "slug" in protocol.keys():
            dl_slugs.append(protocol["slug"])
    return dl_slugs
#DL_SLUGS = get_defi_llama_slugs()


def format_df(df_in: pd.DataFrame) -> pd.DataFrame:

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


def get_defi_llama_protocol(asset_slugs: Union[str, List]) -> pd.DataFrame:
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
    slugs = validate_input(asset_slugs)

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

    return total_slugs_df

#slugs = ["curve", "uniswap"]
#tt = get_defi_llama_protocol(slugs)
#print(tt)

def get_defi_llama_charts():
    """Returns historical values of the total sum of TVLs from all listed protocols"""
    dl_get_chain_chart_url = "https://api.llama.fi/charts/"
    charts = requests.get(dl_get_chain_chart_url).json()
    df = pd.DataFrame(charts)
    df.set_index(f'date', inplace=True)
    df.index = pd.to_datetime(df.index, unit='s', origin='unix')
    return df

#print(get_defi_llama_charts())

def get_defi_llama_chain_chart(chains_in: Union[str, List]) -> pd.DataFrame:
    """Retrive timeseries TVL for a given chain

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin)

    Returns
    -------
        DataFrame
            TODO
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
    return chains_df

chains=["ethereum", "avalanche"]
tt = get_defi_llama_chain_chart(chains)
print(tt)


# TODO handle requests error, ie wrong url
def get_defi_llama_protocol_tvl(asset_slugs: Union[str, List]) -> Dict:
    """Retrive current protocol tvl for an asset

    Parameters
    ----------
        asset_slugs: str, list
            Single asset slug string or list of asset slugs (i.e. bitcoin)

    Returns
    -------
        Dict
            dictionary {slug: tvl, ...}
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


