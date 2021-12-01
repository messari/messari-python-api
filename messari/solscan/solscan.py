"""This module is meant to contain the Solscan class"""

from messari.dataloader import DataLoader
from messari.utils import validate_input
from string import Template
from typing import Union, List, Dict
import pandas as pd

#### Block
BLOCK_LAST_URL = "https://public-api.solscan.io/block/last"
BLOCK_TRANSACTIONS_URL = "https://public-api.solscan.io/block/transactions"
BLOCK_BLOCK_URL = Template("https://public-api.solscan.io/block/$block")
#### Transaction
TRANSACTION_LAST_URL = "https://public-api.solscan.io/transaction/last"
TRANSACTION_SIGNATURE_URL = Template("https://public-api.solscan.io/transaction/$signature")
#### Account
ACCOUNT_TOKENS_URL = "https://public-api.solscan.io/account/tokens"
ACCOUNT_TRANSACTIONS_URL = "https://public-api.solscan.io/account/transactions"
ACCOUNT_STAKE_URL = "https://public-api.solscan.io/account/stakeAccounts"
ACCOUNT_SPL_TXNS_URL = "https://public-api.solscan.io/account/splTransfers"
ACCOUNT_SOL_TXNS_URL = "https://public-api.solscan.io/account/solTransfers"
ACCOUNT_EXPORT_TXNS_URL = "https://public-api.solscan.io/account/exportTransactions"
ACCOUNT_ACCOUNT_URL = Template("https://public-api.solscan.io/account/$account")
#### Token
TOKEN_HOLDERS_URL = "https://public-api.solscan.io/token/holders"
TOKEN_META_URL = "https://public-api.solscan.io/token/meta"
TOKEN_LIST_URL = "https://public-api.solscan.io/token/list"
#### Market
MARKET_INFO_URL = Template("https://public-api.solscan.io/market/token/$tokenAddress")
#### Chain Information
CHAIN_INFO_URL = "https://public-api.solscan.io/chaininfo"

#TODO max this clean/ not hardcoded? look into how this works
HEADERS={'accept': 'application/json', 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}

class Solscan(DataLoader):
    """This class is a wrapper around the Solscan API
    """

    def __init__(self):
        DataLoader.__init__(self, api_dict=None, taxonomy_dict=None)

    #################
    # Block endpoints
    def get_last_block(self, num_blocks=1) -> pd.DataFrame:
        """Returns last 10 blocks
        Parameters
        ----------
        Returns
        -------
        """

        # Max value is 20 or API bricks
        limit=num_blocks if num_blocks < 21 else 20
        params = {"limit": limit}

        last_blocks = self.get_response(BLOCK_LAST_URL,
                                        params=params,
                                        headers=HEADERS)
        last_blocks_df = pd.DataFrame(last_blocks)

        # TODO, extract data from 'result'

        return last_blocks_df

    def get_block_transactions(self, blocks_in: Union[str, List],
                               offset=0, num_transactions=10) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        blocks = validate_input(blocks_in)

        df_list = []
        for block in blocks:
            params = {"block": block,
                      "offset": offset,
                      "limit": num_transactions}
            txns = self.get_response(BLOCK_TRANSACTIONS_URL,
                                         params=params,
                                         headers=HEADERS)
            txns_df = pd.DataFrame(txns)
            # TODO, unpack this
            df_list.append(txns_df)
        fin_df = pd.concat(df_list, keys=blocks, axis=1)

        return fin_df

    def get_block(self, blocks_in: Union[str, List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        blocks = validate_input(blocks_in)

        df_list = []
        for block in blocks:
            endpoint_url = BLOCK_BLOCK_URL.substitute(block=block)
            response = self.get_response(endpoint_url,
                                         headers=HEADERS)
            df = pd.DataFrame(response)
            df_list.append(df)
        fin_df = pd.concat(df_list, keys=blocks, axis=1)
        # TODO, unpack result
        return fin_df


    #######################
    # Transaction endpoints
    def get_last_transaction(self, num_transactions=10) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        # 20
        limit=num_transactions if num_transactions < 21 else 20
        params = {"limit": limit}
        response = self.get_response(TRANSACTION_LAST_URL,
                                    params=params,
                                    headers=HEADERS)
        df = pd.DataFrame(response)
        # TODO, unpack
        return df


    def get_transaction(self, signatures_in: Union[str, List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        signatures = validate_input(signatures_in)

        df_list = []
        for signature in signatures:
            endpoint_url = TRANSACTION_SIGNATURE_URL.substitute(signature=signature)
            response = self.get_response(endpoint_url,
                                        headers=HEADERS)
            df = pd.DataFrame(response)
            print(response)
            df_list.append(df)
        fin_df = pd.concat(df_list, keys=signatures, axis=1)
        return fin_df

    ###################
    # Account endpoints
    def get_account_tokens(self, accounts_in: Union[str, List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        accounts = validate_input(accounts_in)

        df_list=[]
        for account in accounts:
            params={"account":account}
            response = self.get_response(ACCOUNT_TOKENS_URL,
                                         params=params,
                                         headers=HEADERS)
            df = pd.DataFrame(response)
            df_list.append(df)
        fin_df = pd.concat(df_list, keys=accounts, axis=1)
        return fin_df

    def get_account_transactions(self, accounts_in: Union[str,List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        accounts = validate_input(accounts_in)

        df_list=[]
        for account in accounts:
            params={"account":account}
            response = self.get_response(ACCOUNT_TRANSACTIONS_URL,
                                         params=params,
                                         headers=HEADERS)
            df = pd.DataFrame(response)
            df_list.append(df)
        fin_df = pd.concat(df_list, keys=accounts, axis=1)
        return fin_df

    def get_account_stake(self, accounts_in: Union[str, List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        accounts = validate_input(accounts_in)

        df_list=[]
        for account in accounts:
            params={"account":account}
            response = self.get_response(ACCOUNT_STAKE_URL,
                                         params=params,
                                         headers=HEADERS)
            df = pd.DataFrame(response)
            df_list.append(df)
        fin_df = pd.concat(df_list, keys=accounts, axis=1)
        return fin_df

    def get_account_spl_transactions(self, accounts_in: Union[str, List], from_time: int=None, to_time: int=None, offset: int=0, limit: int=10) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        accounts = validate_input(accounts_in)

        df_list=[]
        for account in accounts:
            params={"account":account}
            response = self.get_response(ACCOUNT_SPL_TXNS_URL,
                                         params=params,
                                         headers=HEADERS)
            df = pd.DataFrame(response)
            df_list.append(df)
        fin_df = pd.concat(df_list, keys=accounts, axis=1)
        return fin_df

    def get_account_sol_transactions(self, accounts_in: Union[str, List], from_time: int=None, to_time: int=None, offset: int=0, limit: int=10) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        accounts = validate_input(accounts_in)

        df_list=[]
        for account in accounts:
            params={"account":account}
            response = self.get_response(ACCOUNT_SOL_TXNS_URL,
                                         params=params,
                                         headers=HEADERS)
            df = pd.DataFrame(response)
            df_list.append(df)
        fin_df = pd.concat(df_list, keys=accounts, axis=1)
        return fin_df

    def get_account_export_transactions(self, accounts_in: Union[str, List], type: str, from_time: int, to_time: int) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        accounts = validate_input(accounts_in)
        for account in accounts:
            params={}
            response = self.get_response(ACCOUNT_EXPORT_TXNS_URL,
                                         params=params,
                                         headers=HEADERS)


        return "do this"

    def get_account(self, accounts_in: Union[str, List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        accounts = validate_input(accounts_in)
        df_list=[]
        for account in accounts:
            endpoint_url = ACCOUNT_ACCOUNT_URL.substitute(account=account)
            response = self.get_response(endpoint_url)
            df = pd.DataFrame(response)
            df_list.append(df)
        fin_df = pd.concat(df_list, keys=accounts, axis=1)
        return fin_df

    #################
    # Token endpoints
    def get_token_holders(self, tokens_in: Union[str, List], offset: int=0, limit: int=10) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        tokens = validate_input(tokens_in)

        df_list = []
        for token in tokens:
            params={"tokenAddress": token,
                    "offset": offset,
                    "limit": limit}
            response = self.get_response(TOKEN_HOLDERS_URL,
                                         params=params,
                                         headers=HEADERS)
            df = pd.DataFrame(response)
            df_list.append(df)
        fin_df = pd.concat(df_list, keys=tokens, axis=1)
        # TODO, unpack result
        return fin_df

    def get_token_meta(self, tokens_in: Union[str, List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        tokens = validate_input(tokens_in)

        df_list = []
        for token in tokens:
            params={"tokenAddress": token}
            response = self.get_response(TOKEN_META_URL,
                                         params=params,
                                         headers=HEADERS)
            df = pd.DataFrame(response)
            df_list.append(df)
        fin_df = pd.concat(df_list, keys=tokens, axis=1)
        # TODO, unpack result
        return fin_df

    def get_token_list(self, sort_by: str=None, ascending: bool=True, limit: int=None, offset: int=None) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        # TODO
        return

    ##################
    # Market endpoints
    def get_market_info(self, tokens_in: Union[str, List]) -> pd.DataFrame:
        """Get market information of the given token
        Parameters
        ----------
        Returns
        -------
        """
        tokens = validate_input(tokens_in)
        # TODO, transalte name into address

        market_info_list = []
        for token in tokens:
            endpoint_url = MARKET_INFO_URL.substitute(tokenAddress=token)
            market_info = self.get_response(endpoint_url,
                                            headers=HEADERS)
            market_info_series = pd.Series(market_info)
            market_info_list.append(market_info_series)
        market_info_df = pd.concat(market_info_list, keys=tokens, axis=1)
        return market_info_df


    #############################
    # Chain Information endpoints
    def get_chain_info(self) -> Dict:
        """Return Blockchain overall information
        Parameters
        ----------
        Returns
        -------
        """
        chain_info = self.get_response(CHAIN_INFO_URL,
                                       headers=HEADERS)
        return chain_info
