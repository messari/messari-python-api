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

class Solscan(DataLoader):
    """This class is a wrapper around the Solscan API
    """

    def __init__(self):
        DataLoader.__init__(self, api_dict=None, taxonomy_dict=None)

    #################
    # Block endpoints
    def get_last_block(self) -> pd.DataFrame:
        """Returns last 10 blocks
        Parameters
        ----------
        Returns
        -------
        """
        last_blocks = self.get_response(BLOCK_LAST_URL)
        last_blocks_df = pd.DataFrame(last_blocks)
        return last_blocks_df

    def get_block_transactions(self) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        response = self.get_response(BLOCK_TRANSACTIONS_URL)

    def get_block(self, blocks_in: Union[str, List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        blocks = validate_input(blocks_in)

        for block in blocks:
            endpoint_url = BLOCK_BLOCK_URL.substitute(block=block)
            response = self.get_response(endpoint_url)


    #######################
    # Transaction endpoints
    def get_last_transaction(self) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        respone = self.get_response(TRANSACTION_LAST_URL)

    def get_transaction(self, signatures_in: Union[str, List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        signatures = validate_input(signatures_in)

        for signature in signatures:
            endpoint_url = TRANSACTION_SIGNATURE_URL.substitute(signature=signature)
            respone = self.get_response(endpoint_url)

    ###################
    # Account endpoints
    def get_account_tokens(self) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        return

    def get_account_transactions(self) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        return

    def get_account_stake(self) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        return

    def get_account_spl_transactions(self) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        return

    def get_account_sol_transactions(self) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        return

    def get_account_export_transactions(self) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        return

    def get_account(self, accounts_in: Union[str, List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        accounts = validate_input(accounts_in)
        for account in accounts:
            endpoint_url = ACCOUNT_ACCOUNT_URL.substitute(account=account)
            response = self.get_response(endpoint_url)
        return

    #################
    # Token endpoints
    def get_token_holders(self, tokens_in: Union[str, List], offset: int=None, limit: int=None) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        # TODO
        return

    def get_token_meta(self, tokens_in: Union[str, List]) -> pd.DataFrame:
        """
        Parameters
        ----------
        Returns
        -------
        """
        # TODO
        return

    def get_token_info(self, sort_by: str=None, ascending: bool=True, limit: int=None, offset: int=None) -> pd.DataFrame:
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
            market_info = self.get_response(endpoint_url)
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
        chain_info = self.get_response(CHAIN_INFO_URL)
        return chain_info
