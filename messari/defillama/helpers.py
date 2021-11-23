"""This module is dedicated to helpers for the DeFiLlama class"""


import pandas as pd


def format_df(df_in: pd.DataFrame) -> pd.DataFrame:
    """format a typical DF from DL, replace date & drop duplicates

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
        df_new.set_index('date', inplace=True)
        df_new.index = pd.to_datetime(df_new.index, unit='s', origin='unix')
        df_new.index = df_new.index.date

    # drop duplicates
    # NOTE: sometimes DeFi Llama has duplicate dates, choosing to just keep the last
    # NOTE: Data for duplicates is not the same
    # TODO: Investigate which data should be kept (currently assuming last is more recent
    df_new = df_new[~df_new.index.duplicated(keep='last')]
    return df_new
