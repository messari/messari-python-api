"""This module is dedicated to helpers for the DeepDAO class"""


import pandas as pd

def unpack_dataframe_of_lists(df_in: pd.DataFrame) -> pd.DataFrame:
    """Unpacks a dataframe where all entries are list of dicts

    Parameters
    ----------
       df_in: pd.DataFrame
           input DataFrame

    Returns
    -------
       DataFrame
           formated pandas DataFrame
    """
    df_list=[]
    for column_name in df_in.columns:
        sub_df = df_in[column_name]
        tmp_df_list=[]
        for entry in sub_df:
            if isinstance(entry, list):
                tmp_df = pd.DataFrame(entry)
                tmp_df_list.append(tmp_df)
        reorg_df = pd.concat(tmp_df_list)
        reorg_df.reset_index(drop=True, inplace=True) # Reset indexes so there are no repeats
        df_list.append(reorg_df)
    df_out = pd.concat(df_list, keys=df_in.columns, axis=1)
    return df_out

def unpack_dataframe_of_dicts(df_in: pd.DataFrame) -> pd.DataFrame:
    """Unpacks a dataframe where all entries are dicts

    Parameters
    ----------
       df_in: pd.DataFrame
           input DataFrame

    Returns
    -------
       DataFrame
           formated pandas DataFrame
    """
    df_list=[]
    for column_name in df_in.columns:
        sub_df = df_in[column_name]
        tmp_series_list=[]
        for entry in sub_df:
            tmp_series = pd.Series(entry)
            tmp_series_list.append(tmp_series)
        reorg_df = pd.DataFrame(tmp_series_list)
        reorg_df.reset_index(drop=True, inplace=True) # Reset indexes so there are no repeats
        df_list.append(reorg_df)
    df_out = pd.concat(df_list, keys=df_in.columns, axis=1)
    return df_out
