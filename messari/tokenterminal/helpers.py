"""This module is dedicated to helpers for the TokenTerminal class"""


from typing import Dict
import pandas as pd


# Token Terminal API utility functions
# Need to add tests
# TODO, we don't really need this
def response_to_df(response: Dict):
    """
    Transforms Token Terminal's JSON response to pandas DataFrame

    :param resp: dict
        API JSON response
    :return: pandas DataFrame
    """
    df = pd.DataFrame(response)
    df.set_index('datetime', inplace=True)
    df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
    df.index = df.index.date
    return df
