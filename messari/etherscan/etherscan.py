"""This module is meant to contain the Etherscan class"""


from messari.dataloader import DataLoader
from messari.utils import validate_input
from string import Template
from typing import Union, List, Dict
import pandas as pd


class Etherscan(DataLoader):
    """This class is a wrapper around the Etherscan API
    """

    def __init__(self):
        DataLoader.__init__(self, api_dict=None, taxonomy_dict=None)
