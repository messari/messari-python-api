"""This module is meant to contain the DataLoader class"""


from typing import List, Union, Dict
import requests
from messari.utils import validate_input


class DataLoader:
    """This class is meant to represent a base wrapper around
    a variety of different API's used as data sources
    """
    def __init__(self, api_dict: Dict, taxonomy_dict: Dict):
        self.api_dict = api_dict
        self.taxonomy_dict = taxonomy_dict
        self.session = requests.Session()

    def __del__(self):
        self.session.close()


    def set_api_dict(self, api_dict: Dict) -> None:
        """Sets a new dictionary to be used as an API key pair

        :param api_dict: Dict
            New API dictionary
        """
        self.api_dict = api_dict

    def set_taxonomy_dict(self, taxonomy_dict: Dict) -> None:
        """Sets a new dictionary to be used for taxonomy translations

        :param taxonomy_dict: Dict
            New taxonomy dictionary
        """
        self.taxonomy_dict = taxonomy_dict

    def get_response(self, endpoint_url: str, params: Dict = None, headers: Dict = None) -> Dict:
        """Gets response from endpoint and checks for HTTP errors when requesting data.

        :param endpoint_url: str
            URL API string.
        :param params: dict
            Dictionary of query parameters.
        :param headers: str:
            Dictionary of headers
        :return: JSON with requested data
        :raises SystemError if HTTP error occurs
        """
        try:
            response = self.session.get(endpoint_url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            # NOTE if this doesn't work remove 'from e'
            raise SystemError(e) from e

    def translate(self, input_slugs: Union[str, List]) -> Union[List, None]:
        """Wrapper around messari.utils.validate_input,
        validate input & check if it's supported by DeFi Llama

        Parameters
        ----------
           input_slugs: str, list
               Single input slug string or list of input slugs (i.e. bitcoin)

        Returns
        -------
           List
               list of validated & translated slugs
        """
        slugs = validate_input(input_slugs)

        translated_slugs = []
        for slug in slugs:
            if slug in self.taxonomy_dict.keys():
                translated_slugs.append(self.taxonomy_dict[slug])
            else:
                translated_slugs.append(slug)
        return translated_slugs
