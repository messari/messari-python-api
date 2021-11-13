from typing import List, Union, Dict
from messari.utils import validate_input
#from messari.utils import retrieve_data
#from messari.utils import validate_datetime
#from messari.utils import time_filter_df
#from messari.utils import get_taxonomy_dict



class DataSource:
    #### init
    def __init__(self, api_dict: Dict, taxonomy_dict: Dict):
        self.api_dict = api_dict
        self.taxonomy_dict = taxonomy_dict
        self.session = requests.Session()

    #### setters
    def set_api_dict(self, api_dict: Dict) -> None:
        self.api_dict = api_dict

    def set_taxonomy_dict(self, taxonomy_dict: Dict) -> None:
        self.taxonomy_dict = taxonomy_dict

    #### functions
    def get_response(self, endpoint_url: str, params: Dict=None, headers: Dict=None) -> Dict:
        """ Gets response from endpoint and checks for HTTP errors when requesting data.

        :param url: str
            URL API string.
        :param payload: dict
            Dictionary of query parameters.
        :return: JSON with requested data
        :raises SystemError if HTTP error occurs
        """
        try:
            response = self.session.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            raise SystemError(e)

    #### used to translate taxonomy
    def translate(self, input_slugs: Union[str, List]) -> Union[List, None]:
        """Wrapper around messari.utils.validate_input, validate input & check if it's supported by DeFi Llama

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






