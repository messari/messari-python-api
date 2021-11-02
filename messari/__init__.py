import requests

MESSARI_API_KEY = ''


# callback function for pyxll excel plug-in
def pyxll_set_api(text, console):
    MESSARI_API_KEY = text


class APIKeyMissingError(Exception):
    pass


if MESSARI_API_KEY is None:
    raise APIKeyMissingError(
        "All methods require an API key. "
    )

session = requests.Session()
