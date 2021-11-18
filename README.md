# Messari API
Messari provides a free API for crypto prices, market data metrics, on-chain metrics, and qualitative information (asset profiles).

This documentation will provide the basic steps to start using messari’s python library.

## Remote Install
To install the messari package remotely:

```
$> pip install git+https://github.com/messari/messari-python-api.git
```


## Local Install
To install the messari package from the source:
```
$> git clone https://github.com/messari/messari-python-api.git
$> cd messari
messari$> python -m pip install -r requirements.txt
messari$> python setup.py install
```

## Quickstart
For a quick demo, you can try the following:
```
$> python
>>> from messari.timeseries import get_metric_timeseries
>>> assets = ['btc', 'eth']
>>> metric = 'price'
>>> start = '2020-06-01'
>>> end = '2021-01-01'
>>> timeseries_df = get_metric_timeseries(asset_slugs=assets, asset_metric=metric, start=start, end=end)
>>> print(timeseries_df)
```

## Docs
To open the offical docs go [here](https://objective-lalande-8ec88b.netlify.app/).

Examples can be found in [this](https://github.com/messari/messari-python-api/blob/master/examples/Messari%20API%20Tutorial.ipynb) Jupyter Notebook. 
