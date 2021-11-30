.. Messari documentation master file, created by
   sphinx-quickstart on Sun Feb 21 13:13:45 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Messari's Documentation!
===================================

Messari provides a free API for crypto prices, market data metrics, on-chain metrics,
and qualitative information (asset profiles).

This documentation will provide the basic steps to start using messari's python library.

Installing messari
==================

The first step is installing messari. messari is a python project, so it can be installed like any
other python library. Every Operating System should have Python pre-installed,
so you should just have to run::

   $> pip install git+https://github.com/messari/messari-python-api.git

Authentication
==============

Once the python library is installed, the next step is to create an API key. To do this sign up for an
account at https://messari.io/ and navigate to https://messari.io/account/api to generate a key.

.. note::
   Without an API key, requests are rate limited to 20 requests per minute and 1000 requests
   per day. Users that create an account will have slightly higher limits of 30 requests per
   minute and 2000 requests per day. PRO users have the highest limit at 60 requests per minute
   up to a maximum of 4000 requests per day. Contact us at api@messsari.io if you need a higher limit.


Once you generate an API key, import messari then set your API key by running::

   # Import Messari API wrapper
   from messari.messari import Messari

   # Set up Messari instance
   MESSARI_API_KEY = 'add_your_api_key'
   messari = Messari(api_key=MESSARI_API_KEY)

   # Run a quick demo
   markets_df = messari.get_all_markets()
   markets_df.head()


.. note::
   We recommend using the library with `JupyterLab <https://jupyterlab.readthedocs.io/en/stable/>`_. Detailed
   installation instructions can be found `here <https://jupyterlab.readthedocs.io/en/stable/getting_started/installation.html>`_.

   Using ``conda``, you can install JupyterLab by running::

      conda install -c conda-forge jupyterlab

   With ``pip``, you can install it with::

      pip install jupyterlab

Usage
=====

Check out our examples `here <https://github.com/messari/messari-python-api/blob/master/examples/Messari%20API%20Tutorial.ipynb>`_ for detailed usage.

:ref:`Here<genindex>` is detailed description of all the available functions in the library.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   notebooks/Messari API Tutorial.ipynb
   notebooks/DeFiLlama API Tutorial.ipynb
   notebooks/TokenTerminal API Tutorial.ipynb


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
