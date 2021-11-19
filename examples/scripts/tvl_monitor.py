from messari.defillama import DeFiLlama
import sys
from messari.utils import validate_input
import pandas as pd
import numpy as np
import math
import seaborn as sns
import matplotlib.pyplot as plt
import argparse

import warnings
warnings.filterwarnings('ignore')




def clean_tvl_data(multiindex_df):
    # identify chains
    chains = list(multiindex_df.columns.levels[1])
    # keep only USD columns
    protocol_name = multiindex_df.columns.levels[0][0]
    cols_to_remove = []
    for chain in chains:
        cols_to_drop = list(multiindex_df[protocol_name][chain].columns)
        cols_to_drop = [x for x in cols_to_drop if '_usd' not in x]
        cols_to_drop = [(chain, x) for x in cols_to_drop]
        cols_to_remove = cols_to_drop + cols_to_remove
    multiindex_df = multiindex_df[protocol_name].drop(cols_to_remove, axis=1)
    chain_dfs = []
    for chain in chains:
        chain_df = multiindex_df[chain]
        if len(chain_df.columns) > 9:
            # Calculate top 10 protocols by TVL and sum the rest as other
            current_tvl = chain_df.iloc[-1]
            current_tvl = current_tvl.to_frame(name='tvl')
            current_tvl.fillna(0, inplace=True)
            current_tvl['dom'] = (current_tvl['tvl'] / sum(current_tvl['tvl'])) * 100
            cols_to_keep = list(current_tvl.sort_values(by='dom', ascending=False).index[:9])
            cols_to_sum_as_other = [x for x in chain_df.columns if x not in cols_to_keep]
            chain_df['Other'] = chain_df[cols_to_sum_as_other].sum(axis=1)
            chain_df = chain_df[cols_to_keep + ['Other']]
            chain_df = chain_df.interpolate(method='linear', limit_direction='forward', axis=0)
            chain_df.columns = [x.split('_')[0] for x in list(chain_df.columns)]
            chain_dfs.append(chain_df)
        else:
            chain_df.columns = [x.split('_')[0] for x in list(chain_df.columns)]
            chain_df = chain_df.interpolate(method='linear', limit_direction='forward', axis=0)
            chain_dfs.append(chain_df)

    return pd.concat(chain_dfs, keys=chains, axis=1)

def billions(x, pos):
    """The two args are the value and tick position."""
    return '${:1.0f}B'.format(x*1e-9)

parser = argparse.ArgumentParser(description="A function built to give a detailed breakdown of a specific protocol's TVL")
parser.add_argument('-p', '--protocol', help='protocol to recieve TVL for')
parser.add_argument('-s', '--start', help='start date used to filter results')
parser.add_argument('-e', '--end', help='end date used to filter results')
arguments = parser.parse_args()

#### Get protocol
if arguments.protocol is not None:
    protocols = validate_input(arguments.protocol)
else:
    sys.exit()


#### Get start & end date
if arguments.start is not None:
    start = arguments.start
else:
    start = None
if arguments.end is not None:
    end = arguments.end
else:
    end = None

# Set fonts & colors
plt.rcParams.update({'font.size': 13})
colors = ['#233A4F', '#C767DD', '#CDE5BA', '#75D9E3', '#4A90FF',
          '#FF9400', '#75AAFD', '#FFA7A7', '#7030A0', '#FFC000']
sns.set_palette(sns.color_palette(colors))

# Get & clean TVL data from DeFiLlama
dl = DeFiLlama()
protocol_tvls = dl.get_protocol_tvl_timeseries(protocols, start_date=start, end_date=end)
df = clean_tvl_data(protocol_tvls)

protocol_name = protocols[0].capitalize()
total_subplots = len(list(df.columns.levels[0]))
cols = 2

Rows = total_subplots // cols
cols += total_subplots % cols

Position = range(1, total_subplots + 1)

fig = plt.figure(constrained_layout=True, figsize=(20, total_subplots*3))
fig.suptitle(f'{protocol_name} TVL Decomposition by Chain', fontsize=25)
for k, chain in enumerate(list(df.columns.levels[0])):
    # add every single subplot to the figure with a for loop
    sns.set_style('whitegrid')
    sns.despine()
    ax = fig.add_subplot(Rows, cols, Position[k])
    df[chain].plot(kind='area', linewidth=0, ax=ax)
    f = ax.get_figure()
    plt.legend(loc='lower center',
        ncol=4, fancybox=True, shadow=True)
    ax.yaxis.set_major_formatter(billions)
    idx = int(len(df[chain].index) / 2)
    total_current_tvl = df[chain].sum(axis=1).iloc[-1]
    ax.annotate('${:.2f}B'.format(total_current_tvl*1e-9),
                xy=(df[chain].index[idx], total_current_tvl + (total_current_tvl / 70)),
                fontsize='x-large', fontfamily='sans-serif', weight='bold', color='#244666')
    plt.axhline(y=df[chain].sum(axis=1).iloc[-1], color='#244666', linestyle='--', linewidth=2)
    ax.set_xlim([df[chain].index[0], df[chain].index[-1]])
    ax.set_xlabel('Date')
    if chain == 'all':
        ax.set_title('Aggregate TVL Decomposition')
    else:
        ax.set_title(f'{chain} TVL Decomposition')

plt.show()
