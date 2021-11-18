###########################################################################
# The purpose of this is to update the messari taxonomy dictionaries. It is 
# not meant to be included as part of the messari python package, rather it 
# is meant to update mappings objects used by the messari python package for
# taxonomy translation across different API's. This design is intentionally 
# meant to just be run as a script, don't worry about wrapping anything 
# into functions.
###########################################################################
import json
import os

import messari
from messari.messari import Messari
from messari.tokenterminal import TokenTerminal
from messari.defillama import DeFiLlama

##########################
# Get all DeFi Llama slugs
##########################
dl = DeFiLlama()

protocols_df = dl.get_protocols()
slugs_series = protocols_df.loc['slug']
DL_SLUGS = slugs_series.tolist()

########################
# Get all messari assets
########################
m = Messari()

limit = 500
page = 1
messari_assets = m.get_all_assets(page=page, limit=limit)
current_len = len(messari_assets)

# NOTE: this will probably crash if one page len == limit and the next page len == 0
while current_len == limit:
    page += 1
    # NOTE: pushing up against messari rate limiting here, not sure how to handle user experience
    new_assets = m.get_all_assets(page=page, limit=limit)
    messari_assets.update(new_assets)
    current_len = len(new_assets)

#########################################
# Create Messari to DeFi Llama dictionary
#########################################
messari_to_dl_dict = {}
for asset in messari_assets:
    if asset in DL_SLUGS:
        # Messari is case insensitive & DeFi Llama is always lowercase so drop to lower()
        slug = str(messari_assets[asset]['slug']).lower()
        symbol = str(messari_assets[asset]['symbol']).lower()
        messari_to_dl_dict[slug] = asset
        messari_to_dl_dict[symbol] = asset

# Open hardcoded values and appened
with open("../messari/mappings/messari_to_dl_hardcode.json", "r") as infile:
    messari_to_dl_hardcode = json.load(infile)

for entry in messari_to_dl_hardcode:
    # If there is an overlap print about it to notify user
    if messari_to_dl_hardcode[entry] in messari_to_dl_dict.values():
        print(f"overlapping entry for {entry} using hardcoded value {messari_to_dl_hardcode[entry]}")

    messari_to_dl_dict[entry] = messari_to_dl_hardcode[entry]

with open("../messari/mappings/messari_to_dl.json", "w") as outfile:
    json.dump(messari_to_dl_dict, outfile)

# Get not caught slugs
caught_dl_slugs = list(set(messari_to_dl_dict.values()))
not_translated = list(set(DL_SLUGS) - set(caught_dl_slugs))
print(not_translated)
print(len(caught_dl_slugs))
print(len(not_translated))

#########################################
# Create Messari to Token Terminal dictionary
#########################################
from messari.tokenterminal import TokenTerminal

API_KEY = 'API_KEY_HERE'
tt = TokenTerminal(api_key=API_KEY)

TT_SLUGS = tt.get_project_ids()

messari_to_tt_dict = {}
for asset in messari_assets:
    if asset in TT_SLUGS:
        # Messari is case insensitive & DeFi Llama is always lowercase so drop to lower()
        slug = str(messari_assets[asset]['slug']).lower()
        symbol = str(messari_assets[asset]['symbol']).lower()
        messari_to_tt_dict[slug] = asset
        messari_to_tt_dict[symbol] = asset

with open("../messari/mappings/messari_to_tt_hardcode.json", "r") as infile:
    messari_to_tt_hardcode = json.load(infile)

for entry in messari_to_tt_hardcode:
    # If there is an overlap print about it to notify user
    if messari_to_tt_hardcode[entry] in messari_to_tt_dict.values():
        print(f"overlapping entry for {entry} using hardcoded value {messari_to_tt_hardcode[entry]}")

    messari_to_tt_dict[entry] = messari_to_tt_hardcode[entry]

with open("../messari/mappings/messari_to_tt.json", "w") as outfile:
    json.dump(messari_to_tt_dict, outfile)

# Get not caught slugs
caught_tt_slugs = list(set(messari_to_tt_dict.values()))
not_translated = list(set(TT_SLUGS) - set(caught_tt_slugs))
print(not_translated)
print(len(caught_tt_slugs))
print(len(not_translated))
