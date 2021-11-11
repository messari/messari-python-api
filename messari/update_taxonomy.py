###########################################################################
# The purpose of this is to update the messari taxonomy dictionaries. It is 
# not meant to be included as part of the messari python package, rather it 
# is meant to update json objects used by the messari python package for 
# taxonomy translation across different API's. This design is intentionally 
# meant to just be run as a script, don't worry about wrapping anything 
# into functions.
###########################################################################
import json
import os

import messari
messari.MESSARI_API_KEY = '28166d37-3766-4f08-bb42-4469ab78fd0b'

##########################
# Get all DeFi Llama slugs
##########################
from messari.defi_llama import DL_SLUGS

########################
# Get all messari assets
########################
from messari.assets import get_all_assets

limit=500
page=1
messari_assets = get_all_assets(page=page, limit=limit)
current_len = len(messari_assets)

# NOTE: this will probably crash if one page len == limit and the nex page len == 0
while current_len == limit:
    page+=1
    # NOTE: pushing up against messari rate limiting here, not sure how to handle user experience
    new_assets = get_all_assets(page=page, limit=limit)
    messari_assets.update(new_assets)
    current_len = len(new_assets)

#########################################
# Create Messari to DeFi Llama dictionary
#########################################
messari_to_dl_dict={}
for asset in messari_assets:
    if asset in DL_SLUGS:
        # Messari is case insensitive & DeFi Llama is always lowercase so drop to lower()
        slug = str(messari_assets[asset]['slug']).lower()
        symbol = str(messari_assets[asset]['symbol']).lower()
        messari_to_dl_dict[slug] = asset
        messari_to_dl_dict[symbol] = asset

# Open hardcoded values and appened
with open("json/messari_to_dl_hardcode.json", "r") as infile:
    messari_to_dl_hardcode = json.load(infile)



for entry in messari_to_dl_hardcode:
    messari_to_dl_dict[entry] = messari_to_dl_hardcode[entry]

    # If there is an overlap print about it to notify user
    if messari_to_dl_hardcode[entry] in messari_to_dl_dict.values():
        print(f"overlapping entry for {entry} using hardcoded value {messari_to_dl_hardcode[entry]}")

with open("json/messari_to_dl.json", "w") as outfile:
    json.dump(messari_to_dl_dict, outfile)


# Get not caught slugs
caught_dl_slugs = list(set(messari_to_dl_dict.values()))
not_translated = list(set(DL_SLUGS) - set(caught_dl_slugs))
print(not_translated)
