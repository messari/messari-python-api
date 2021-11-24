"""This module is meant to contain the DeFiLlama class"""

import requests
import json
from string import Template
from typing import Union, List, Dict
import pandas as pd

from messari.dataloader import DataLoader
from messari.utils import validate_input

##########################
# URL Endpoints
##########################
## DAOs
ORGANIZATIONS_URL = "https://backend.deepdao.io/dashboard/organizations"
DASHBOARD_URL = "https://backend.deepdao.io/dashboard/ksdf3ksa-937slj3"
DAO_URL = Template("https://backend.deepdao.io/dao/ksdf3ksa-937slj3/$dao_id")

## People
PEOPLE_STATS_URL = "https://backend.deepdao.io/people/stats"
PEOPLE_URL = "https://backend.deepdao.io/people/top?limit=50&offset=0&sortBy=daoAmount"
USER_URL = Template("https://backend.deepdao.io/user/$user") #user is 0xpubkey
USER_PROPOSALS_URL = Template("https://backend.deepdao.io/user/$user/proposals")
USER_VOTES_URL = Template("https://backend.deepdao.io/user/$user/votes")

## Governance


#### DAOs
class DeepDAO(DataLoader):
    """This class is a wrapper around the DeepDAO API
    """

    def __init__(self):
        #messari_to_dl_dict = get_taxonomy_dict("messari_to_dl.json")
        self.id_tax = self.get_id_dict()
        DataLoader.__init__(self, api_dict=None, taxonomy_dict=self.get_id_dict)

    def get_organizations(self) -> pd.DataFrame:
        organizations = requests.get(ORGANIZATIONS_URL).json()
        organizations_df = pd.DataFrame(organizations)
        return organizations_df

    # Dashboard
    def get_dashboard(self) -> Dict:
        dashboard = requests.get(DASHBOARD_URL).json()
        return dashboard

    def get_id_dict(self) -> Dict:
        dashboard = self.get_dashboard()
        id_dict={}
        for dao in dashboard["daosSummary"]:
            id_dict[dao["daoName"]] = dao["daoId"]
        return id_dict


    def get_info(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
        ### TODO extract nested lists & dicts

        slugs = validate_input(dao_slugs)

        dao_info_list = []
        for slug in slugs:
            # TODO swap w/ validate
            dao_id = self.id_tax[slug]
            endpoint_url = DAO_URL.substitute(dao_id=dao_id)
            dao_info = self.get_response(endpoint_url)
            dao_info_series = pd.Series(dao_info)
            dao_info_list.append(dao_info_series)

        dao_info_df = pd.concat(dao_info_list, keys=slugs, axis=1)
        return dao_info_df

    #print(get_info(prots))


    ####### People

    def get_people_stats(self) -> pd.DataFrame:
        people_stats = self.get_response(PEOPLE_STATS_URL)
        return people_stats

    def get_people(self) -> pd.DataFrame:
        #TODO can work on paging this
        people = self.get_response(PEOPLE_URL)
        people_df = pd.DataFrame(people)
        return people_df

    def get_users(self, users_input: Union[str, List]) -> pd.DataFrame:
        users = validate_input(users_input)
        user_info_list = []
        for user in users:
            endpoint_url = USER_URL.substitute(user=user)
            user_info = requests.get(endpoint_url).json()
            user_info_series = pd.Series(user_info)
            user_info_list.append(user_info_series)
        users_info_df = pd.concat(user_info_list, keys=users, axis=1)
        return users_info_df

    def get_users_proposals(self, users_input: Union[str, List]) -> pd.DataFrame:
        #TODO what even is this
        users = validate_input(users_input)
        proposal_info_list = []
        for user in users:
            endpoint_url = USER_PROPOSALS_URL.substitute(user=user)
            proposal_info = self.get_response(endpoint_url)
            proposal_info_series = pd.Series(proposal_info)
            proposal_info_list.append(proposal_info_series)
        proposal_info_df = pd.concat(proposal_info_list, keys=users, axis=1)
        return proposal_info_df


    def get_users_votes(self, users_input: Union[str, List]) -> pd.DataFrame:
        users = validate_input(users_input)
        votes_info_list = []
        for user in users:
            endpoint_url = USER_VOTES_URL.substitute(user=user)
            votes_info = self.get_response(endpoint_url)
            votes_info_series = pd.Series(votes_info)
            votes_info_list.append(votes_info_series)
        votes_info_df = pd.concat(votes_info_list, keys=users, axis=1)
        return votes_info_df


prots = ["Uniswap", "Compound"]
users = ["0xd26a3f686d43f2a62ba9eae2ff77e9f516d945b9", "0x68d36dcbdd7bbf206e27134f28103abe7cf972df"]
