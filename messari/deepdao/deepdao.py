"""This module is meant to contain the Deep DAO class"""

import requests
from string import Template
from typing import Union, List, Dict
import pandas as pd

from messari.dataloader import DataLoader
from messari.utils import validate_input
from .helpers import format_df

##########################
# URL Endpoints
##########################
## DAOs
ORGANIZATIONS_URL = "https://backend.deepdao.io/dashboard/organizations"
DASHBOARD_URL = "https://backend.deepdao.io/dashboard/ksdf3ksa-937slj3"
DAO_URL = Template("https://backend.deepdao.io/dao/ksdf3ksa-937slj3/$dao_id")

## People
PEOPLE_URL = "https://backend.deepdao.io/people/top"
USER_URL = Template("https://backend.deepdao.io/user/$user") #user is 0xpubkey
USER_PROPOSALS_URL = Template("https://backend.deepdao.io/user/$user/proposals")
USER_VOTES_URL = Template("https://backend.deepdao.io/user/$user/votes")

## Governance


#### DAOs
class DeepDAO(DataLoader):
    """This class is a wrapper around the DeepDAO API
    """

    def __init__(self):
        DataLoader.__init__(self, api_dict=None, taxonomy_dict=self.get_id_dict)
        self.id_tax = self.get_id_dict()
        self.name_tax = self.get_name_dict()
        self.people_tax = self.get_people_dict()
        self.address_tax = self.get_address_dict()

    def get_organizations(self) -> pd.DataFrame:
        """Returns basic info for all Deep DAO tracked organizations
        Returns
        -------
           DataFrame
               pandas DataFrame of Deep DAO organizations info
        """
        organizations = requests.get(ORGANIZATIONS_URL).json()
        organizations_df = pd.DataFrame(organizations)
        return organizations_df

    # Dashboard
    def get_summary(self) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        response = requests.get(DASHBOARD_URL).json()
        summary = response["daosSummary"]
        summary_df = pd.DataFrame(summary)
        return summary_df

    def get_overview(self) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        response = requests.get(DASHBOARD_URL).json()
        overview = response["daoEcosystemOverview"]
        #print(overview)
        #print(overview.keys())
        dict_list = []
        count=0
        for date in overview["datesArray"]:
            ts_dict = {"date":date,
                       "aum":overview["aumArray"][count],
                       "members":overview["membersArray"][count],
                       "over1M":overview["over1MArray"][count],
                       "over50k":overview["over50KArray"][count],
                       "over10Members":overview["over10MembersArray"][count],
                       "over100Members":overview["over100MembersArray"][count]}
            dict_list.append(ts_dict)
            count+=1
        overview_df = pd.DataFrame(dict_list)
        overview_df.set_index('date', inplace=True)
        old_index = overview_df.index
        new_index = []
        for index in old_index:
            dt_string = str(index).split('T')[0]
            new_index.append(dt_string)
        #print(new_index)
        overview_df.index=new_index
        # drop last row because it's not formatted correctly & data is weird
        # TODO look into this
        overview_df.drop(overview_df.tail(1).index,inplace=True)
        overview_df.index = pd.to_datetime(overview_df.index, format='%Y-%m-%d')
        overview_df = overview_df[~overview_df.index.duplicated(keep='last')]
        #overview_df['date'] = overview_df['date'][0].split('T')[0]
        #overview_df.set_index('date', inplace=True)
        #overview_df.index = overview_df.index.date
        #datboy2 = format_df(datboy)
        return overview_df

    def get_rankings(self) -> pd.DataFrame:
        """Returns various rankings for organizations tracked by Deep DAO
        Returns
        -------
           DataFrame
               pandas DataFrame of Deep DAO organizations rankings
        """
        response = requests.get(DASHBOARD_URL).json()
        rankings = response["daoEcosystemOverview"]["daoRankings"]
        rankings_df = pd.DataFrame(rankings)
        return rankings_df

    def get_tokens(self) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        response = requests.get(DASHBOARD_URL).json()
        tokens = response["daoTokens"]
        tokens_df = pd.DataFrame(tokens)
        return tokens_df

    def get_id_dict(self) -> Dict:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        summary = self.get_summary()
        id_dict={}
        for index, dao in summary.iterrows():
            id_dict[dao["daoName"]] = dao["daoId"]
        return id_dict

    def get_name_dict(self) -> Dict:
        summary = self.get_summary()
        name_dict={}
        for index, dao in summary.iterrows():
            name_dict[dao["daoId"]] = dao["daoName"]
        return name_dict

    def get_people_dict(self) -> Dict:
        # NOTE, if this starts to not capture all people
        # Increase num_people
        people = self.get_people(num_people=100000)
        people_dict={}
        for index, person in people.iterrows():
            people_dict[person["address"]] = person["name"]
        return people_dict

    def get_address_dict(self) -> Dict:
        # NOTE, if this starts to not capture all people
        # Increase num_people
        people = self.get_people(num_people=100000)
        people_dict={}
        for index, person in people.iterrows():
            people_dict[person["name"]] = person["address"]
        return people_dict

    def get_info(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        ### TODO extract nested lists & dicts

        slugs = validate_input(dao_slugs)

        dao_info_list = []
        for slug in slugs:
            # TODO swap w/ validate
            if slug in self.id_tax.keys():
                dao_id = self.id_tax[slug]
            else:
                dao_id = slug
            endpoint_url = DAO_URL.substitute(dao_id=dao_id)
            dao_info = self.get_response(endpoint_url)
            dao_info_series = pd.Series(dao_info)
            dao_info_list.append(dao_info_series)

        dao_info_df = pd.concat(dao_info_list, keys=slugs, axis=1)
        return dao_info_df

    #print(get_info(prots))


    ####### People
    def get_people(self, num_people: int=50) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        #TODO can work on paging this
        params = {"limit": num_people,
                  "offset": 0,
                  "sortBy": "daoAmount"}
        people = self.get_response(PEOPLE_URL, params=params)
        people_df = pd.DataFrame(people)
        return people_df

    def get_users(self, users_input: Union[str, List]) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
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
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        #TODO what even is this
        users = validate_input(users_input)
        proposal_info_list = []
        for user in users:
            endpoint_url = USER_PROPOSALS_URL.substitute(user=user)
            proposal_info = self.get_response(endpoint_url)
            proposal_info_series = pd.Series(proposal_info)
            proposal_info_list.append(proposal_info_series)
        proposals_info_df = pd.concat(proposal_info_list, keys=users, axis=1)


        old_index = proposals_info_df.index
        new_index = []
        for old in old_index:
            if old in self.name_tax.keys():
                new_index.append(self.name_tax[old])
            else:
                new_index.append(old)
        proposals_info_df.index = new_index

        old_columns = proposals_info_df.columns
        new_columns = []
        for old in old_columns:
            if old in self.people_tax.keys():
                new_columns.append(self.people_tax[old])
            else:
                new_columns.append(old)
        proposals_info_df.columns = new_columns

        user_df_list=[]
        for column_name in proposals_info_df.columns:
            user_df = proposals_info_df[column_name]
            tmp_df_list=[]
            for entry in user_df:
                if type(entry) == list:
                    tmp_df = pd.DataFrame(entry)
                    tmp_df_list.append(tmp_df)
            reorg_user_df = pd.concat(tmp_df_list)
            reorg_user_df.reset_index(drop=True, inplace=True)
            user_df_list.append(reorg_user_df)
        proposals_df = pd.concat(user_df_list, keys=proposals_info_df.columns, axis=1)

        return proposals_df


    def get_users_votes(self, users_input: Union[str, List]) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        users = validate_input(users_input)
        votes_info_list = []
        for user in users:
            endpoint_url = USER_VOTES_URL.substitute(user=user)
            votes_info = self.get_response(endpoint_url)
            votes_info_series = pd.Series(votes_info)
            votes_info_list.append(votes_info_series)

        votes_info_df = pd.concat(votes_info_list, keys=users, axis=1)

        old_index = votes_info_df.index
        new_index = []
        for old in old_index:
            if old in self.name_tax.keys():
                new_index.append(self.name_tax[old])
            else:
                new_index.append(old)
        votes_info_df.index = new_index

        old_columns = votes_info_df.columns
        new_columns = []
        for old in old_columns:
            if old in self.people_tax.keys():
                new_columns.append(self.people_tax[old])
            else:
                new_columns.append(old)
        votes_info_df.columns = new_columns

        user_df_list=[]
        for column_name in votes_info_df.columns:
            user_df = votes_info_df[column_name]
            tmp_df_list=[]
            for entry in user_df:
                if type(entry) == list:
                    tmp_df = pd.DataFrame(entry)
                    tmp_df_list.append(tmp_df)
            reorg_user_df = pd.concat(tmp_df_list)
            reorg_user_df.reset_index(drop=True, inplace=True)
            user_df_list.append(reorg_user_df)
        votes_df = pd.concat(user_df_list, keys=votes_info_df.columns, axis=1)

        return votes_df


prots = ["Uniswap", "Compound"]
users = ["0xd26a3f686d43f2a62ba9eae2ff77e9f516d945b9", "0x68d36dcbdd7bbf206e27134f28103abe7cf972df"]
