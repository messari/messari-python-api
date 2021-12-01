"""This module is meant to contain the Deep DAO class"""

import requests
from string import Template
from typing import Union, List, Dict
import json
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
        # Need to init DataLoader first to have get_respone work in rest of __init__
        DataLoader.__init__(self, api_dict=None, taxonomy_dict=None)

        people = self.get_top_members(count=100000)
        people_dict={}
        address_dict={}
        for index, person in people.iterrows():
            people_dict[person["address"]] = person["name"]
            address_dict[person["name"]] = person["address"]

        # get person
        self.people_tax = people_dict
        # get address
        self.address_tax = address_dict

        summary = self.get_summary()
        name_dict={}
        id_dict={}
        for index, dao in summary.iterrows():
            name_dict[dao["daoId"]] = dao["daoName"]
            id_dict[dao["daoName"]] = dao["daoId"]

        # get name
        self.name_tax = name_dict
        # get id
        self.id_tax = id_dict

    ####### Class helpers
    def get_dao_list(self) -> List:
        return list(self.id_tax.keys())

    def get_member_list(self) -> List:
        return list(self.address_tax.keys())



    ####### Overview
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

    def get_summary(self) -> pd.DataFrame:
        """Returns
        Returns
        -------
        """
        response = requests.get(DASHBOARD_URL).json()
        summary = response["daosSummary"]
        summary_df = pd.DataFrame(summary)
        summary_df.drop('daosArr', axis=1, inplace=True)
        return summary_df

    def get_overview(self) -> pd.DataFrame:
        """Returns
        Returns
        -------
        """
        response = requests.get(DASHBOARD_URL).json()
        overview = response["daoEcosystemOverview"]
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
        overview_df.index=new_index
        # drop last row because it's not formatted correctly & data is weird
        # TODO look into this
        overview_df.drop(overview_df.tail(1).index,inplace=True)
        overview_df.index = pd.to_datetime(overview_df.index, format='%Y-%m-%d')
        overview_df = overview_df[~overview_df.index.duplicated(keep='last')]
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
        Returns
        -------
        """
        response = requests.get(DASHBOARD_URL).json()
        tokens = response["daoTokens"]
        tokens_df = pd.DataFrame(tokens)
        return tokens_df


    ####### Overview
    def get_dao_info(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """

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
        dao_info_df.drop(['rankings', 'indices', 'proposals', 'members', 'votersCoalition', 'financial'], inplace=True)
        return dao_info_df


    def get_dao_indices(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
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
        indices = dao_info_df.loc['indices']

        indices_list = []
        for index in indices:
            data = json.loads(index)
            # Note, the hardcoding here won't scale if DD expands this API
            data_series = pd.Series(data)
            indices_list.append(data_series)

        indices_df = pd.concat(indices_list, keys=slugs, axis=1)
        return indices_df

    def get_dao_proposals(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
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
        proposals = dao_info_df.loc['proposals']

        proposals_list = []
        for proposal in proposals:
            data = json.loads(proposal)
            data_series = pd.Series(data)
            proposals_list.append(data_series)
        proposals_df = pd.concat(proposals_list, keys=slugs, axis=1)

        #extract
        proposals_df_list=[]
        for column_name in proposals_df.columns:
            user_df = proposals_df[column_name]
            tmp_series_list=[]
            for entry in user_df:
                #if type(entry) == Dict:
                tmp_series = pd.Series(entry)
                tmp_series_list.append(tmp_series)
            reorg_proposals_df = pd.DataFrame(tmp_series_list)
            reorg_proposals_df.reset_index(drop=True, inplace=True)
            proposals_df_list.append(reorg_proposals_df)
        proposals_df = pd.concat(proposals_df_list, keys=proposals_df.columns, axis=1)


        return proposals_df

    def get_dao_members(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
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
        members = dao_info_df.loc['members']

        members_list = []
        for member in members:
            data = json.loads(member)
            data_series = pd.Series(data)
            members_list.append(data_series)

        members_df = pd.concat(members_list, keys=slugs, axis=1)

        proposals_df_list=[]
        for column_name in members_df.columns:
            user_df = members_df[column_name]
            tmp_series_list=[]
            for entry in user_df:
                #if type(entry) == Dict:
                tmp_series = pd.Series(entry)
                tmp_series_list.append(tmp_series)
            reorg_proposals_df = pd.DataFrame(tmp_series_list)
            reorg_proposals_df.reset_index(drop=True, inplace=True)
            proposals_df_list.append(reorg_proposals_df)
        members_df = pd.concat(proposals_df_list, keys=members_df.columns, axis=1)

        return members_df

    def get_dao_voter_coalitions(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
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
        coalitions = dao_info_df.loc['votersCoalition']

        coalitions_list = []
        for coalition in coalitions:
            data = json.loads(coalition)
            data_series = pd.Series(data)
            coalitions_list.append(data_series)

        coalitions_df = pd.concat(coalitions_list, keys=slugs, axis=1)


        # pulling out data from list members of DF
        user_df_list=[]
        for column_name in coalitions_df.columns:
            user_df = coalitions_df[column_name]
            tmp_df_list=[]
            for entry in user_df:
                if type(entry) == list:
                    tmp_df = pd.DataFrame(entry)
                    tmp_df.rename(columns={"subsetLength": "size"}, inplace=True)
                    tmp_df_list.append(tmp_df)
            reorg_user_df = pd.concat(tmp_df_list)
            reorg_user_df.reset_index(drop=True, inplace=True)
            user_df_list.append(reorg_user_df)
        coalitions_df = pd.concat(user_df_list, keys=coalitions_df.columns, axis=1)



        return coalitions_df

    def get_dao_financials(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
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
        financials = dao_info_df.loc['financial']

        financials_list = []
        for financial in financials:
            data = json.loads(financial)
            data_series = pd.Series(data)
            financials_list.append(data_series)

        financials_df = pd.concat(financials_list, keys=slugs, axis=1)

        return financials_df


    ####### Members
    def get_top_members(self, count: int=50) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        #TODO can work on paging this
        params = {"limit": count,
                  "offset": 0,
                  "sortBy": "daoAmount"}
        people = self.get_response(PEOPLE_URL, params=params)
        people_df = pd.DataFrame(people)
        return people_df

    def get_member(self, pubkeys: Union[str, List]) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        users = validate_input(pubkeys)
        user_info_list = []
        for user in users:
            endpoint_url = USER_URL.substitute(user=user)
            user_info = requests.get(endpoint_url).json()
            user_info_series = pd.Series(user_info)
            user_info_list.append(user_info_series)
        users_info_df = pd.concat(user_info_list, keys=users, axis=1)
        return users_info_df

    def get_member_proposals(self, pubkeys: Union[str, List]) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        #TODO what even is this
        users = validate_input(pubkeys)
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


    def get_member_votes(self, pubkeys: Union[str, List]) -> pd.DataFrame:
        """Returns
        Parameters
        ----------
        Returns
        -------
        """
        users = validate_input(pubkeys)
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
