"""This module is meant to contain the Deep DAO class"""

import requests
from string import Template
from typing import Union, List, Dict
import json
import pandas as pd
import numpy as np

from messari.dataloader import DataLoader
from messari.utils import validate_input
from .helpers import unpack_dataframe_of_lists, unpack_dataframe_of_dicts

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
        """Returns list of DAOs tracked by Deep DAO
        Returns
        -------
           list
               list of DAOs
        """
        return list(self.id_tax.keys())

    def get_member_list(self) -> List:
        """Returns list of DAO members tracked by Deep DAO
        Returns
        -------
           list
               list of memberss
        """
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
        """Returns basic summaries of information for all Deep DAO tracked organizations
        Returns
        -------
           DataFrame
               pandas DataFrame of Deep DAO organizations summaries
        """
        response = requests.get(DASHBOARD_URL).json()
        summary = response["daosSummary"]
        summary_df = pd.DataFrame(summary)
        summary_df.drop('daosArr', axis=1, inplace=True)
        return summary_df

    def get_overview(self) -> pd.DataFrame:
        """Returns an overview of the DAO ecosystem aggreated by Deep DAO
        Returns
        -------
           DataFrame
               pandas DataFrame of DAO ecosystem overview
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
        """Returns rankings for organizations tracked by Deep DAO
        Returns
        -------
           DataFrame
               pandas DataFrame of Deep DAO organizations rankings
        """
        response = requests.get(DASHBOARD_URL).json()
        rankings = response["daoEcosystemOverview"]["daoRankings"]
        rankings_df = pd.DataFrame(rankings)
        rankings_df.drop('date', axis=1, inplace=True)
        return rankings_df

    def get_tokens(self) -> pd.DataFrame:
        """Returns information about the utilization of different tokens across all DAOs tracked by Deep DAO
        Returns
        -------
           DataFrame
               pandas DataFrame with token utilization
        """
        response = requests.get(DASHBOARD_URL).json()
        tokens = response["daoTokens"]
        tokens_df = pd.DataFrame(tokens)
        return tokens_df


    ####### Overview
    def get_dao_info(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
        """Returns basic information for given DAO(s)
        Parameters
        ----------
            dao_slugs: Union[str, List]
                Input DAO names as a string or list (ex. 'Uniswap')

        Returns
        -------
           DataFrame
               pandas DataFrame with general DAO information
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
        """Returns financial indices for given DAO(s) like the Gini Index or the Herfindahl–Hirschman index
        Parameters
        ----------
            dao_slugs: Union[str, List]
                Input DAO names as a string or list (ex. 'Uniswap')

        Returns
        -------
           DataFrame
               pandas DataFrame with DAO indices
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
            # NOTE, the hardcoding here won't scale if DD expands this API
            new_data = {}
            if 'giniIndex' in data.keys():
                new_data['giniIndex'] = data['giniIndex']['giniIndex']
            if 'HHIndex' in data.keys():
                new_data['HHIndex'] = data['HHIndex']['hh']
            if 'HHIndexTop20' in data.keys():
                new_data['HHIndexTop20'] = data['HHIndexTop20']['hh']
            data_series = pd.Series(new_data)
            indices_list.append(data_series)

        indices_df = pd.concat(indices_list, keys=slugs, axis=1)
        # NOTE, somehow nan sneaks into the df despite all dicts being None?
        indices_df.replace({np.nan: None}, inplace=True)
        return indices_df

    def get_dao_proposals(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
        """Returns information about governance proposals for given DAO(s)
        Parameters
        ----------
            dao_slugs: Union[str, List]
                Input DAO names as a string or list (ex. 'Uniswap')

        Returns
        -------
           DataFrame
               pandas DataFrame with DAO governance proposals
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

        proposals_df = unpack_dataframe_of_dicts(proposals_df)


        return proposals_df

    def get_dao_members(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
        """Returns information about the Members of given DAO(s)
        Parameters
        ----------
            dao_slugs: Union[str, List]
                Input DAO names as a string or list (ex. 'Uniswap')

        Returns
        -------
           DataFrame
               pandas DataFrame with DAO Members
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

        members_df = unpack_dataframe_of_dicts(members_df)

        return members_df

    def get_dao_voter_coalitions(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
        """Returns information about different voting coalitions for given DAO(s)
        Parameters
        ----------
            dao_slugs: Union[str, List]
                Input DAO names as a string or list (ex. 'Uniswap')

        Returns
        -------
           DataFrame
               pandas DataFrame with DAO voter coalitions
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


        coalitions_df = unpack_dataframe_of_lists(coalitions_df)
        # TODO
        #coalitions_df.rename(columns={"subsetLength": "size"}, inplace=True)



        return coalitions_df

    def get_dao_financials(self, dao_slugs: Union[str, List]) -> pd.DataFrame:
        """Returns information about the financials of given DAO(s)
        Parameters
        ----------
            dao_slugs: Union[str, List]
                Input DAO names as a string or list (ex. 'Uniswap')

        Returns
        -------
           DataFrame
               pandas DataFrame with DAO financials
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
        tokens = financials_df.loc['tokens']
        df_list=[]
        for token in tokens:
            df = pd.DataFrame(token)
            df_list.append(df)
        tokenss_df=pd.concat(df_list,keys=financials_df.columns,axis=1)

        return tokenss_df


    ####### Members
    def get_top_members(self, count: int=50) -> pd.DataFrame:
        """Returns a dataframe of basic information for the the top 'count' Members tracked by Deep DAO sorted amount of DAO's particpating in.
        Parameters
        ----------
            count: int
                Amount of members to return info for (default=50)

        Returns
        -------
           DataFrame
               pandas DataFrame with top members across DAOs
        """
        #TODO can work on paging this
        params = {"limit": count,
                  "offset": 0,
                  "sortBy": "daoAmount"}
        people = self.get_response(PEOPLE_URL, params=params)
        people_df = pd.DataFrame(people)
        return people_df

    def get_member_info(self, pubkeys: Union[str, List]) -> pd.DataFrame:
        """Returns basic information for given member(s)
        Parameters
        ----------
            pubkeys: Union[str, List]
                Input public keys for DAO members as a string or list

        Returns
        -------
           DataFrame
               pandas DataFrame with member information
        """
        users = validate_input(pubkeys)
        user_info_list = []
        for user in users:
            if user in self.address_tax.keys():
                user_address = self.address_tax[user]
            else:
                user_address = user
            endpoint_url = USER_URL.substitute(user=user_address)
            user_info = requests.get(endpoint_url).json()
            user_info_series = pd.Series(user_info)
            user_info_list.append(user_info_series)
        users_info_df = pd.concat(user_info_list, keys=users, axis=1)
        return users_info_df

    def get_member_proposals(self, pubkeys: Union[str, List]) -> pd.DataFrame:
        """Returns proposal history for given member(s)
        Parameters
        ----------
            pubkeys: Union[str, List]
                Input public keys for DAO members as a string or list

        Returns
        -------
           DataFrame
               pandas DataFrame with member governance proposals
        """
        #TODO what even is this
        users = validate_input(pubkeys)
        proposal_info_list = []
        for user in users:
            if user in self.address_tax.keys():
                user_address = self.address_tax[user]
            else:
                user_address = user
            endpoint_url = USER_PROPOSALS_URL.substitute(user=user_address)
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

        proposals_df = unpack_dataframe_of_lists(proposals_info_df)

        return proposals_df


    def get_member_votes(self, pubkeys: Union[str, List]) -> pd.DataFrame:
        """Returns voting history for given member(s)
        Parameters
        ----------
            pubkeys: Union[str, List]
                Input public keys for DAO members as a string or list

        Returns
        -------
           DataFrame
               pandas DataFrame with member voting history
        """
        users = validate_input(pubkeys)
        votes_info_list = []
        for user in users:
            if user in self.address_tax.keys():
                user_address = self.address_tax[user]
            else:
                user_address = user
            endpoint_url = USER_VOTES_URL.substitute(user=user_address)
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

        votes_df = unpack_dataframe_of_lists(votes_info_df)

        return votes_df
