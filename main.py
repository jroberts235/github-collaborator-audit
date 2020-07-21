#!/usr/bin/env python
import csv
from python_graphql_client import GraphqlClient
from os import environ
from requests.exceptions import HTTPError
from sys import argv


class GithubContributorPermsReporter(Exception): pass


DEBUG = False


def graphql_call(query, variables):
    try:
        return client.execute(query=query, variables=variables)
    except HTTPError as e:
        raise GithubContributorPermsReporter(e)


def get_members(client, members_dict, organization, pages_to_get):
    """
    Call graphql endpoint and fetch member data

    Args:
        client:
        members_dict:
        organization:
        pages_to_get:

    Returns:
        total_count_returned:Int
    """
    print('Getting Contributors...')

    def add_member_data(members_with_role_edge):
        for member in members_with_role_edge:
            members_dict[member['node']['login']] = {'login': member['node']['login'], 'name': member['node']['name']}

    query = """
      query($organization:String!, $pagesToGet:Int) {
        organization(login:$organization) {
          membersWithRole(first:$pagesToGet) {
            totalCount
            edges {
              hasTwoFactorEnabled
              node {
                login
                name
              }
            role
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
      }
    """
    variables = {"organization": organization, "pagesToGet": pages_to_get}
    ret = graphql_call(query, variables)
    if DEBUG:
        print(ret)
    add_member_data(ret['data']['organization']['membersWithRole']['edges'])
    has_next_page = ret['data']['organization']['membersWithRole']['pageInfo']['hasNextPage']
    end_cursor = ret['data']['organization']['membersWithRole']['pageInfo']['endCursor']

    while has_next_page:
        query = """
          query($organization:String!, $pagesToGet:Int, $endCursor:String) {
            organization(login:$organization) {
              membersWithRole(first:$pagesToGet, after:$endCursor) {
                totalCount
                edges {
                  hasTwoFactorEnabled
                  node {
                    login
                    name
                  }
                role
                }
                pageInfo {
                  endCursor
                  hasNextPage
                }
              }
            }
          }
        """
        variables = {"organization": organization, "pagesToGet": pages_to_get, "endCursor": end_cursor}
        ret = graphql_call(query, variables)
        if DEBUG:
            print(ret)
        add_member_data(ret['data']['organization']['membersWithRole']['edges'])
        has_next_page = ret['data']['organization']['membersWithRole']['pageInfo']['hasNextPage']
        end_cursor = ret['data']['organization']['membersWithRole']['pageInfo']['endCursor']

    # basic sanity check on number of returned items and size of members_dict
    total_count_returned = ret['data']['organization']['membersWithRole']['totalCount']
    if len(members_dict.keys()) != total_count_returned:
        raise GithubContributorPermsReporter(f'len(members_dict.keys()) ({len(members_dict.keys())}) != total_count ({total_count_returned}) returned from Github!')

    return total_count_returned


def get_repos_and_perms(client, list_of_repos, organization, pages_to_get):
    """
    Call graphql endpoint and fetch Repo data

    Args:
        client:
        list_of_repos:
        pages_to_get:
        organization:

    Returns:
        total_count_returned:Int
    """
    print('Getting Repositories...')

    query = """
    query($organization:String!, $pagesToGet:Int) {
      organization(login:$organization) {
        repositories(first:$pagesToGet) {
          totalCount
          edges {
            node {
              isArchived
              name
              collaborators {
                edges {
                  permission
                  node {
                    login
                  }
                }
              }
            }
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }
    """
    variables = {"organization": organization, "pagesToGet": pages_to_get}
    ret = graphql_call(query, variables)
    if DEBUG:
        print(ret)
    list_of_repos.extend(ret['data']['organization']['repositories']['edges'])
    has_next_page = ret['data']['organization']['repositories']['pageInfo']['hasNextPage']
    end_cursor = ret['data']['organization']['repositories']['pageInfo']['endCursor']

    while has_next_page:
        query = """
        query($organization:String!, $pagesToGet:Int, $endCursor:String) {
          organization(login:$organization) {
            repositories(first:$pagesToGet, after:$endCursor ) {
              totalCount
              edges {
                node {
                  isArchived
                  name
                  collaborators {
                    edges {
                      permission
                      node {
                        login
                      }
                    }
                  }
                }
              }
            pageInfo {
              endCursor
              hasNextPage
            }
            }
          }
        }
        """
        variables = {"organization": organization, "pagesToGet": pages_to_get, "endCursor": end_cursor}
        ret = graphql_call(query, variables)
        if DEBUG:
            print(ret)
        #list_of_repos.extend(ret['data']['organization']['repositories']['edges'])

        '''
        Without the below, the error lists at the bottom of returned pages trigger exceptions.
        This maybe due to the API key not having permissions enough on certain repositories but 
        the endpoint returns the error status without the repository name. A better graphql client
        maybe to handle this condition.
        '''
        try:
            list_of_repos.extend(ret['data']['organization']['repositories']['edges'])
        except KeyError as e:
            print(f'Error: {e}')

        has_next_page = ret['data']['organization']['repositories']['pageInfo']['hasNextPage']
        end_cursor = ret['data']['organization']['repositories']['pageInfo']['endCursor']

    # basic sanity check on number of returned items and size of list_of_repos
    total_count_returned = ret['data']['organization']['repositories']['totalCount']
    if len(list_of_repos) != total_count_returned:
        raise GithubContributorPermsReporter(f'len(list_of_repos) ({len(list_of_repos)}) != total_count ({total_count_returned}) returned from Github!')

    return total_count_returned


def process_repo_list(list_of_repos, members_dict):
    """
    Create a key and value pair, where the key is the repo name, and the value is the level of
    access that a particular Member has. The k,v is added to the members_dict, and the repo_name
    is appended to the list of column headings.

    Args:
        list_of_repos:
        members_dict:

    Returns:
        column_headings:List
    """
    column_headings = []

    for repo in list_of_repos:
        if repo['node']['isArchived']:
            repo_name = f"{repo['node']['name']}(ARCHIVED)"
        else:
            repo_name = repo['node']['name']

        column_headings.append(repo_name)

        if repo['node']['collaborators'] is not None and 'edges' in repo['node']['collaborators'].keys():
            for collaborator in repo['node']['collaborators']['edges']:
                login = collaborator['node']['login']
                permission = collaborator['permission']

                # associate w/ members using 'login' as key
                try:
                    members_dict[login][repo_name] = permission
                except KeyError:
                    continue

    return column_headings


def get_graphql_client(graphql_endpoint):
    # Github access token must be export in current shell ex: 'export GITHUB_ACCESS_TOKEN=XXXXXXXXXXXXXXXXXXXXXXX'
    try:
        access_token = environ["GITHUB_ACCESS_TOKEN"]
    except KeyError:
        raise GithubContributorPermsReporter('The env.var, "GITHUB_ACCESS_TOKEN" must be set before running.')
    else:
        # Instantiate the client with an endpoint.
        print('Connecting...')
        try:
            client = GraphqlClient(endpoint=graphql_endpoint, headers={'Authorization': f'Bearer {access_token}'})
        except HTTPError as e:
            raise GithubContributorPermsReporter(e)
        else:
            print('Success!')
            return client


def generate_csv_file(csv_ready_list, output_filename):
    # write CSV file to current directory
    try:
        with open(output_filename, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in csv_ready_list:
                writer.writerow(data)
    except IOError:
        raise GithubContributorPermsReporter('I/O error')
    else:
        print(f'\nCSV written to: {output_filename}\n')


if __name__ == '__main__':
    try:
        organization = argv[1]
    except IndexError:
        print('Please provided the Github organization as argument ex. $> main.py my-org-name')
        exit(0)

    base_csv_columns = ['login', 'name']
    graphql_endpoint = "https://api.github.com/graphql"
    output_filename = f'{organization}_github_contributor_permissions_report.csv'
    pages_to_get = 25
    sort_columns = True
    sort_rows = True

    client = get_graphql_client(graphql_endpoint)

    # make graphql call to get members list
    members_dict = {}
    total_members_returned = get_members(client, members_dict, organization, pages_to_get)

    # make graphql call to get repository list
    list_of_repos = []
    total_repos_returned = get_repos_and_perms(client, list_of_repos, organization, pages_to_get)

    # collect repositories and permissions for each user and generate a list of column headings
    addtl_column_headings = process_repo_list(list_of_repos, members_dict)

    if sort_columns:
        column_headings = sorted(addtl_column_headings, key=lambda i: i.casefold())

    # add repo.names to base csv columns
    csv_columns = base_csv_columns + addtl_column_headings

    '''
    Until this point the members_dict has used 'login' as a key so that each member could be updated
    with newly retrieved repo data. We need to flatten it before generating the CSV file.
    '''
    csv_ready_list = []
    for key, value in members_dict.items():
        csv_ready_list.append(value)

    # sort by 'login' key
    if sort_rows:
        csv_ready_list = sorted(csv_ready_list, key=lambda i: i['login'].casefold())

    print(f"Completed audit of {total_members_returned} contributors and {total_repos_returned} repositories.")
    generate_csv_file(csv_ready_list, output_filename)




