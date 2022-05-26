# this script is responsible to clean, create, delete ES indexes for history run.

import requests
import sys


# delete existing index
def delete_indexes(args, target_indexes, data):
    tenant_list = list(args.tenants.split(","))
    elastic_api_url = data['elastic_api']['api_urls']['generic_api_url']
    try:
        for tenant in tenant_list:
            for index in target_indexes:
                index_per_tenant = index.format(args.env, tenant)
                delete_api_url = elastic_api_url + index_per_tenant
                response = requests.delete(delete_api_url)
                if response.status_code == 200 or response.status_code == 404:
                    print("Delete API executed successfully with Code: ", response.status_code)
                    print("Index: ", index_per_tenant)
                else:
                    print("Error Occurred while deleting the elastic index with exit code", response.status_code)
                    print("Index: ", index_per_tenant)
                    sys.exit(1)
    except requests.exceptions.RequestException as e:
            print("Index: ", index_per_tenant)
            print('Exception occurred : {}'.format(e))
            exit(1)
    else:
        print("Successfully completed deleting the elastic index.")


# create the index with mapping
def create_indexes(args, target_indexes, data):
    tenant_list = list(args.tenants.split(","))
    elastic_api_url = data['elastic_api']['api_urls']['generic_api_url']
    try:
        for tenant in tenant_list:
            for index in target_indexes:
                mapping = data["_mappings"][index]
                index_per_tenant = index.format(args.env, tenant)
                create_index_url = elastic_api_url + index_per_tenant
                response = requests.put(create_index_url, json=mapping)
                if response.status_code == 200:
                    print("Create API executed successfully for code", response.status_code)
                    print("Index: ", index_per_tenant)
                else:
                    print("Error Occurred while creating the elastic index with exit code", response.status_code)
                    print("Index: ", index_per_tenant)
                    sys.exit(1)
    except requests.exceptions.RequestException as e:
        print("Index: ", index_per_tenant)
        print('Exception occurred : {}'.format(e))
        exit(1)
    else:
        print("Successfully completed creating of the elastic indexes.")


# create the index with mapping
def delete_indexes_data(args, target_indexes, data):
    tenant_list = list(args.tenants.split(","))
    elastic_api_url = data['elastic_api']['api_urls']['purgeData_POST']
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": "application/json"}
    try:
        for tenant in tenant_list:
            for index in target_indexes:
                index_per_tenant = index.format(args.env, tenant)
                delete_index_data_url = elastic_api_url.format(index_per_tenant)
                print(delete_index_data_url)
                response = requests.post(delete_index_data_url, data={"query": {"match_all": {}}},headers=headers)
                if response.status_code == 200:
                    print("Delete dat API executed successfully for code", response.status_code)
                    print("Index: ", index_per_tenant)
                else:
                    print("Error Occurred while deleting data for elastic index with exit code", response.status_code)
                    print("Index: ", index_per_tenant)
                    sys.exit(1)
    except Exception as e:
        print("error occurred while deleting of indexes. ", e)
        print('Exception occurred : {}'.format(e))
        sys.exit(1)
    else:
        print("Successfully completed creating of the elastic indexes.")