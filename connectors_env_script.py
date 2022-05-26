# this script is responsible to create, pause, restart, delete sink and source connectors.

import requests
from datetime import datetime
import copy


def pause_sink_connectors(args, sink_connectors, data):
    sink_connect_base_url = data["connectors_url"]["SINK_CONNECTOR_WORKER_HOST_BASE_URL"]
    for sink_con in sink_connectors:
        try:
            pause_connector_url = sink_connect_base_url + data["connectors_url"][
                "SINK_CONNECTOR_PAUSE_URL"].format(
                sink_con)
            response = requests.put(pause_connector_url)
            if response.status_code == 202:
                print("Paused connector successfully with Code: ", response.status_code)
                print("Connector: ", pause_connector_url)
            else:
                print("Error Occurred while pausing of sink connector with exit code", response.status_code)
                print("Connector: ", pause_connector_url)
                exit(1)
        except requests.exceptions.RequestException as e:
            print("Url : " + pause_connector_url)
            print('Exception occurred : {}'.format(e))
            exit(1)


def resume_sink_connectors(args, sink_connectors, data):
    sink_connect_base_url = data["connectors_url"]["SINK_CONNECTOR_WORKER_HOST_BASE_URL"]
    for sink_con in sink_connectors:
        try:
            resume_connector_url = sink_connect_base_url + data["connectors_url"][
                "SINK_CONNECTOR_RESUME_URL"].format(
                sink_con)
            response = requests.put(resume_connector_url)
            if response.status_code == 202:
                print("Resume connector successfully with Code: ", response.status_code)
                print("Connector: ", resume_connector_url)
            else:
                print("Error Occurred while resuming of sink connector with exit code", response.status_code)
                print("Connector: ", resume_connector_url)
                exit(1)
        except requests.exceptions.RequestException as e:
            print("Url : " + resume_connector_url)
            print('Exception occurred : {}'.format(e))
            exit(1)


def create_source_connector(args, mongo_collections, data):
    tenant_list = list(args.tenants.split(","))
    mongo_collections_as_string = ''
    for tenant in tenant_list:
        for mongo_col in mongo_collections:
            mongo_collections_as_string += mongo_col.format(tenant) + ','
    mongo_collections_as_string = mongo_collections_as_string[:-1]
    source_connector = copy.deepcopy(data["connectors_payload"]["source_connector_payload"])
    source_connector["name"] = source_connector["name"].format(args.env, datetime.now().date()) + "_" + str(datetime.now().time())
    source_connector["config"]["name"] = source_connector["name"]
    source_connector["config"]["collection.whitelist"] = mongo_collections_as_string
    create_source_connector_url = data["connectors_url"]["SOURCE_CONNECTOR_WORKER_HOST_BASE_URL"]
    try:
        response = requests.post(create_source_connector_url, json=source_connector)
        if response.status_code == 201:
            print("Created source connector successfully for code", response.status_code)
            print("Connector Name: ", source_connector["name"])
        else:
            print("Error Occurred while creating source connector with exit code", response.status_code)
            print("Connector payload: ", source_connector)
            exit(1)
        return source_connector["name"]
    except requests.exceptions.RequestException as e:
        print("name : " + source_connector["name"])
        print('Exception occurred : {}'.format(e))
        exit(1)


def create_sink_connector(args, data):
    tenant_list = list(args.tenants.split(","))
    sink_connector = copy.deepcopy(data["connectors_payload"]["sink_connector_payload"])
    sink_connector["name"] = sink_connector["name"].format(args.env, datetime.now().date()) + "_" + str(datetime.now().time())
    sink_connector["config"]["name"] = sink_connector["name"]
    sink_connector["config"]["topics.regex"] = sink_connector["config"]["topics.regex"].format(args.env)
    create_sink_connector_url = data["connectors_url"]["SINK_CONNECTOR_WORKER_HOST_BASE_URL"] + "/connectors"
    try:
        response = requests.post(create_sink_connector_url, json=sink_connector)
        if response.status_code == 201:
            print("Created sink connector successfully for code", response.status_code)
            print("Connector Name: ", sink_connector["name"])
        else:
            print("Error Occurred while creating sink connector with exit code", response.status_code)
            print("Connector payload: ", sink_connector)
            exit(1)
        return sink_connector["name"]
    except requests.exceptions.RequestException as e:
        print("name : " + sink_connector["name"])
        print('Exception occurred : {}'.format(e))
        exit(1)


def delete_sink_connectors(args, sink_connectors, data):
    sink_connect_base_url = data["connectors_url"]["SINK_CONNECTOR_WORKER_HOST_BASE_URL"]
    for sink_con in sink_connectors:
        try:
            delete_connector_url = sink_connect_base_url + data["connectors_url"][
                "SINK_CONNECTOR_DELETE_URL"].format(
                sink_con)
            response = requests.delete(delete_connector_url)
            if response.status_code == 204:
                print("Delete sink connector successfully with Code: ", response.status_code)
                print("sink Connector: ", delete_connector_url)
            else:
                print("Error Occurred while deleting of sink connector with exit code", response.status_code)
                print("Connector: ", delete_connector_url)
                exit(1)
        except requests.exceptions.RequestException as e:
            print("sink url : " + delete_connector_url)
            print('Exception occurred : {}'.format(e))
            exit(1)


def delete_source_connectors(args, source_connectors, data):
    sink_connect_base_url = data["connectors_url"]["SOURCE_CONNECTOR_WORKER_HOST_BASE_URL"]
    for sink_con in source_connectors:
        delete_connector_url = sink_connect_base_url + "/" + sink_con
        print(delete_connector_url)
        try:
            response = requests.delete(delete_connector_url)
            if response.status_code == 204:
                print("Deletion of source connector successfully with Code: ", response.status_code)
                print("Source Connector: ", delete_connector_url)
            else:
                print("Error Occurred while deletion of source connector with exit code", response.status_code)
                print("Connector: ", delete_connector_url)
                exit(1)
        except requests.exceptions.RequestException as e:
            print("Source url : " + delete_connector_url)
            print('Exception occurred : {}'.format(e))
            exit(1)