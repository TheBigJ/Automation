import argparse
import json
import sys
import os
import time
import kafka_env_script
import es_index_env_script
import connectors_env_script
import external_api_call
import requests


def list_to_string(paramlist):
    str1 = ""
    for param in paramlist:
        str1 += '"--' + param + '"'
        str1 += ","
    str1 = str1[:-1]
    return str1


# get and set the passed parameters
parser = argparse.ArgumentParser(description='Personal information')
parser.add_argument('--env', dest='env', type=str, help='Name of the Environment')
parser.add_argument('--tenants', dest='tenants', type=str, help='Name of the Tenants')
parser.add_argument('--modules', dest='modules', type=str, help='Name of the modules')
args = parser.parse_args()

if None in vars(args).values():
    print("None Values found for the below Input Parameters:")
    res = []
    for ele in vars(args):
        if vars(args)[ele] is None:
            res.append(ele)
    resStr = list_to_string(res)
    print(resStr)
    print("1:--env {ENV}, 2:--tenants {TOPIC TENANTS}, 3:--modules {MODULES}")
    sys.exit(1)

baseDir = os.path.dirname(os.path.realpath(sys.argv[0]))
envInputFile = os.path.join(baseDir, 'planner_env_input.json')
print(" basedir" + baseDir)

if len(sys.argv) < 3:
    print("\nError: Missing Input Parameters for the Script from below list:")
    print("1:Env, 2:tenants, 3:moduels")
    sys.exit(1)

env = args.env
with open(envInputFile) as json_data:
    data = json.load(json_data)
tenantList = list(args.tenants.split(","))

# ====================starting kafka related process====================================
topics_per_tenant_to_execute: list = []
targetTopics: list = []
if "header" in args.modules:
    targetTopics = list(data[env]["kafka"]['planner_kafka_topics']["header_kafka_topics"])
if "workspace" in args.modules:
    targetTopics.extend(data[env]["kafka"]['planner_kafka_topics']["ws_kafka_topics"])
for tenant in tenantList:
    for topic in targetTopics:
        topics_per_tenant_to_execute.append(topic.format(env, tenant))

# purging of data
#kafka_env_script.purge_topics_data(args, topics_per_tenant_to_execute, data[args.env]["kafka"])
# creation of topic
#kafka_env_script.create_topics(topics_per_tenant_to_execute, data[args.env]["kafka"])
# describe a topic
#kafka_env_script.describe_topics(topics_per_tenant_to_execute,data[args.env]["kafka"])
# deletion of topic
#kafka_env_script.delete_topics(topics_per_tenant_to_execute, data[args.env]["kafka"])
#print("======================waiting for the topics data to be purged.======================================")
#time(100)
# getting kafka count
is_topic_data_purged = True
#is_topic_data_purged = kafka_env_script.is_kafka_topics_purged(args, topics_per_tenant_to_execute, data[args.env]["kafka"])
print(is_topic_data_purged)

if not is_topic_data_purged:
    raise Exception("Topics found with data. Please wait and rerun after sometime.")
    exit(1)
# =============================ending kafka related process============================================



# ==============================starting create delete purge indexes ====================================

target_indexes: list = []
if "header" in args.modules:
    target_indexes = list(data[args.env]["elastic"]['es_indexes']["planner_ws_index"])
if "workspace" in args.modules:
    target_indexes.extend(data[args.env]["elastic"]['es_indexes']["planner_header_index"])

#es_index_env_script.delete_indexes(args, target_indexes, data[args.env]["elastic"])
#time(10)
es_index_env_script.create_indexes(args, target_indexes, data[args.env]["elastic"])

#============================completed elastic indexes delete, create, purge.=============================

#===========================started connectors config, pausing, starting,stop and create.=================
"""
sinkConnectors: list = []
mongo_collections: list = []
if "header" in args.modules:
    sinkConnectors = list(data[args.env]["connectors"]['connector_list']["header_sink_connector"]["CONNECTOR_NAME"])
    mongo_collections = list(data[args.env]["connectors"]['mongo_collections_to_whitelist']["header_collections"])
if "workspace" in args.modules:
    sinkConnectors.extend(data[args.env]["connectors"]['connector_list']["ws_sink_connector"]["CONNECTORS_NAME"])
    mongo_collections.extend(list(data[args.env]["connectors"]['mongo_collections_to_whitelist']["ws_collections"]))

#connectors_env_script.pause_sink_connectors(args,sinkConnectors, data[args.env]["connectors"])
#connectors_env_script.resume_sink_connectors(args,sinkConnectors, data[args.env]["connectors"])
#source_connector_name = connectors_env_script.create_source_connector(args, mongo_collections, data[args.env]["connectors"])
#sink_connector_name = connectors_env_script.create_sink_connector(args,data[args.env]["connectors"])
#===========================completed connectors config, pausing, starting,stop and create.=================

source_connectors_to_delete.append(source_connector_name)
sink_connectors_to_delete.append(sink_connector_name)
data["is_step_complete"] = "started"
data["source_connectors_to_delete"] = source_connectors_to_delete
data["sink_connectors_to_delete"] = sink_connectors_to_delete
with open(envInputFile, 'w') as f:
    f.write(json.dumps(data, indent=4))
    f.close()
"""
#===============================starting the count of mongo and elastic===============================
"""
tenants = args.tenants.lower()
tenant_ids = list(tenants.split(","))
key = ""
token = ""
for tenant in tenant_ids:
    connection = external_api_call.get_secrets(tenant, args, data[env])
    key = connection[0]
    token = connection[1]
    break;

headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
for index in data[args.env]["index_name_for_count"]:
    index_count_url = data['dev']['elastic_count_api'].format(env, key,index)
    print("Calling index count API: ", index_count_url)
    try:
        response = requests.post(index_count_url, headers=headers, json=tenant_ids)
        print(response)
        json_content = response.json()
        print(json_content)
        if response.status_code == 200:
            print("API executed successfully for count index: " + index, ",with exit code: ", response.status_code)
        else:
            print("Error Occurred for API count index " + index, ",with exit code:", response.status_code)
    except:
        print("Error occurred while calling count API/s for elastic indexes.")
    else:
        print("successfully completed the count api/s for elastic index.")

print("=================================================================================================")
for collection in data[args.env]["collection_name_for_count"]:
    mongo_count_url = data['dev']['mongo_count_api'].format(env, key, collection)
    print("Calling mongo count API: ", mongo_count_url)
    try:
        response = requests.post(mongo_count_url, headers=headers, json=tenant_ids)
        print(response)
        json_content = response.json()
        print(json_content)
        if response.status_code == 200:
            print("API executed successfully for count mongo: " + collection, ",with exit code: ", response.status_code)
        else:
            print("Error Occurred for API count mongo " + collection, ",with exit code:", response.status_code)
    except:
        print("Error occurred while calling count API/s for mongo collection.")
    else:
        print("successfully completed the count api/s for mongo.")
"""