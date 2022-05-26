import argparse
import json
import sys
import os
import time
import kafka_env_script
import es_index_env_script
import connectors_env_script
import RundeckJobRemote


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

if "connector_creation" not in data["is_step_complete"]:
    print("================from start===================================")
    # =============================data needed for the input START=======================
    targetTopics: list = []
    target_indexes: list = []
    sinkConnectors: list = []
    mongo_collections: list = []
    topics_per_tenant_to_execute: list = []

    if "header" in args.modules:
        targetTopics = list(data[env]["kafka"]['planner_kafka_topics']["header_kafka_topics"])
        target_indexes = list(data[args.env]["elastic"]['es_indexes']["planner_header_index"])
        sinkConnectors = list(data[args.env]["connectors"]['connector_list']["header_sink_connector"]["CONNECTOR_NAME"])
        mongo_collections = list(data[args.env]["connectors"]['mongo_collections_to_whitelist']["header_collections"])
    if "workspace" in args.modules:
        targetTopics.extend(data[env]["kafka"]['planner_kafka_topics']["ws_kafka_topics"])
        target_indexes.extend(data[args.env]["elastic"]['es_indexes']["planner_ws_index"])
        sinkConnectors.extend(data[args.env]["connectors"]['connector_list']["ws_sink_connector"]["CONNECTORS_NAME"])
        mongo_collections.extend(list(data[args.env]["connectors"]['mongo_collections_to_whitelist']["ws_collections"]))

    for tenant in tenantList:
        for topic in targetTopics:
            topics_per_tenant_to_execute.append(topic.format(env, tenant))
    # =============================data needed for the input END==================================

    #stop stream application and change the migration flag
    RundeckJobRemote.callRemoteRundeckJob("PlannerStream_changeMigrationFlag");

    # pausing all existing sink connectors
    connectors_env_script.pause_sink_connectors(args,sinkConnectors, data[args.env]["connectors"])

    # clean all the topics data
    kafka_env_script.purge_topics_data(args, topics_per_tenant_to_execute, data[args.env]["kafka"])
    time.sleep(200)

    # is topics data is cleaned
    is_topic_data_purged = True
    is_topic_data_purged = kafka_env_script.is_kafka_topics_purged(args, topics_per_tenant_to_execute, data[args.env]["kafka"])
    print("Is all the data purged: "+str(is_topic_data_purged))

    if not is_topic_data_purged:
        raise Exception("Topics found with data. Please wait and rerun after sometime.")
        exit(1)

    # switch back default retention period
    kafka_env_script.set_retention_period_7_days(args, topics_per_tenant_to_execute, data[args.env]["kafka"])
    print("waiting for the retention period to default.....")
    time.sleep(100)

    # delete indexes
    es_index_env_script.delete_indexes(args, target_indexes, data[args.env]["elastic"])
    time.sleep(5)

    # create index
    es_index_env_script.create_indexes(args, target_indexes, data[args.env]["elastic"])
    time.sleep(5)

    # create sink n source connector
    source_connectors_to_delete = []
    sink_connectors_to_delete = []
    if "header" in args.modules:
        source_connector_name = connectors_env_script.create_source_connector(args, mongo_collections, data[args.env]["connectors"])
        source_connectors_to_delete.append(source_connector_name)
        data["source_connectors_to_delete"] = source_connectors_to_delete
    if "workspace" in args.modules:
        sink_connector_name = connectors_env_script.create_sink_connector(args, data[args.env]["connectors"])
        sink_connectors_to_delete.append(sink_connector_name)
        data["sink_connectors_to_delete"] = sink_connectors_to_delete

    data["is_step_complete"] = "connector_creation"
    with open(envInputFile, 'w') as f:
        f.write(json.dumps(data, indent=4))
        f.close()
else:
    print("===================skipping the pre stage of migration===========================")
    topics_per_tenant_to_execute: list = []
    if "header" in args.modules:
        targetTopics = list(data[env]["kafka"]['planner_kafka_topics']["header_kafka_topics"])
    if "workspace" in args.modules:
        targetTopics.extend(data[env]["kafka"]['planner_kafka_topics']["ws_kafka_topics"])
    for tenant in tenantList:
        for topic in targetTopics:
            topics_per_tenant_to_execute.append(topic.format(env, tenant))
    is_topic_data_purged = kafka_env_script.is_kafka_topics_purged(args, topics_per_tenant_to_execute,data[args.env]["kafka"])
    print("Is all the data purged: " + str(is_topic_data_purged))
    if not is_topic_data_purged:
        print("Wait till data purged. Cannot proceed to next step.")
        raise Exception("Topics found with data. Please wait and rerun after sometime.")
        exit(1)
    else:
        print("Proceeding to next step.")
