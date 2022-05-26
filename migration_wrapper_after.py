import argparse
import json
import sys
import os
import connectors_env_script
import external_api_call
import requests
import time
import RundeckJobRemote
import MailNotification


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


# =============================data needed for the input START=======================
sinkConnectors: list = []
index_name_for_count: list = []
collection_name_for_count: list = []
if "header" in args.modules:
    sinkConnectors = list(data[args.env]["connectors"]['connector_list']["header_sink_connector"]["CONNECTOR_NAME"])
    index_name_for_count = data[args.env]["index_name_for_count"]["header"]
    collection_name_for_count = data[args.env]["collection_name_for_count"]["header"]
if "workspace" in args.modules:
    sinkConnectors.extend(data[args.env]["connectors"]['connector_list']["ws_sink_connector"]["CONNECTORS_NAME"])
    index_name_for_count.extend(data[args.env]["index_name_for_count"]["workspace"])
    collection_name_for_count.extend(data[args.env]["collection_name_for_count"]["workspace"])
# =============================data needed for the input END==================================

#Restart the stream application
RundeckJobRemote.callRemoteRundeckJob("start_streamApplication");

# resume sink connectors
connectors_env_script.resume_sink_connectors(args, sinkConnectors, data[args.env]["connectors"])
print("============ waiting for the sink connectors to push the data to elastic indexes.==================")
time.sleep(360)

# starting the count of mongo and elastic
data_in_mongo_map = {}
data_in_elastic_map = {}
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
for index in index_name_for_count:
    index_count_url = data['dev']['elastic_count_api'].format(env, key,index)
    print("Calling index count API: ", index_count_url)
    try:
        response = requests.post(index_count_url, headers=headers, json=tenant_ids)
        json_content = response.json()
        data_in_elastic_map.update(json_content)
        if response.status_code == 200:
            print("API executed successfully for count index: " + index, ",with exit code: ", response.status_code)
        else:
            print("Error Occurred for API count index " + index, ",with exit code:", response.status_code)
            sys.exit(1)
    except Exception as e:
        print("Error occurred while calling count API/s for elastic indexes.", e)
        print('Exception occurred : {}'.format(e))
        sys.exit(1)

print("=================================================================================================")
for collection in collection_name_for_count:
    mongo_count_url = data['dev']['mongo_count_api'].format(env, key, collection)
    print("Calling mongo count API: ", mongo_count_url)
    try:
        response = requests.post(mongo_count_url, headers=headers, json=tenant_ids)
        json_content = response.json()
        data_in_mongo_map.update(json_content)
        if response.status_code == 200:
            print("API executed successfully for count mongo: " + collection, ",with exit code: ", response.status_code)
        else:
            print("Error Occurred for API count mongo " + collection, ",with exit code:", response.status_code)
            sys.exit(1)
    except Exception as e:
        print("Error occurred while calling count API/s for mongo collection.", e)
        print('Exception occurred : {}'.format(e))
        sys.exit(1)

isCountMatched = False
for i in data_in_mongo_map:
    elasticName = "aos"+env+"."+i
    if "line.view" not in elasticName:
        elasticName = elasticName + ".es"
    mongo_count = data_in_mongo_map.get(i)
    elastic_count = data_in_elastic_map.get(elasticName)
    if elastic_count != mongo_count:
        isCountMatched = True
        diff = abs(elastic_count - mongo_count)
        print("=========================================================")
        print("Elastic index :=> " + elasticName + ":" + str(elastic_count))
        print("Mongo Collection :=> " + i + ":" + str(mongo_count))
        print("Difference of records :=> " + str(diff))

if not isCountMatched:
    print("Please try after some time.")
    raise Exception("Mongo and Elastic count not matched. Please run after sometime.")
    exit(1)

# delete the temporary sink and source connectors after count matches
connectors_env_script.delete_source_connectors(args, data["source_connectors_to_delete"], data[args.env]["connectors"])
if "workspace" in args.modules:
    connectors_env_script.delete_sink_connectors(args, data["sink_connectors_to_delete"], data[args.env]["connectors"])

data["is_step_complete"] = "start"
with open(envInputFile, 'w') as f:
    f.write(json.dumps(data, indent=4))
    f.close()


# Call Rundeck Job normal flow + restart the application
RundeckJobRemote.callRemoteRundeckJob("PlannerStream_changeToNormalFlag");

# send success notification mail.
MailNotification.sendMailNotification('devfox, aosonair', 'abhijeets@sintecmedia.com')

print("==============Migration Process completed successfully.=======================")

