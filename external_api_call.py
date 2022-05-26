# this script is responsible verify the history run and run stream application in normal mode.\
import sys

import requests
import base64

def get_secrets(tenant, args, data):
    env = args.env
    user_name = data['api_username']
    password = data['api_password']
    may_i_service_base_url = data['mayiServiceBaseUrl'].format(env)
    print("Executing for tenant: ", tenant)
    # Creating userId's at run time based on tenants passed.
    uid = "supportops@{0}.com".format(tenant)

    def get_api(tid, uid, may_i_service_base_url, user_name, password):
        userPassword = user_name + ':' + password
        encodedBytes = base64.b64encode(userPassword.encode())
        encodedStr = str(encodedBytes, 'utf-8')
        # encodedStr = str(encodedBytes).encode("utf-8")
        auth = 'Basic ' + encodedStr
        URL = may_i_service_base_url + '/dataStore/keyDetails'
        PARAMS = {'userId': uid, 'tenantId': tid}
        HEADERS = {'Authorization': auth, 'Accept': 'application/json', 'userId': uid, 'tenantId': tid}
        print("Calling mayIservice", may_i_service_base_url)
        response = requests.get(URL, headers=HEADERS, params=PARAMS)
        json_content = response.json()
        token = json_content["token"]
        key = json_content["apiKey"]
        return [key, token]

    try:
        return get_api(tenant, uid, may_i_service_base_url, user_name, password)

    except Exception as e:
        print("error occurred while calling may i service. ", e)
        print('Exception occurred : {}'.format(e))
        sys.exit(1)
