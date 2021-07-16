############------------ IMPORTS ------------############
import sys
import gc
import datetime
import requests
import pandas as pd
from pandas.io.json import json_normalize
import settings
import json
from google.cloud import bigquery
from google.oauth2 import service_account

############------------ GLOBAL VARIABLE(S) ------------############
url = settings.gbfs_end_point
path_to_bigquery_key = settings.bigquery_account_key_path

json_file = open('etlproject.json', 'r')
api_credentials = json.load(json_file)

############------------ FUNCTION(S) ------------############
def big_query_authentication():
    '''
     set credentials from etlproject.json (key imported from Google Cloud)
     sets client using bigquery import object `Client`
    '''
    credentials = service_account.Credentials.from_service_account_file(
        path_to_bigquery_key, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    print(client)


def test_endpoint():
    '''
     creates and makes a request, 
     returns whether or not connection is successful
    '''
    req = requests.get(url)

    if req.status_code != 200:
        return "Nope"
        gc.collect()
        sys.exit()
    return "All good"


############------------ DRIVER CODE ------------############
if __name__ == "__main__":
    # print(test_endpoint())
    # <Response [200]>
    big_query_authentication()
    # <google.cloud.bigquery.client.Client object at 0x7fb442242f40>