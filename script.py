############------------ IMPORTS ------------############
### Python
import sys
import gc
from datetime import date, datetime, timezone
import requests
import pandas as pd
from pandas import json_normalize
import json
### Big Query
from google.cloud import bigquery
from google.oauth2 import service_account
### Project
import settings

############------------ GLOBAL VARIABLE(S) ------------############
### settings.py
gbfs_station_information = settings.gbfs_end_point
path_to_bigquery_key = settings.bigquery_account_key_path
bq_ds_tbl_id = settings.big_query_data_set_and_table_ids
project_id = settings.projectid
### key.json
key_file = open('etlproject.json')
key = json.load(key_file)


############------------ FUNCTION(S) ------------############
### GBFS API
def generate_request():
    '''
     creates and makes a request, 
     returns whether or not connection is successful
    '''
    global gbfs_station_information

    req = requests.get(gbfs_station_information)

    if req.status_code != 200:
        return "Nope"
        gc.collect()
        sys.exit()
    else:
        print("Server status hows new update\n")
        pass
    return req.json()


def generate_dataframe():
    '''
     first, generates request
     then, creates dataframe with data
    '''
    req = generate_request()
    
    last_updated = req['last_updated']
    dt_object = datetime.fromtimestamp(last_updated).strftime('%Y-%m-%d %H:%M:%S')
    print("Last updated: " + str(dt_object) + '\n')

    stations = req['data']['stations']
    df = json_normalize(stations)
    df['last_system_update_date'] = dt_object
    df['insertion_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("Data for insertion constructed\n")
    return df


def write_data_to_database():
    '''
     localizes variables `key` and `projectid`
     and writes data in form of dataframe to db
    '''
    global key
    global bq_ds_tbl_id

    df = generate_dataframe()

    df = df.rename(columns={'rental_uris.ios':'rental_uris_ios', 
    'rental_uris.android': 'rental_uris_android'})

    df.to_gbq(bq_ds_tbl_id, project_id)
    print("All set")


### Big Query
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


def test_data():
    '''
     testing data quality before loading it
     to table to avoid duplicates
    '''
    pass


############------------ DRIVER CODE ------------############
if __name__ == "__main__":
    # API
    # print(test_endpoint())
    # <Response [200]>

    ### Big Query
    # big_query_authentication()
    # <google.cloud.bigquery.client.Client object at 0x7fb442242f40>
    # generate_dataframe()
    # Last updated: 2021-07-18 11:42:45
    write_data_to_database()
