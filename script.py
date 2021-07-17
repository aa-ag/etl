############------------ IMPORTS ------------############
### Python
import sys
import gc
import datetime
import requests
import pandas as pd
from pandas.io.json import json_normalize
import json
### Big Query
from google.cloud import bigquery
from google.oauth2 import service_account
### Project
import settings

############------------ GLOBAL VARIABLE(S) ------------############
url = settings.gbfs_end_point
path_to_bigquery_key = settings.bigquery_account_key_path


############------------ FUNCTION(S) ------------############
def query_stackoverflow():
    '''
     sample code from docs to test connection
     modified to use credentials
     https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries
    '''
    
    credentials = service_account.Credentials.from_service_account_file(
        path_to_bigquery_key, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    query_job = client.query(
        """
        SELECT
          CONCAT(
            'https://stackoverflow.com/questions/',
            CAST(id as STRING)) as url,
          view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 3"""
    )

    results = query_job.result()  # Waits for job to complete.

    for row in results:
        print("{} : {} views".format(row.url, row.view_count))


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
    # big_query_authentication()
    # <google.cloud.bigquery.client.Client object at 0x7fb442242f40>
    query_stackoverflow()
    '''
    https://stackoverflow.com/questions/35159967 : 98375 views
    https://stackoverflow.com/questions/22879669 : 93709 views
    https://stackoverflow.com/questions/10604135 : 92234 views
    '''