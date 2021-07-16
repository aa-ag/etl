############------------ IMPORTS ------------############
import sys
import gc
import datetime
import requests
import pandas as pd
from pandas.io.json import json_normalize
import settings

############------------ GLOBAL VARIABLE(S) ------------############
url = settings.end_point

############------------ FUNCTION(S) ------------############
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
    print(test_endpoint())
    # <Response [200]>