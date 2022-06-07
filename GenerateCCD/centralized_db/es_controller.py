import json
# from botocore.vendored import requests
import requests
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

URL=os.environ["ES_URL"]
headers = {'Authorization': os.environ["ES_AUTHORIZATION"],'content-type': 'application/json'}

def get_root_companies_id(es_payload):
  
    url = URL+"patient_identifier/_search/"
    
    payload = json.dumps(es_payload)
      
      ## Use elasticsearch-python library to sent request to ES.
      # response = es.search(index="es_demo.udm_landing.patient_identifier", body=payload)
      
      ## Use python request module to send post request to ES.
    response = requests.post( url, headers=headers, data=payload)
      
      # return response
      ## Convert ES response into text
    return (response.text)
