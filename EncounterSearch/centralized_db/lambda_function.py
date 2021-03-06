import json
from es_controller import get_docs,get_patient_docs

response_content_type = "application/json"
def lambda_handler(event, context):
    # TODO implement
    
    event = json.loads(event["body"])
    print ("event is >> ",event)
    
    if "START_ADMIT_DATE" in event and "END_ADMIT_DATE" not in event:
     
        return {
        'statusCode': 404,
        "headers": {
            "Content-Type": response_content_type
            },
        'body': json.dumps("END_ADMIT_DATE is required")
    }
     
    if "START_ADMIT_DATE" not in event and "END_ADMIT_DATE" in event:
     
        return {
        'statusCode': 404,
        "headers": {
            "Content-Type": response_content_type
            },
        'body': json.dumps("START_ADMIT_DATE is required")
    }
    
    es_entity_data = []
    es_query_payload = { "size" : 10000,"query":{"bool":{}}}
    
    es_query_payload["_source"] = ["ENTITY_ID","ENCOUNTER_ID","DOCUMENT_ID", "PATIENT_ID_EXT", "PATIENT_ID_ROOT", "ACCOUNT_NUMBER",
    "ENCOUNTER_DATE", "ADMISSION_TYPE_NAME", "ADMIT_DATE", "DISCHARGE_DATE", "PATIENT_CLASS", "CLASS_CODE", "CLINIC", 
    "HOSPITAL_SERVICE_PROVIDER", "LOCATION_CODE", "LOCATION_CODE_DISPLAY_NAME", "LOCATION_NAME", "ROOT_COMPANIES_ID", 
    "ROW_CREATED_ON", "ROW_UPDATED_ON", "DOCUMENT_TYPE", "SERIAL_COUNTER", "VALID_FLAG", "CONFIDENTIALITY_FLAG", "SELF_PAY_FLAG", "AGGREGATE_ID"]
    
    es_query_must = [{"match": {"VALID_FLAG": "1"}},{"match": {"__deleted": "false"}}]
    
    if "START_ADMIT_DATE" in event and "END_ADMIT_DATE" in event:
        es_query_must.append({"range": {"ADMIT_DATE":{"gte":event["START_ADMIT_DATE"],"lte":event["END_ADMIT_DATE"]} }})
        
    if "CLINIC" in event:
        es_query_must.append({"match": {"CLINIC": event["CLINIC"]}})
        
    if "LOCATION_NAME" in event:
        es_query_must.append({"match": {"LOCATION_NAME": event["LOCATION_NAME"]}})
        
    if "DOCUMENT_TYPE" in event:
        es_query_must.append({"match": {"DOCUMENT_TYPE": event["DOCUMENT_TYPE"]}})
        
    if "PATIENT_ID_ROOT" in event:
        es_query_must.append({"match": {"PATIENT_ID_ROOT": event["PATIENT_ID_ROOT"]}})
        
    data_response_list = []
    
    es_query_payload["query"]["bool"]["must"] = es_query_must
    
    es_response = get_docs(es_query_payload)
    
    encounter_response_payload = json.loads(es_response)
    
    if encounter_response_payload["hits"]["total"]["value"] > 0:
        for i in range(0,len(encounter_response_payload["hits"]["hits"])):
            
            patient_list = {}
            es_entity_data.append(encounter_response_payload["hits"]["hits"][i]["_source"])

            patient_list["encounter"] = encounter_response_payload["hits"]["hits"][i]["_source"]
            patient_aggregate_id = encounter_response_payload["hits"]["hits"][i]["_source"]["AGGREGATE_ID"]
            
            
            es_pd_query_payload = { 
                "size" : 1,
                "query":{
                    "bool":{
                        "must":
                            [
                                {"match": {"__deleted": "false"}},
                                {"match": {"AGGREGATE_ID": patient_aggregate_id}},
                                {"match": {"VALID_FLAG": "1"}}
                            ]
                    }
                }
            }
            
            es_pd_query_payload["_source"] = ["GIVEN_NAME","FAMILY_NAME","DATE_OF_BIRTH","ADMINISTRATIVE_GENDER_CODE","STREET_ADDRESS","CITY","STATE","POSTAL_CODE"]
            
            es_pd_response = get_patient_docs(es_pd_query_payload)

            es_pd_response_payload = json.loads(es_pd_response)
            if es_pd_response_payload["hits"]["total"]["value"] > 0:
                patient_list["patient_demographics"] = es_pd_response_payload["hits"]["hits"][0]["_source"]
                
            data_response_list.append(patient_list)

    data_response_payload = json.dumps(data_response_list)
    
    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": response_content_type
            },
        'body': data_response_payload
    }
