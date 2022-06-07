import json
import boto3
import os
import logging
import datetime


from botocore.exceptions import ClientError
from es_controller import get_root_companies_id

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

CCD_BUCKET=os.environ["CCD_BUCKET"]
GENERATECCD_INTERNAL_LAMBDA_FUNCTION = os.environ["GENERATECCD_INTERNAL_LAMBDA_FUNCTION"]

def get_object(file_path,file_name):
    
    KEY = file_path+file_name
    
    s3_response = s3_client.get_object(
        Bucket=CCD_BUCKET,
        Key=KEY
    )
    
    return s3_response
    
def ccd_head_object(s3_path,file_name):
    
    
    Key = s3_path+file_name
    try:
        response = s3_client.head_object(Bucket=CCD_BUCKET,Key=Key)
        
    except ClientError as ex:
        
        if ex.response['Error']['Message'] == 'Not Found':
            logger.info('No object found - returning empty')
            return "No Key found"
        else:
            raise
    return response


def lambda_handler(event, context):
    # TODO implement

    Error_message = []
    
    if "Transaction_id" not in event["body"]:
        Error_message.append({"Message":"Transaction_id not found in request"})
        
    if "file_name" not in event["body"]:
        Error_message.append({"Message":"file_name not found in request"})
    
    if "Common_key" not in event["body"] and "aggregate_id" not in event["body"] and "empi" not in event["body"]:
        Error_message.append({"Message":"Common_key/aggregate_id/empi not found in request"})
        
    if "s3_path" not in event["body"]:
        Error_message.append({"Message":"s3_path not found in request"})
    
    if len(Error_message) > 0:
        error_response = {
            
            "Error_message":Error_message,
            "Description":"All 4 fields(Transaction_id,  Common_key/aggregate_id/empi, s3_path, file_name) are required"
        }
        
        return{
            "statusCode": 404,
            "headers": {
            "Content-Type": "*/*"
            },
            "body": json.dumps(error_response)
        }
        # raise Exception(json.loads(json.dumps(error_response)))
    event = json.loads(event["body"])
    logger.info("Event>> {}".format(event))	
    transaction_id = event["Transaction_id"]
    file_name = event["file_name"]
    
    client_s3_path = event["s3_path"]
    
    if "." in file_name:
        file_list = file_name.split(".")
        del file_list[-1]
        
        file_name = ".".join(file_list)
    
    current_time = datetime.datetime.now()
    current_time_formated = current_time.strftime('%Y%m%d%H')
    
    S3_path = "s3://"+CCD_BUCKET+"/CCDGen/"+str(current_time_formated)+"/"+transaction_id

        
    ### Response related data
    ## Generate Presigned URL
    
    
    ## Get Root Companies ID
    es_entity_data = []
    
    es_search_payload = {"size" : 1,"query": {"match": {}},"fields": ["ROOT_COMPANIES_ID"],"_source": False}
    
    if "aggregate_id" in event:
        es_search_payload["query"]["match"] = {"AGGREGATE_ID": event["aggregate_id"]}
        
    elif "empi" in event:
        es_search_payload["query"]["match"] = {"EMPI_ID": event["empi"]}
        
    elif "Common_key" in event:
        es_search_payload["query"]["match"] = {"EMPI_ID": event["Common_key"]}
        event["empi"] = event["Common_key"]
        del event["Common_key"]
        
    es_response = get_root_companies_id(es_search_payload)
    
    es_response_payload = json.loads(es_response)
    es_entity_data.extend(es_response_payload["hits"]["hits"])

    if len(es_entity_data) == 0:
        if "aggregate_id" in event:
            es_error_response = "aggregate_id not found in records"
        else:
            es_error_response = "Common Key/empi not found in records"
        return{
            "statusCode": 404,
            "headers": {
            "Content-Type": "application/json"
            },
            "body": json.dumps({"Error":es_error_response})
        }
        
    root_companies_id = es_entity_data[0]['fields']["ROOT_COMPANIES_ID"][0]
    event["root_companies_id"] = root_companies_id
    
    ## Check CCD File
    
    s3_object_path = S3_path
            
    s3_object_path_list = s3_object_path.split("/")
        
    del s3_object_path_list[:3]
    
    s3_object_path = '/'.join(s3_object_path_list)

    if s3_object_path[-1] == "/":
        pass
    else:
        s3_object_path = s3_object_path+"/"
     
    
    check_file_name = event["file_name"]
    
    if "." in check_file_name:
        pass
    
    else:
        check_file_name = check_file_name+".xml"
    
    s3_head_object = ccd_head_object(s3_object_path,check_file_name)

    response = {}
    if s3_head_object == "No Key found":
    
        event_payload = event
        lambda_event = {"body":json.dumps(event_payload)}
        lambda_payload = json.dumps(lambda_event).encode('utf-8')
        lambda_client.invoke(
            FunctionName=GENERATECCD_INTERNAL_LAMBDA_FUNCTION,
            InvocationType='Event',
            Payload=lambda_payload
        )
        response = {"Message":"CCD Generation is in progress"}
    
    else:

        is_raw=False
        if "is_raw" in event:
            is_raw=event["is_raw"]
            
        if is_raw:
            logger.info("s3_object_path:{0}+++check_file_name:{1}".format(s3_object_path,check_file_name))
            get_object_response = get_object(s3_object_path,check_file_name)
            
            logger.info("object response:{0}".format(get_object_response))
            s3_body = get_object_response['Body'].read()
            
            return {
                'statusCode': 200,
                "headers": {
                    "Content-Type": "application/xml"
                    },
                'body': s3_body.decode('utf-8')
            }
                    
        if "." in file_name:
            file_list = file_name.split(".")
            del file_list[-1]
            
            file_name = ".".join(file_list)
        
        client_s3_path = S3_path
        if client_s3_path[-1] == "/":
            xml_file_name = client_s3_path+file_name+".xml"
            htm_file_name = client_s3_path+file_name+".htm"
            json_file_name = client_s3_path+file_name+"-data.json"
        else:
            xml_file_name = client_s3_path+"/"+file_name+".xml"
            htm_file_name = client_s3_path+"/"+file_name+".htm"
            json_file_name = client_s3_path+"/"+file_name+"-data.json"
        
        response["Transaction_id"] = transaction_id
        if "empi" in event:
            response["Common_key"] = event["empi"]
        elif "aggregate_id" in event:
            response["Common_key"] = event["aggregate_id"]
        response["file_name"] = file_name
        response["xml_file_name"]  = xml_file_name
        response["htm_file_name"] = htm_file_name
        response["json_file_name"] = json_file_name
     
    return{
            "statusCode": 200,
            "headers": {
            "Content-Type": "application/json"
            },
            "body": json.dumps(response)
        }
