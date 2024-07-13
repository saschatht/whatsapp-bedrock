import boto3
import json
import os
from boto3.dynamodb.conditions import Key
from utils import (normalize_phone,get_config,whats_reply)
from botocore.exceptions import ClientError

from db_utils import query_gd, query

from file_utils import download_file

#import idempotency functions from aws power tools layer
from aws_lambda_powertools.utilities.idempotency import (
    idempotent_function,
    DynamoDBPersistenceLayer,
    IdempotencyConfig,
)

#initialize idempotency variables
IDEMPOTENCY_TABLE_NAME = os.getenv("IDEMPOTENCY_TABLE_NAME", "")

persistence_layer = DynamoDBPersistenceLayer(table_name=IDEMPOTENCY_TABLE_NAME)
idempotency_config = IdempotencyConfig(
    event_key_jmespath="[detail.TranscriptionJobName, detail.TranscriptionJobStatus]",
    raise_on_no_idempotency_key=True,
    expires_after_seconds=60 * 60 * 2,  # 2 hours
)


    
lambda_client = boto3.client('lambda')

table_name_active_connections = os.environ.get('whatsapp_MetaData')

key_name_active_connections = os.environ.get('ENV_KEY_NAME')
Index_Name = os.environ.get('ENV_INDEX_NAME')
whatsapp_out_lambda = os.environ.get('WHATSAPP_OUT')
LAMBDA_AGENT_TEXT = os.environ['ENV_LAMBDA_AGENT_TEXT']

client_s3 = boto3.client('s3')
dynamodb_resource=boto3.resource('dynamodb')
table = dynamodb_resource.Table(table_name_active_connections)

base_path="/tmp/"

#create idempotent function
@idempotent_function(
    persistence_store=persistence_layer,
    config=idempotency_config,
    data_keyword_argument="event",
)
def process_transcribed_text(event, context):
    keyvalue = os.environ.get('TranscribeTextFolder')
    s3bucket = os.environ.get('TranscribeBucket')
    s3object = keyvalue + 'texto_' + event['detail']['TranscriptionJobName']
    filename = 'texto_' + event['detail']['TranscriptionJobName']
    
    print(s3object)
    
    download_file(base_path,s3bucket, s3object, filename)
    value = filename.split("_")[-1].replace(".txt","").strip().replace(" ","")
    print(value)

    with open(base_path+filename) as f:
        message = f.readlines()

    messages_id = query_gd("jobName",table,value,Index_Name)[key_name_active_connections]
    whatsapp_data = query(key_name_active_connections,table,messages_id)
    message_json = json.loads(message[0])
    print(message_json)
    text = message_json["results"]['transcripts'][0]['transcript']
    phone = '+' + str(whatsapp_data['changes'][0]["value"]["messages"][0]["from"])
    phone_number = str(whatsapp_data['changes'][0]["value"]["messages"][0]["from"])
    whats_token = whatsapp_data['whats_token']
    phone_id = whatsapp_data['changes'][0]["value"]["metadata"]["phone_number_id"]

    try:
        print('REQUEST RECEIVED:', event)
        print('REQUEST CONTEXT:', context)
        print("PROMPT: ",text)

        whats_reply(whatsapp_out_lambda,phone, whats_token, phone_id, f"entendi: {text}", keyvalue)
        print("Whatsapp message sent")
        #remove this comment if you want to send voice notes to the agent!


        try:       

            response_3 = lambda_client.invoke(
                FunctionName = LAMBDA_AGENT_TEXT,
                InvocationType = 'Event' ,#'RequestResponse', 
                Payload = json.dumps({
                    'whats_message': text,
                    'whats_token': whats_token,
                    'phone': phone,
                    'phone_id': phone_id,
                    'messages_id': messages_id

                })
            )

            print(f'\nRespuesta:{response_3}')
        
            return response_3

            print("calling function LAMBDA_AGENT_TEXT")
            return({
                "body":"200 OK"
            })
        except ClientError as e:
            err = e.response
            error = err
            print(err.get("Error", {}).get("Code"))
            return f"Un error invocando {LAMBDA_AGENT_TEXT}"
    except Exception as error: 
        print('FAILED!', error)
        return True

def lambda_handler(event, context):
    
    print(event)
    idempotency_config.register_lambda_context(context)
    process_transcribed_text(event=event, context = context)
    
    return ({
                "body":"200 OK"
            })