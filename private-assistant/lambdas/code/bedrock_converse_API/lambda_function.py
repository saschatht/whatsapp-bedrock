import json
import boto3
import os
import time
from datetime import datetime
from botocore.client import Config

from db_utils import query,save_item_ddb,update_items_out,update_item_session

from utils import (whats_reply)

from googlesearch import search
import requests
from bs4 import BeautifulSoup
from io import BytesIO
from pypdf import PdfReader

toolConfig = {'tools': [],
'toolChoice': {
    'auto': {},
    #'any': {},
    #'tool': {
    #    'name': 'get_weather'
    #}
    }
}
toolConfig['tools'].append({
        'toolSpec': {
            'name': 'get_weather',
            'description': 'Get weather of a location.',
            'inputSchema': {
                'json': {
                    'type': 'object',
                    'properties': {
                        'city': {
                            'type': 'string',
                            'description': 'City of the location'
                        },
                        'state': {
                            'type': 'string',
                            'description': 'State of the location'
                        }
                    },
                    'required': ['city', 'state']
                }
            }
        }
    })

toolConfig['tools'].append({
        'toolSpec': {
            'name': 'web_search',
            'description': 'Search a term in the public Internet. \
                Useful for getting up to date information.',
            'inputSchema': {
                'json': {
                    'type': 'object',
                    'properties': {
                        'query' : {
                            'type': 'string',
                            'description': 'Term to search in the Internet'
                        }
                    },
                    'required': ['search_term']
                }
            }
        }
    })

toolConfig['tools'].append({
        'toolSpec': {
            'name': 'knowledge_search',
            'description': 'Search a term in the internal knowledgebase. \
                Useful for getting up to date internal information.',
            'inputSchema': {
                'json': {
                    'type': 'object',
                    'properties': {
                        'query' : {
                            'type': 'string',
                            'description': 'Term to search in the Internet'
                        }
                    },
                    'required': ['search_term']
                }
            }
        }
    })

#required variables for converse API
#model ids
#'anthropic.claude-3-sonnet-20240229-v1:0'
#'anthropic.claude-3-haiku-20240307-v1:0'
#'cohere.command-r-plus-v1:0'
#'cohere.command-r-v1:0'
#'mistral.mistral-large-2402-v1:0'
model_id = os.environ.get('ENV_MODEL_ID')
region_id = os.environ.get('ENV_REGION_ID')
kb_id = os.environ.get('ENV_KB_ID')
print(f'Using modelId: {model_id}')
print('Using region: ', region_id)

bedrock = boto3.client(
    service_name = 'bedrock-runtime',
    region_name = region_id,
    )

bedrock_config = Config(connect_timeout=120, read_timeout=120, retries={'max_attempts': 0})
bedrock_agent_client = boto3.client("bedrock-agent-runtime",
                              config=bedrock_config)



#END required variables for converse API
#TODO: REMOVE REDUNDANT VARIABLES
dynamodb_resource=boto3.resource('dynamodb')
bedrock_client = boto3.client("bedrock-runtime")

whatsapp_out_lambda = os.environ.get('WHATSAPP_OUT')

table_name_active_connections = os.environ.get('whatsapp_MetaData')

table_session_active = dynamodb_resource.Table(os.environ['user_sesion_metadata'])
table_name_session = os.environ.get('session_table_history')

base_path="/tmp/"


table = dynamodb_resource.Table(table_name_active_connections)

class ToolsList3:
    #Define our get_weather tool function...
    def get_weather(self, city, state):
        #print(city, state)
        result = f'Weather in {city}, {state} is 70F and clear skies.'
        return result

    #Define our web_search tool function...
    def web_search(self, query):
        #print(f'{datetime.now().strftime("%H:%M:%S")} - Searching for {search_term} on Internet.')
        results = []
        response_list = []
        visited_url = []
        results.extend([r for r in search(query, 5, 'en')])
        print(f'resultados obtenidos: {len(results)}')
        if len(results) == 0: return "no encontre ningun resultado."
        print(results)
        for j in results:
            result_type = j[-3:]
            if j not in visited_url:
                try:
                    response = requests.get(j, timeout=5)
                    if response.status_code == 200:
                        if result_type != 'pdf':
                            soup = BeautifulSoup(response.text, 'html.parser')
                            response_list.append(soup.get_text().strip())
                        else:
                            bytes_stream = BytesIO(response.content)
                            reader = PdfReader(bytes_stream)
                            for page in reader.pages:
                                response_list.append(page.extract_text().strip())
                except Exception as err:
                    print(f'Error: {err}')
            visited_url.append(j)    
        response_text = ",".join(str(i) for i in response_list)
        print(f'Caracteres en la busquueda: {len(response_text)}')
        if len(response_text) > 200000:
            response_text = response_text[200000:]
        #print(f'{datetime.now().strftime("%H:%M:%S")} - Search results: {response_text}')
        return response_text
    
    def knowledge_search(self, query):
        print(f'{datetime.now().strftime("%H:%M:%S")} - Searching for {query} on KnowledgeBase.')
   
        response_text = retrieveAndGenerate(query, kb_id,model_id=model_id,region_id=region_id)
        #print(f"{datetime.now().strftime('%H:%M:%S')} - Search results: {response_text['output']['text']}")
        return response_text['output']['text']

def converse_with_tools(messages, system='', toolConfig=toolConfig):
    inference_config = {
        "temperature":0.0,
        "topP":0.9,
        "maxTokens":2000
    }
    
    response = bedrock.converse(
        modelId=model_id,
        system=system,
        messages=messages,
        toolConfig=toolConfig,
        inferenceConfig=inference_config
    )
    return response

#Function for orchestrating the conversation flow...
def converse_multi(prompt, system=''):
    #Add the initial prompt:
    messages = []
    messages.append(
        {
            "role": "user",
            "content": [
                {
                    "text": prompt
                }
            ]
        }
    )
    print(f"\n{datetime.now().strftime('%H:%M:%S')} - Initial prompt:\n{json.dumps(messages, indent=2)}")

    #Invoke the model the first time:
    output = converse_with_tools(messages, system)
    print(f"\n{datetime.now().strftime('%H:%M:%S')} - Output so far:\n{json.dumps(output['output'], indent=2, ensure_ascii=False)}")

    #Add the intermediate output to the prompt:
    messages.append(output['output']['message'])

    function_calling = next((c['toolUse'] for c in output['output']['message']['content'] if 'toolUse' in c), None)

    #Check if function calling is triggered:
    if function_calling:
        #Get the tool name and arguments:
        tool_name = function_calling['name']
        tool_args = function_calling['input'] or {}
        
        #Run the tool:
        print(f"\n{datetime.now().strftime('%H:%M:%S')} - Running ({tool_name}) tool whith parameters {tool_args}")
        tool_response = getattr(ToolsList3(), tool_name)(**tool_args)

        #Add the tool result to the prompt:
        messages.append(
            {
                "role": "user",
                "content": [
                    {
                        'toolResult': {
                            'toolUseId':function_calling['toolUseId'],
                            'content': [
                                {
                                    "text": tool_response
                                }
                            ]
                        }
                    }
                ]
            }
        )

        #Invoke the model one more time:
        output = converse_with_tools(messages, system)
        print(f"\n{datetime.now().strftime('%H:%M:%S')} - Final output:\n{json.dumps(output['output'], indent=2, ensure_ascii=False)}\n")
    outputtext = output['output']['message']['content'][0]['text']
    return outputtext

def retrieveAndGenerate(input, kbId, sessionId=None, model_id = 'anthropic.claude-3-haiku-20240307-v1:0', region_id = "us-east-1"):
    model_arn = f'arn:aws:bedrock:{region_id}::foundation-model/{model_id}'
    if sessionId:
        return bedrock_agent_client.retrieve_and_generate(
            input={
                'text': input
            },
            retrieveAndGenerateConfiguration={
                'type': 'KNOWLEDGE_BASE',
                'knowledgeBaseConfiguration': {
                    'knowledgeBaseId': kbId,
                    'modelArn': model_arn
                }
            },
            sessionId=sessionId
        )
    else:
        return bedrock_agent_client.retrieve_and_generate(
            input={
                'text': input
            },
            retrieveAndGenerateConfiguration={
                'type': 'KNOWLEDGE_BASE',
                'knowledgeBaseConfiguration': {
                    'knowledgeBaseId': kbId,
                    'modelArn': model_arn
                }
            }
        )
        
def get_converse_response(prompt):
    #system = [{"text": "You're provided with a tool that can get the weather information for a specific locations 'get_weather', and another tool to perform a web search for up to date information 'web_search'; \
    #        use those tools if required. Don't mention the tools in your final answer."}]
    system = [{"text":"cuentas con una herramienta que puede obtener la informacion del clima para ubicaciones especificadas llamada 'get_weather', otra herramienta que puede realizar busquedas en la web llamada 'web_search' y otra herramienta que puede hacer busquedas en bases de conocimientos llamada 'knowledge_search'; \
             utiliza esas herramientas unicamente si es necesario. No menciones las herramientas en la respuesta final."}]
    response = converse_multi(prompt, system)
    return response
    
#todo: add llm as a parameter in all converse functions

def lambda_handler(event, context):
    print (event)

    whats_message = event['whats_message']
    print(whats_message)

    whats_token = event['whats_token']
    messages_id = event['messages_id']
    phone = event['phone']
    phone_id = event['phone_id']
    phone_number = phone.replace("+","")

    #The session ID is created to store the history of the messages. 

    try:
        session_data = query("phone_number",table_session_active,phone_number)
        now = int(time.time())
        diferencia = now - session_data["session_time"]
        if diferencia > 240:  #session time in seg
            update_item_session(table_session_active,phone_number,now)
            id = str(phone_number) + "_" + str(now)
        else:
            id = str(phone_number) + "_" + str(session_data["session_time"])

    except:
        now = int(time.time())
        new_row = {"phone_number": phone_number, "session_time":now}
        save_item_ddb(table_session_active,new_row)
        
        id = str(phone_number) + "_" + str(now)

    try:
        print('REQUEST RECEIVED:', event)
        print('REQUEST CONTEXT:', context)
        print("PROMPT: ",whats_message)

        #s = re.sub(r'[^a-zA-Z0-9]', '', query)
        
        print('Running boto3 version:', boto3.__version__)
        
        response = get_converse_response(whats_message)

        print(response)

        whats_reply(whatsapp_out_lambda,phone, whats_token, phone_id, f"{response}", messages_id)
        
        end = int( time.time())

        update_items_out(table,messages_id,response,end)
                
        return({"body":response})
        
        
    except Exception as error: 
            print('FAILED!', error)
            return({"body":"Cuek! I dont know"})