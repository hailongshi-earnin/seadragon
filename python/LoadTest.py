import boto3
import random
import json
import os
from datetime import datetime
# for DAX
import amazondax
import botocore.session
import time
import sys

session = botocore.session.get_session()
#client = amazondax.AmazonDaxClient(session, region_name='us-west-2', endpoints=['earnin-dax-hs.sgll3p.clustercfg.dax.usw2.cache.amazonaws.com:8111'])


QUEUE_NAME = os.environ['QUEUE_NAME']
MAX_QUEUE_MESSAGES = os.environ['MAX_QUEUE_MESSAGES']
DYNAMODB_TABLE = os.environ['DYNAMODB_TABLE']
INDEX_NAME = os.environ['INDEX_NAME']

sqs = boto3.resource('sqs')
client = boto3.client('dynamodb')

def lambda_handler(event, context):
    #queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
    print('dax_load_test triggered') 
    #print('Total number of items: '+str(len(event['Records'])))
    #for record in event['Records']:
    #   payload=record["body"]
    #   print(str(payload))
    startProcess()
        
def startProcess():
    timeElapsed = 0.0    
    numberQuery = 0
    start_time = time.time()
    while timeElapsed < 600:
        queryDynamodb()
        timeElapsed = time.time() - start_time
        numberQuery += 1
    print("Total number of queries : " + str(numberQuery))

def queryDynamodb(): 
    try:
        response = client.query(
            TableName=DYNAMODB_TABLE, 
            IndexName=INDEX_NAME,
            KeyConditionExpression="userId = :v_userId",
            ExpressionAttributeValues={ 
                ':v_userId': {'S': str(random.randint(0, 10000))}
            }
        )
        #print( 0 / 0)
    except:
        e = sys.exc_info()[0]
        print(e)
    #print(len(response['Items']))
