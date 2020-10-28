from datadog_lambda.metric import lambda_metric
from datadog_lambda.wrapper import datadog_lambda_wrapper
import urllib.parse
import boto3
import json
import gzip
from constant import * 

s3 = boto3.client('s3')

#@datadog_lambda_wrapper
def lambda_handler(event, context):
    """
    lambda_metric(
        "custom_matrix_name",             # Metric name
        9.99,                             # Metric value
        tags=['dev', 'bank_tx_lambda']    # Associated tags
    )
    """
    print(json.dumps(event))
    #1 - Get the bucket name & key name
    bucket = event[RECORDS][ZERO][S3][BUCKET][NAME]
    key = urllib.parse.unquote_plus(event[RECORDS][ZERO][S3][OBJECT][KEY], encoding=UTF8)
    
    print("bucket name: " + bucket)
    print("key: " + key)
    typeDict = {}
    #2 Process the content of the file
    obj = s3.get_object(Bucket=bucket, Key=key)
    with gzip.GzipFile(fileobj=obj[BODY]) as gzipfile:
        for content in gzipfile.readlines() :
            print(content)
            record = json.loads(content)
            print("EventType:" + record['EventType']) 
            if typeDict.get(record[EVENT_TYPE]) is None:
                typeDict[record[EVENT_TYPE]] = [record]
            else: 
                typeDict.get(record[EVENT_TYPE]).append(record) 

    print("typeDict:" + json.dumps(typeDict))

    for dictKey in typeDict.keys():
        temp = NEW_LINE.join([str(item) for item in typeDict.get(dictKey)])
        response = s3.put_object(
             Bucket=bucket,
             Key=key.replace(INPROGRESS, 'event/' + dictKey),
             Body=gzip.compress(bytes(temp, UTF8))
        )
    print("Save to archive:")

    #4 Archive the original file to a different folder
    copyResponse = s3.copy_object(
        Bucket=bucket,
        CopySource=bucket + FORWARD_SLASH + key,
        Key=key.replace(INPROGRESS, ARCHIVE),
    )

    print(copyResponse)
    
    
    """
    print("Delete the original one.")
    #5 Delete the original file. 
    delResponse = s3.delete_object(
        Bucket=bucket,
        Key=key,
    )
    print(delResponse)
    """


