import json
import urllib.parse
import boto3
import os

import dynamodb_operations

print('Loading function')

s3 = boto3.client('s3')
TABLE = os.environ['TABLE']


def handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        content_object = s3.get_object(Bucket=bucket, Key=key)
        file_content = content_object['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        execution_id = json_content['executionId']
        print("CONTENT TYPE: " + content_object['ContentType'] + " S3 Glacier Path: " + json_content['glacierPath'] + " Execution ID: " + json_content['executionId'])
        dynamodb_operations.populate_job_details(execution_id, TABLE)
        return {
          'statusCode': 200,
          'body':       json.dumps(json_content)
        }
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
