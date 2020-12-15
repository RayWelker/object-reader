import json
import urllib.parse
import boto3
import os

import dynamodb_operations
import s3_restore
import sqs_operations

print('Loading function')

s3 = boto3.client('s3')
TABLE = os.environ['TABLE']
Topic = os.environ['SNS_ARN']
SQSQueueURL = os.environ['SQS_QUEUE_URL']

def handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        content_object = s3.get_object(Bucket=bucket, Key=key)
        file_content = content_object['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        execution_id = json_content['execution_id']
        bcl_paths = json_content['bcl_paths']
        print("CONTENT TYPE: " + content_object['ContentType'] + " Execution ID: " + json_content['execution_id'])
        s3_restore.s3_restore(bcl_paths)
        dynamodb_operations.populate_job_details(execution_id, TABLE)
        sqs_operations.send_message(execution_id, SQSQueueURL)
        return {
          'statusCode': 200,
          'body':       json.dumps(json_content)
        }
    except Exception as e:
        print(e)
        print(str(json_content))
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e