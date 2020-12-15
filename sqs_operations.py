import boto3
import json

def send_message(execution_id, SQSQueueURL):
    sqs = boto3.client('sqs')
    response = sqs.send_message(
            QueueUrl=SQSQueueURL,
            MessageBody=(execution_id)
        )