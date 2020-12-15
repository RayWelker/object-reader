import boto3
import json

def send_message(execution_id, sqs_queue_url):
    sqs = boto3.client('sqs')
    sqs.send_message(
        QueueUrl=sqs_queue_url,
        MessageBody=(execution_id)
    )