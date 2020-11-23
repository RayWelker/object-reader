import boto3
from boto3.dynamodb.conditions import Key

PRIMARY_KEY = 'executionId'

def populate_job_details(execution_id, table_name):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(table_name)
  table.update_item(
    Key={
      "executionId": execution_id,
    },
    UpdateExpression="SET #s = :newStatus",
    ExpressionAttributeValues={
      ":newStatus": "WAITING"
    },
    ExpressionAttributeNames={
      "#s": "Status"
    },
    ReturnValues="UPDATED_NEW"
  )
  return True