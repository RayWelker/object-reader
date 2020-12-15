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
      "#s": "status"
    },
    ReturnValues="UPDATED_NEW"
  )
  return True

def populate_job_details_restoring(execution_id, GLACIER_RESTORE_TABLE, obj_key, obj_bucket):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(GLACIER_RESTORE_TABLE)
  table.update_item(
    Key={
      "executionId": execution_id,
    },
    UpdateExpression="SET #s = :newStatus, #o = :newObject, #b = :newBucket",
    ExpressionAttributeValues={
      ":newStatus": "RESTORING",
      ":newObject": obj_key,
      ":newBucket": obj_bucket,
    },
    ExpressionAttributeNames={
      "#s": "status",
      "#o": "s3_object",
      "#b": "s3_bucket"
    },
    ReturnValues="UPDATED_NEW"
  )
  return True

def populate_job_details_complete(execution_id, GLACIER_RESTORE_TABLE, obj_key):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(GLACIER_RESTORE_TABLE)
  table.update_item(
    Key={
      "executionId": execution_id,
    },
    UpdateExpression="SET #s = :newStatus, #o = :newObject",
    ExpressionAttributeValues={
      ":newStatus": "COMPLETE",
      ":newObject": obj_key
    },
    ExpressionAttributeNames={
      "#s": "status",
      "#o": "s3_object"
    },
    ReturnValues="UPDATED_NEW"
  )
  return True