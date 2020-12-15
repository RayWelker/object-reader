import boto3
from boto3.dynamodb.conditions import Key

def populate_job_details(execution_id, snakemake_table):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(snakemake_table)
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

def populate_job_details_restoring(execution_id, status_table, obj_key, obj_bucket):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(status_table)
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

def populate_job_details_complete(execution_id, status_table, obj_key):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(status_table)
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