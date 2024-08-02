import json
import boto3
import os

def handler(event, context):
    date = event['queryStringParameters']['date']
    dynamodb = boto3.client('dynamodb')
    
    table_name = os.environ['DDB_TABLE_NAME']
    
    params = {
        'TableName': table_name,
        'KeyConditionExpression': '#date = :date',
        'ExpressionAttributeNames': {
            '#date': 'date'
        },
        'ExpressionAttributeValues': {
            ':date': {'S': date}
        }
    }
    
    try:
        response = dynamodb.query(**params)
        return {
            'statusCode': 200,
            'body': json.dumps(response['Items'])
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error querying items: {str(e)}")
        }
