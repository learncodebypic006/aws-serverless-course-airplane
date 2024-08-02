import json
import boto3
import time
import random
import os
import csv
from datetime import datetime


def handler(event, context):
    # Initialize the S3 client
    s3_client = boto3.client('s3')
    
    # Initialize the SNS client
    sns_client = boto3.client('sns')
    
    # Initialize the DynamoDB client
    dynamodb = boto3.client('dynamodb')
    
    # Define the S3 bucket and file name
    bucket_name = os.environ['S3_BUCKET_NAME_PLAN_BAG_MAPPING']
    file_name = os.environ['S3_FILE_NAME_PLAN_BAG_MAPPING']    
    
    # Initialize the Lambda client
    lambda_client = boto3.client('lambda')
    
    # The name of the Lambda function to be called
    target_lambda_name = os.environ['TARGET_LAMBDA_NAME']
    
    # Create SQS client     
    # sqs = boto3.client('sqs')
    # queue_url = os.environ['SQS_URL_FOR_BAG_LINK_TO_CHECK']    
    
    # Get payloads from S3
    # payloads = get_payloads_from_s3(s3_client, bucket_name, file_name)
    # print("payloads from s3: ", payloads)
    
    # Get payload from testing
    payloads = get_payloads_for_testing()
    print("payloads from test: ", payloads)
    
    # Get the SNS topic ARN from environment variables
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    
    lowest_price_response = None
    lowest_price = float('inf')  # Initialize with a very high value    
    for target_url, airplane_name in payloads.items():
        print("parsing payload: ", {"target_url": target_url, "airplane_name": airplane_name})
        response = lambda_client.invoke(
            FunctionName=target_lambda_name,
            InvocationType='RequestResponse',  # Synchronous invocation
            Payload=json.dumps({"target_url": target_url, "airplane_name": airplane_name})
        )
        
        # Read and parse the response payload
        response_payload = json.loads(response['Payload'].read())
        body = response_payload.get('body', {})
        lowest_price_current = body.get('lowest_price', float('inf'))
        print("response payload: ", response_payload)        
        
        # Update the lowest price response
        if lowest_price_current < lowest_price:
            lowest_price = lowest_price_current
            lowest_price_response = response_payload        
    
    # next: send to sqs for further processing and to ensure the message is processed at least once 
    # sqs_payload = json.dumps(lowest_price_response)
    # sqs_response = sqs.send_message(
    #     QueueUrl=queue_url,
    #     MessageBody=sqs_payload
    # )
    # print(f'sqs payload: {sqs_payload}')
    # print(f'sqs response: {sqs_response}')
    
    # Send the lowest_price_response to SNS topic
    sns_response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=json.dumps(lowest_price_response)
    )
    print(f'sns payload: {json.dumps(lowest_price_response)}')
    print(f'sns response: {sns_response}')    

    # Put the lowest_price_response into DynamoDB
    put_item_to_dynamodb(lowest_price_response, dynamodb)
    
    # Query 
    timestamp = datetime.utcnow()
    date_today = timestamp.strftime('%Y-%m-%d')  # Extract date part
    ddb_result = query_items_by_date(date_today, dynamodb)
    print(f'ddb_result: {ddb_result}')    
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'lowest_price_response': lowest_price_response
        })
    }   
    
def get_payloads_for_testing():
    return {
        f'{os.environ['APPLE_WEBSITE_URL']}': 'apple',
        f'{os.environ['BANANA_WEBSITE_URL']}': 'banana',
        'XXXXX': 'random'
    }

def get_payloads_from_s3(s3_client, bucket_name, file_name):
    file_content = read_s3_file(s3_client, bucket_name, file_name)
    return parse_csv_content(file_content)

def read_s3_file(s3_client, bucket_name, file_name):
    """
    Reads a file from S3 and returns its content.
    """
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    content = response['Body'].read().decode('utf-8')
    return content

def parse_csv_content(content):
    """
    Parses CSV content and returns a dictionary.
    """
    reader = csv.reader(content.splitlines())
    next(reader)  # Skip header row
    return {row[0]: row[1] for row in reader}

def put_item_to_dynamodb(response, dynamodb):
    table_name = os.environ['DDB_TABLE_NAME']
    timestamp = datetime.utcnow()
    date_str = timestamp.strftime('%Y-%m-%d')  # Extract date part
    time_str = timestamp.strftime('%H-%M-%S')  # Extract time part
    item = {
        'date': {'S': date_str},
        'time': {'S': time_str},
        'airline': {'S': response['body']['airplane_name']},
        'price': {'N': str(response['body']['lowest_price'])}
    }
    
    try:
        dynamodb.put_item(TableName=table_name, Item=item)
        print('Item added to DynamoDB')
    except Exception as e:
        print(f"Error adding item to DynamoDB: {str(e)}")

def query_items_by_date(date, dynamodb):
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