import json
import boto3
import time
import random
import os
import csv


def handler(event, context):
    # Initialize the S3 client
    s3_client = boto3.client('s3')
    
    # Define the S3 bucket and file name
    bucket_name = os.environ['S3_BUCKET_NAME_PLAN_BAG_MAPPING']
    file_name = os.environ['S3_FILE_NAME_PLAN_BAG_MAPPING']    
    
    # Initialize the Lambda client
    lambda_client = boto3.client('lambda')
    
    # The name of the Lambda function to be called
    target_lambda_name = os.environ['TARGET_LAMBDA_NAME']
    
    # Create SQS client     
    sqs = boto3.client('sqs')
    queue_url = os.environ['SQS_URL_FOR_BAG_LINK_TO_CHECK']    
    
    # Get payloads from S3
    payloads = get_payloads_from_s3(s3_client, bucket_name, file_name)
    print("payloads from s3: ", payloads)
    
    # Get payloads for testing
    # payloads = get_payloads_for_testing()
    # print("payloads for testing: ", payloads)
    
    # sleep randomly
    # random_sleep(0, 10000)
    
    # Invoke the target Lambda function for each payload
    # each request takes 40s
    # concurrent limit is 5 requests 
    # avarge time I coudl send out a request: 40s/5 = 8s
    # set the time gap: 8s ~ 60s
    results = []
    for bag_link, wix_plan_name in payloads.items():
        delay_sec = random.randint(1, 300) # expect to finish all in 5 minutes 
        # delay_sec = random.randint(1, 9) # testing only 
        sqs_payload = json.dumps({"bag_url": bag_link, "wix_plan_name": wix_plan_name})
        sqs_response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=sqs_payload,
            DelaySeconds=delay_sec
        )
        print(f'sqs payload: {sqs_payload}')
        print(f'sqs response: {sqs_response}')
        
        # Collect the status of each invocation
        results.append({
            'Payload': {"bag_url": bag_link, "wix_plan_name": wix_plan_name}
        })        
        
        
        # random_sleep(8000, 15000)
        # print("parsing payload: ", {"bag_url": bag_link, "wix_plan_name": wix_plan_name})
        # response = lambda_client.invoke(
        #     FunctionName=target_lambda_name,
        #     InvocationType='Event',  # Asynchronous invocation
        #     Payload=json.dumps({"bag_url": bag_link, "wix_plan_name": wix_plan_name})
        # )
        
        # Collect the status of each invocation
        # results.append({
        #     'Payload': {"bag_url": bag_link, "wix_plan_name": wix_plan_name},
        #     'StatusCode': response['StatusCode']
        # })
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }    
    
def get_payloads_for_testing():
    return {        
    }

def get_payloads_from_s3(s3_client, bucket_name, file_name):
    """
    Reads the file from S3 and parses the CSV content into a dictionary.
    """
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

def random_sleep(min_time_ms=0, max_time_ms=10000):
    """
    Sleeps for a random time between min_time_ms and max_time_ms milliseconds.

    :param min_time_ms: Minimum sleep time in milliseconds (default is 0)
    :param max_time_ms: Maximum sleep time in milliseconds (default is 10000)
    """
    # Generate a random sleep time between min_time_ms and max_time_ms milliseconds
    sleep_time_ms = random.uniform(min_time_ms, max_time_ms)
    
    # Convert milliseconds to seconds
    sleep_time_sec = sleep_time_ms / 1000.0
    
    # Log the sleep time (optional)
    print(f"Sleeping for {sleep_time_ms:.2f} milliseconds ({sleep_time_sec:.2f} seconds)")
    
    # Sleep for the generated time in seconds
    time.sleep(sleep_time_sec)