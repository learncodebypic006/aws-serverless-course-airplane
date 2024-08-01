import os
import json
from urllib.parse import urlencode, urljoin
import boto3
from bs4 import BeautifulSoup
import re

def send_bag_available_notification(url):
    sns_client = boto3.client('sns')
    topic_arn = os.environ['SNS_TOPIC_ARN']
    
    message = f'Click here: {url}'
    subject = 'Your Bag Is Available'
    
    sns_response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject=subject
    )
    
    return sns_response

def is_add_to_cart_enabled(status_code, html_content):
    if status_code == 404: 
        return False
    
    soup = BeautifulSoup(html_content, 'html.parser')
    add_to_cart_button = soup.find('button', id=re.compile(r'^add-to-cart-button-in-stock.*'))
    if add_to_cart_button:

        if 'Add to cart' in add_to_cart_button.get_text():
            print('The button contains the text "Add to cart".')  # TODO: add metric here 
        else:
            print('The button does not contain the text "Add to cart".')  # TODO: add metric here 

        is_disabled = add_to_cart_button.get('aria-disabled')
        print("add_to_cart_button: " , add_to_cart_button)
        print("is_disabled: " , is_disabled)
        return is_disabled != 'true'
    return False

def build_url(proxy_url, params):
    query_string = urlencode(params)
    return urljoin(proxy_url, f"?{query_string}")

def save_to_local_file(data, filename='scraped_data.json'):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, filename)
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
    return file_path

def put_metric_data(status_code):
    cloudwatch = boto3.client('cloudwatch')
    response = cloudwatch.put_metric_data(
        Namespace=f'WebScraper-{os.environ['STAGE']}',
        MetricData=[
            {
                'MetricName': 'ResponseStatusCode',
                'Dimensions': [
                    {
                        'Name': 'Service',
                        'Value': 'BagAvailabilityChecker'
                    },
                    {
                        'Name': 'StatusCode',
                        'Value': str(status_code)
                    }
                ],
                'Value': 1,
                'Unit': 'Count',
                'StorageResolution': 1
            },
        ]
    )
    return response
