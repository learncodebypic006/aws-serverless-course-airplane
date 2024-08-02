import requests
import json
from utils import send_bag_available_notification, is_add_to_cart_enabled, build_url, save_to_local_file, put_metric_data
import boto3
import os
from bs4 import BeautifulSoup
import random

# Initialize SQS client
sqs = boto3.client('sqs')

# Example usage:
def handler(event, context):
    print("handler starts")
    print("event: ", event)
    target_url = event["target_url"]
    airplane_name = event["airplane_name"] 
    
    print("target_url: ", target_url)
    print("airplane_name: ", airplane_name)
    
    is_debug = False
    if airplane_name == "apple":
        lowest_price = check_apple_airline_price(target_url)
    elif airplane_name == "banana":
        lowest_price = check_banana_airline_price(target_url)
    else:
        lowest_price = check_random_airline_price()
    print(f"The lowest ticket price of ${airplane_name} is: ${lowest_price}")
    
    return {
        'body': {
            'airplane_name': airplane_name,
            'lowest_price': lowest_price
        }
    }

def check_random_airline_price():
    try:
        # Generate a random price between 300 and 600
        random_price = random.randint(300, 600)
        print(f"Random price generated: ${random_price}")

        return random_price

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    

def check_banana_airline_price(url):
    try:
        # Fetch the webpage content
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all price elements
        price_elements = soup.find_all('div', class_='flight-price')
        
        # Extract prices as integers
        prices = [int(price_element.text.replace('$', '')) for price_element in price_elements]

        print(f"Prices found: {prices}")

        # Return the lowest price
        return min(prices)

    except requests.RequestException as e:
        print(f"HTTP error occurred: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def check_apple_airline_price(url):
    try:
        # Fetch the webpage content
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all price cells in the table
        price_cells = soup.find_all('td', text=lambda x: x and x.startswith('$'))
        
        # Extract prices as integers
        prices = [int(cell.text.replace('$', '')) for cell in price_cells]

        print(f"Prices found: {prices}")

        # Return the lowest price
        return min(prices)

    except requests.RequestException as e:
        print(f"HTTP error occurred: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None    
    

def check_bag_and_send_notification(url, is_debug=True):
    ERROR_TAG = "[ERROR]"
    proxy_url = "https://api.scrapfly.io/scrape"
    params = {
        'tags': 'player,project:default',
        'proxy_pool': 'public_residential_pool',
        'format': 'raw',
        'lang': 'en',
        'asp': 'true', # combat fingerprinting 
        'render_js': 'true', # combat fingerprinting 
        # 'rendering_wait': 25000, # wait for the js to run to look up inventory number of the bag # TODO: improve it
        # 'js': 'ZnVuY3Rpb24gZmluZFRhcmdldEJ1dHRvbigpIHsNCiAgICAvLyBHZXQgYWxsIGJ1dHRvbiBlbGVtZW50cyBhcyBhbiBhcnJheQ0KICAgIGNvbnN0IGFsbEJ1dHRvbnMgPSBBcnJheS5mcm9tKGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3JBbGwoJ2J1dHRvbicpKTsNCg0KICAgIC8vIE1hcCB0aGVpciBpbm5lckhUTUxzDQogICAgY29uc3QgaW5uZXJIVE1McyA9IGFsbEJ1dHRvbnMubWFwKGVsID0-IGVsLmlubmVySFRNTCk7DQoNCiAgICAvLyBGaWx0ZXIgdGhlIGlubmVySFRNTHMgdG8gZmluZCB0aG9zZSB0aGF0IGNvbnRhaW4gdGhlIHRleHQgIkFkZCIsICJ0byIsIGFuZCAiY2FydCINCiAgICBjb25zdCBmaWx0ZXJlZElubmVySFRNTCA9IGlubmVySFRNTHMuZmlsdGVyKHRleHQgPT4gDQogICAgICAgIHRleHQuaW5jbHVkZXMoJ0FkZCcpICYmIHRleHQuaW5jbHVkZXMoJ3RvJykgJiYgdGV4dC5pbmNsdWRlcygnY2FydCcpDQogICAgKTsNCg0KICAgIGxldCBpc190YXJnZXRfZm91bmQgPSBmYWxzZTsNCiAgICBpZiAoZmlsdGVyZWRJbm5lckhUTUwubGVuZ3RoID4gMCkgew0KICAgICAgICBpc190YXJnZXRfZm91bmQgPSB0cnVlOw0KICAgIH0NCg0KICAgIHJldHVybiB7DQogICAgICAgIGlzX3RhcmdldF9mb3VuZDogaXNfdGFyZ2V0X2ZvdW5kLA0KICAgICAgICBmaWx0ZXJlZElubmVySFRNTDogZmlsdGVyZWRJbm5lckhUTUwNCiAgICB9Ow0KfQ0KDQp0cnkgew0KICAgIC8vIENhcHR1cmUgdGhlIHN0YXJ0IHRpbWUNCiAgICBjb25zdCBzdGFydFRpbWUgPSBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCk7DQogICAgbGV0IGlzX3RhcmdldF9mb3VuZCA9IGZhbHNlOw0KICAgIGxldCByZXN1bHQgPSBudWxsOw0KICAgIGxldCBmaWx0ZXJlZElubmVySFRNTCA9IG51bGw7DQogICAgY29uc3QgY291bnRfbWF4ID0gNTsNCiAgICBsZXQgY291bnRfbm93ID0gMDsNCg0KICAgIC8vIGNoZWNrIHRhcmdldCBkb20gDQogICAgd2hpbGUoY291bnRfbm93IDw9IGNvdW50X21heCkgew0KICAgICAgICBjb3VudF9ub3crKzsNCg0KICAgICAgICByZXN1bHQgPSBmaW5kVGFyZ2V0QnV0dG9uKCk7DQogICAgICAgIGZpbHRlcmVkSW5uZXJIVE1MID0gcmVzdWx0LmZpbHRlcmVkSW5uZXJIVE1MOw0KICAgICAgICBpc190YXJnZXRfZm91bmQgPSByZXN1bHQuaXNfdGFyZ2V0X2ZvdW5kOw0KICAgICAgICBpZiAoaXNfdGFyZ2V0X2ZvdW5kID09IHRydWUpIGJyZWFrOw0KICAgIA0KICAgICAgICAvLyBXYWl0IGZvciA1IHNlY29uZHMgKGZvciBkZW1vbnN0cmF0aW9uIHB1cnBvc2VzKQ0KICAgICAgICBjb25zdCB3YWl0ID0gbXMgPT4gbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIG1zKSk7DQogICAgICAgIGF3YWl0IHdhaXQoNTAwMCk7ICAgIA0KICAgIH0NCg0KICAgIC8vIENhcHR1cmUgdGhlIGVuZCB0aW1lDQogICAgY29uc3QgZW5kVGltZSA9IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKTsNCg0KICAgIC8vIENhbGN1bGF0ZSB0aGUgZHVyYXRpb24gaW4gc2Vjb25kcw0KICAgIGNvbnN0IGR1cmF0aW9uSW5TZWNvbmRzID0gKERhdGUucGFyc2UoZW5kVGltZSkgLSBEYXRlLnBhcnNlKHN0YXJ0VGltZSkpIC8gMTAwMDsNCg0KICAgIC8vIFJldHVybiB0aGUgZmlsdGVyZWQgaW5uZXJIVE1McywgdGhlIHN0YXJ0IGFuZCBlbmQgdGltZXMsIGFuZCB0aGUgZHVyYXRpb24NCiAgICByZXR1cm4gew0KICAgICAgICAnaXNfdGFyZ2V0X2ZvdW5kXzAxJzogaXNfdGFyZ2V0X2ZvdW5kLA0KICAgICAgICAnZmlsdGVyZWRJbm5lckhUTUwnOiBmaWx0ZXJlZElubmVySFRNTCwNCiAgICAgICAgJ3N0YXJ0VGltZSc6IHN0YXJ0VGltZSwNCiAgICAgICAgJ2VuZFRpbWUnOiBlbmRUaW1lLA0KICAgICAgICAnZHVyYXRpb25JblNlY29uZHMnOiBkdXJhdGlvbkluU2Vjb25kcywNCiAgICAgICAgJ2NvdW50X25vdyc6IGNvdW50X25vdw0KICAgIH07DQoNCn0gY2F0Y2ggKGVycm9yKSB7DQogICAgLy8gQ2FwdHVyZSB0aGUgZW5kIHRpbWUgaW4gY2FzZSBvZiBhbiBlcnJvcg0KICAgIGNvbnN0IGVuZFRpbWUgPSBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCk7DQoNCiAgICAvLyBDYWxjdWxhdGUgdGhlIGR1cmF0aW9uIGluIHNlY29uZHMNCiAgICBjb25zdCBkdXJhdGlvbkluU2Vjb25kcyA9IChEYXRlLnBhcnNlKGVuZFRpbWUpIC0gRGF0ZS5wYXJzZShzdGFydFRpbWUpKSAvIDEwMDA7DQoNCiAgICAvLyBSZXR1cm4gZXJyb3IgZGV0YWlscywgdGhlIHN0YXJ0IGFuZCBlbmQgdGltZXMsIGFuZCB0aGUgZHVyYXRpb24NCiAgICByZXR1cm4gew0KICAgICAgICAnc3RhdHVzJzogJ2Vycm9yJywNCiAgICAgICAgJ21lc3NhZ2UnOiBlcnJvci5tZXNzYWdlLA0KICAgICAgICAnbmFtZSc6IGVycm9yLm5hbWUsDQogICAgICAgICdzdGFjayc6IGVycm9yLnN0YWNrLA0KICAgICAgICAnc3RhcnRUaW1lJzogc3RhcnRUaW1lLA0KICAgICAgICAnZW5kVGltZSc6IGVuZFRpbWUsDQogICAgICAgICdkdXJhdGlvbkluU2Vjb25kcyc6IGR1cmF0aW9uSW5TZWNvbmRzDQogICAgfTsNCn0NCg',
        'key': 'scp-live-ff5f632ff5d64bcd90ddac0c649d85d1',
        'url': url
    }
    session = None
    if session:
        params['session'] = session
        params['correlation_id'] = str(uuid.uuid4())

    is_bag_page_avaialble = False
    response_json = None
    status_code = None
    result = None
    error = None
    try:
        full_url = build_url(proxy_url, params)
        print("sending request to target url: ", full_url)

        response = requests.get(full_url)
        response_json = response.json()
        result = response_json.get('result', {})
        status_code = result.get("status_code")

        error = result.get("error")
        if status_code not in [404, 403, 200]:
            response.raise_for_status() # throw exception if found here 

        is_cart_available = is_add_to_cart_enabled(status_code, result.get("content"))
        if is_cart_available:
            print('add to cart button is clickable.') # TODO: add metric here 
            is_bag_page_avaialble = True
        else:
            print('add to cart button is not clickable.') # TODO: add metric here 

        if is_bag_page_avaialble:
            print('sending notification.')
            sns_response = send_bag_available_notification(url) # TODO: add metric here 
            print('sns_response:', sns_response)
    except requests.exceptions.RequestException as e:
        print('exception thrown')
        if status_code is None:
            print(f'status_code does not exist, use error_status_code: {e.response.status_code}')
            status_code = e.response.status_code
        else:
            print(f'use existing status_code: {status_code} while seeing error_status_code: {e.response.status_code}')
        
        print(f"{ERROR_TAG}: An error occurred: {e}")
        # if status_code == 429: # Too Many Requests 
        #     retry_after = e.response.headers.get('Retry-After', 'No Retry-After header present')
        #     print(f"{ERROR_TAG}: An error occurred: {e}. Retry-After: {retry_after}")
        # elif status_code == 422:
        #     error_detail = e.response.json() if e.response.headers.get('Content-Type') == 'application/json' else e.response.text
        #     print(f"{ERROR_TAG}: An error occurred: {e}. Details: {error_detail}")
        # else:
        #     print(f"{ERROR_TAG}: An error occurred: {e}")
    except json.JSONDecodeError as e:
        print(f"{ERROR_TAG}: Failed to decode JSON from response: {e}")
    except Exception as e:
        print(f"{ERROR_TAG}: An unexpected error occurred: {e}")

    finally:
        if is_debug:
            file_path = save_to_local_file(response_json, 'scraped_data.json')
            print(f"Saved response to {file_path}")
            print("response_json: ", response_json)
            print("parsing result: ", result)
        
        if result:
            browser_data = result.get("browser_data")
            javascript_evaluation_result = browser_data.get("javascript_evaluation_result")
            print("javascript_evaluation_result: ", javascript_evaluation_result)

        if session:
            print("session in use: ", session)

        if 'response' in locals():
            response.close()
        
        if status_code:
            log_status_code(status_code, error)
            put_metric_data(status_code)
        else: 
            print("status_code does not exist")

    return is_bag_page_avaialble, status_code


def log_status_code(status_code, error):
    print('status_code: ', status_code)
    if status_code == 200:
        print('success response')
    else:
        if status_code == 403:
            print('scraping is blocked by anti-parsing service')
        elif status_code == 404:
            print('bag page is not available')
        elif status_code == 429:
            print('too many requests')
        elif status_code == 422:
            print('Unprocessable Entity. Check Whether it is blocked')
        else:
            print('unexpected error')    
        print('failed response: ', error)
