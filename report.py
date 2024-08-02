import json
import boto3
import os

def handler(event, context):
    date = event['queryStringParameters']['date']
    # date = '2024-08-02' # testing 
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
        items = response['Items']
        
        # Convert DynamoDB items to HTML
        html_content = f'''
        <html>
        <head>
            <style>
                body {{font-family: Arial, sans-serif; margin: 40px;}}
                table {{width: 100%; border-collapse: collapse; margin-bottom: 20px;}}
                th, td {{border: 1px solid #dddddd; text-align: left; padding: 8px;}}
                th {{background-color: #f2f2f2;}}
                tr:nth-child(even) {{background-color: #f9f9f9;}}
                tr:hover {{background-color: #f1f1f1;}}
                h2 {{text-align: center;}}
            </style>
        </head>
        <body>
            <h2>Flight Prices on {date}</h2>
            <table>
                <tr>
                    <th>Date</th>
                    <th>Time</th>
                    <th>Airline</th>
                    <th>Price</th>
                </tr>'''
        
        for item in items:
            html_content += f'''
                <tr>
                    <td>{item["date"]["S"]}</td>
                    <td>{item["time"]["S"]}</td>
                    <td>{item["airline"]["S"]}</td>
                    <td>{item["price"]["N"]}</td>
                </tr>'''
        
        html_content += '''
            </table>
        </body>
        </html>'''
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/html'
            },
            'body': html_content
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error querying items: {str(e)}")
        }
