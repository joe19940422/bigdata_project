import sys
import subprocess

# pip install custom package to /tmp/ and add to path
subprocess.call('pip install requests -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL)

sys.path.insert(1, '/tmp/')

import json
import requests
import boto3
from datetime import datetime, timezone, timedelta


def get_current_time():
    amsterdam_offset = timedelta(hours=2)

    utc_time = datetime.now(timezone.utc)

    # Calculate Amsterdam time by adding the offset
    amsterdam_time = utc_time + amsterdam_offset

    return amsterdam_time.isoformat()


def fetch_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API request failed with status code: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Error occurred during API request: {e}")
        return None


def lambda_handler(event, context):
    api_url = "https://api.freecurrencyapi.com/v1/latest?apikey=fca_live_vnN4NznRW6gv9qOe6MLxmUqbzZIzrlwiDPFKFgx7"
    api_data = fetch_data_from_api(api_url)

    if api_data:
        api_data['tms'] = get_current_time()

        dynamodb = boto3.client('dynamodb', region_name='us-east-1')
        table_name = 'CurrencyExchangeRates'

        data = api_data.get('data', {})
        current_timestamp = api_data.get('tms', get_current_time())

        for currency_code, exchange_rate in data.items():
            dynamodb.put_item(
                TableName=table_name,
                Item={
                    'currency_code': {'S': currency_code},
                    'exchange_rate': {'N': str(exchange_rate)},
                    'timestamp': {'S': current_timestamp}
                }
            )

        return {
            'statusCode': 200,
            'body': json.dumps('Data inserted successfully into DynamoDB table with timestamps.')
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to fetch data from API.')
        }
