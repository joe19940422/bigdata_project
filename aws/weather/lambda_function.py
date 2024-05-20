import sys
import subprocess

# pip install custom package to /tmp/ and add to path
subprocess.call('pip install requests -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL)

sys.path.insert(1, '/tmp/')

import boto3
import requests
import json

api_key = 'f0d8ccec5119f62da702f9121d9c8633'


def get_weather_data(city):
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    complete_url = base_url + "appid=" + api_key + "&q=" + city

    response = requests.get(complete_url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def lambda_handler(event, context):
    cities = ['Rotterdam', 'Taipei']
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')
    stream_name = 'weather'

    try:
        response = kinesis_client.describe_stream(StreamName=stream_name)
        stream_status = response['StreamDescription']['StreamStatus']
        if stream_status == 'ACTIVE':
            print(f"Stream '{stream_name}' exists and is active.")
            for city in cities:
                data = get_weather_data(city=city)
                if data:
                    response = kinesis_client.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(data),
                        PartitionKey=city
                    )
                    print(f"Weather data for {city} written to Kinesis sequencenumber: {response['SequenceNumber']}, ShardId: {response['ShardId']} Data: {data}")
                else:
                    print(f"Failed to fetch data for {city}")
        else:
            print(f"Stream '{stream_name}' exists but is not active. Status: {stream_status}")
    except kinesis_client.exceptions.ResourceNotFoundException:
        print(f"Stream '{stream_name}' does not exist.")

    return {
        'statusCode': 200,
        'body': json.dumps('Weather data written to Kinesis.')
    }