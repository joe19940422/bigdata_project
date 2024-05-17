import boto3
import requests
import json


api_key = 'f0d8ccec5119f62da702f9121d9c86333'


def get_weather_data(city):
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    complete_url = base_url + "appid=" + api_key + "&q=" + city

    response = requests.get(complete_url)
    print(response)
    return response.json()


def put_weather_data(city, data):
    try:
        stream_name = 'weather'
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=city
        )
        print(f"weather data writen to Kinesis sequencenumber: {response['SequenceNumber']},SharId: {response['ShardId']} Data: {data}")
    except Exception as e:
        print(e)


if __name__ == '__main__':
    # Set up Kinesis client
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')
    city = 'Rotterdam'
    data = get_weather_data(city)
    put_weather_data(city, data=data)