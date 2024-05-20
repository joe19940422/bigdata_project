import json
import boto3
import base64
from decimal import Decimal
from datetime import datetime, timedelta

"""
A lambda function that converts weather data from Kinesis to DynamoDB
"""


def lambda_handler(event, context):
    try:
        dynamodb_db = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb_db.Table('weather')

        for record in event["Records"]:
            # Decode the Kinesis data
            decoded_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            print("Decoded Data:", decoded_data)

            # Convert the JSON string to a dictionary
            decoded_data_dic = json.loads(decoded_data, parse_float=Decimal)
            print("Decoded Data as Dictionary:", decoded_data_dic)
            decoded_data_dic.pop('base', None)
            dt = decoded_data_dic.get('dt')
            utc_time = datetime.utcfromtimestamp(dt)
            amsterdam_time = utc_time + timedelta(hours=2)
            amsterdam_time_iso = amsterdam_time.isoformat()
            decoded_data_dic['tms'] = amsterdam_time_iso
            decoded_data_dic.pop('dt', None)
            # Add current timestamp
            current_timestamp = (datetime.utcnow() + timedelta(hours=2)).isoformat()
            decoded_data_dic['insert_time'] = current_timestamp
            # Batch write the item to DynamoDB
            with table.batch_writer() as batch_writer:
                batch_writer.put_item(Item=decoded_data_dic)

    except Exception as e:
        print("Error:", str(e))
        raise e