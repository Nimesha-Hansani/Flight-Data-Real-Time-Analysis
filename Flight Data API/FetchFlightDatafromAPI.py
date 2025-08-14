import requests
import boto3
import requests
import json
import time
import pandas as pd


kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def send_to_kinesis(data):
    # Send data to Kinesis stream
    
    # Use the flight_date as the partition key
    partition_key = data.get('flight_date', 'unknown-date')
    data_record = json.dumps(data)

    kinesis_client.put_record(StreamName='Flight-API-Data-Stream', Data=data_record, PartitionKey= partition_key)


def invoke_api():


    api_endpoint = 'https://api.aviationstack.com/v1/flights'
    params = {
            'access_key': 'b9a99cfbc2971adba7d9b72b3264a66d',
            'limit': 5
    }

    response = requests.get('https://api.aviationstack.com/v1/flights', params = params)

    json_response = response.json()

    for flight in json_response['data']:
        # print(flight)
        send_to_kinesis(flight)


if __name__ == "__main__":
    invoke_api()