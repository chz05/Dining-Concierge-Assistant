import json
import boto3
from botocore.exceptions import ClientError
import requests
import datetime



def insert_data(data_list, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb', aws_access_key_id='AKIA4T7JYIYK7F3A7YFN', aws_secret_access_key='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' ,region_name='us-east-1')
    table = db.Table(table)
    # overwrite if the same index is provided
    # put every data
    for data in data_list:
        response = table.put_item(Item=data)
        print('insert data')
    print('@insert_data: response', response)
    return response



if __name__ == '__main__':
    with open('dynamoData.json', 'r') as file:
        data_list = json.load(file)
    print('hi')
    insert_data(data_list)

