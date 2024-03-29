import json
import os

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

REGION = 'us-east-1'
HOST = 'search-restaurants-bh6cagbdvkryh6m4i7ttc3xyv4.us-east-1.es.amazonaws.com'
INDEX = 'restaurants'
queue = "https://sqs.us-east-1.amazonaws.com/867536750101/Q1"
ses = boto3.client('ses', region_name='us-east-1')
sender_email = 'cz2791@columbia.edu'

def lambda_handler(event, context):
    print('Received event: ' + json.dumps(event))

    
    
    # print(results[0]['Restaurant'])
    
    sqs_client = boto3.client("sqs")
    messages = sqs_client.receive_message(
        QueueUrl=queue,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5,
    )
    data = {}
    if 'Messages' in messages:
        message = messages['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        
        data = json.loads(message['Body'])
        
        
        results = query(data['cuisine'])    
        recipient_email = data['email']
        keys = []
        for i in range(3):
            keys.append(results[i]['Restaurant'])
        
        data_list = []
        for key in keys:
            dict_key = {"Business_ID": key}
            data_list.append(lookup_data(dict_key))
            
        print(data_list)
        
        i = 1
        text = ' '
        for d in data_list:
            text = text + str(i) + ". " + d['Name'] + ' located at ' + d['Address'] + ' '
            i = i + 1
        
        subject = 'restaurants suggestion'
        body_text = 'Hello! Here are my ' + data['cuisine'] + ' restaurant suggestions for ' + data['people_num'] + ' people for ' + data['date'] + ' at ' + data['time'] + text
        # body_html = '<html><body><p>This is the email body in HTML format.</p></body></html>'
        print("body_text:" + body_text)
        # Send the email
        response = ses.send_email(
            Source=sender_email,
            Destination={'ToAddresses': [recipient_email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {
                    'Text': {'Data': body_text},
                }
            }
        )
        
        sqs_client.delete_message(
            QueueUrl=queue,
            ReceiptHandle=receipt_handle
        )
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps({'results': results})
        }
        
    print('not correct way')


def query(term):
    q = {'size': 3, 'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results


def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)


def lookup_data(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        return response['Item']