import json
import boto3
from botocore.exceptions import ClientError
import requests
import datetime

# def lambda_handler(event, context):
# uni is the primary/paritition key
# note they all have unique attributes
# Replace 'YOUR_API_KEY' with your actual Yelp API key
api_key = 'W90dbQp07gqRCEzrYERTgWAcMVlSDJXY2jYMgjRYMpsLK0kC8chg_EVq9dfer2C7DLp-irS5irnhxgnwzPqqfcj-ThMHpJKkGefMzwOlD6d-OsKEYqMuJuav7fwiZXYx'
# Set the API endpoint URL for business search
api_url = 'https://api.yelp.com/v3/businesses/search'

# Define the parameters for the search
params_list = []

params_1 = {
    'location': 'Manhattan',  # Location to search in
    'term': 'restaurants, chinese',
    'limit': 50,  # Number of results to return
    'offset': 0
}

params_2 = {
    'location': 'Manhattan',  # Location to search in
    'term': 'restaurants, japanese',
    'limit': 50,  # Number of results to return
    'offset': 0
}

params_3 = {
    'location': 'Manhattan',  # Location to search in
    'term': 'restaurants, indian',
    'limit': 50,  # Number of results to return
    'offset': 0
}

params_4 = {
    'location': 'Manhattan',  # Location to search in
    'term': 'restaurants, seafood',
    'limit': 50,  # Number of results to return
    'offset': 0
}

params_5 = {
    'location': 'Manhattan',  # Location to search in
    'term': 'restaurants, korean',
    'limit': 50,  # Number of results to return
    'offset': 0
}

params_6 = {
    'location': 'Manhattan',  # Location to search in
    'term': 'restaurants, mexican',
    'limit': 50,  # Number of results to return
    'offset': 0
}

params_7 = {
    'location': 'Manhattan',  # Location to search in
    'term': 'restaurants, thai',
    'limit': 50,  # Number of results to return
    'offset': 0
}
cuisine_type = ['chinese', 'japanese', 'indian', 'seafood', 'korean', 'mexican', 'thai']
params_list.append(params_1)
params_list.append(params_2)
params_list.append(params_3)
params_list.append(params_4)
params_list.append(params_5)
params_list.append(params_6)
params_list.append(params_7)
cuisine_key = 0
# Set up the request headers with your,  API key
headers = {
    'Authorization': f'Bearer {api_key}'
}

data_list = []


total_count = 1000
file_path = "data.json"

# Make the GET request to the Yelp API
for params in params_list:
    offset = 0
    count = 0
    while (params['limit'] + params['offset'] <= total_count):
        response = requests.get(api_url, params=params, headers=headers)
        print(offset)
        # Check if the request was successful (HTTP status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            # Extract and print information about each business

            for business in data['businesses']:
                invalid = 0
                data = {}
                data['insertedAtTimestamp'] = str(datetime.datetime.now().timestamp())
                data['Business_ID'] = business['id']
                data['Name'] = business['name']
                data['Address'] = ', '.join(business['location']['display_address'])
                data['Coordinates'] = {'latitude': str(business['coordinates']['latitude']), 'longitude': str(business['coordinates']['longitude'])}
                data['review_count'] = business['review_count']
                data['rating'] = str(business['rating'])
                data['zip_code'] = business['location']['zip_code']
                for d in data_list:
                    if d['Business_ID'] == data['Business_ID']:
                        invalid = 1
                        break
                if invalid:
                    continue
                data_list.append(data)
                with open(file_path, 'a') as file:
                    file.write('{"index": {"_index": "restaurants", "_id": ' + str(data['Business_ID'] + '}}\n'))
                    file.write('{"Restaurant": ' + str(data['Business_ID']) + ', ' + '"cuisine": ' + str(cuisine_type[cuisine_key] + '}\n'))
                count += 1
            params['offset'] += 50
        else:
            print(f"Error: {response.status_code}")
            print(response.text)


    cuisine_key += 1

with open('dynamoData.json', 'a') as json_file:
    json.dump(data_list, json_file)
    # 1
    # insert_data(student)
    # 2
    # lookup_data({'uni': 'xx777'})
    # 3
    # update_item({'uni': 'xx777'}, 'Canada')
    # 4
    # delete_item({'uni': 'xx777'})

    # return
# file_path = "data.json"
#
# Write the data to the JSON file
# with open(file_path, 'w') as json_file:
#     json.dump(data_json, json_file)


def insert_data(data, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # overwrite if the same index is provided
    # put every data
    response = table.put_item(Item=data)
    print('@insert_data: response', response)
    return response


def lookup_data(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response['Item'])
        return response['Item']


def update_item(key, feature, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # change student location
    response = table.update_item(
        Key=key,
        UpdateExpression="set #feature=:f",
        ExpressionAttributeValues={
            ':f': feature
        },
        ExpressionAttributeNames={
            "#feature": "from"
        },
        ReturnValues="UPDATED_NEW"
    )
    print(response)
    return response


def delete_item(key, db=None, table='6998Demo'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.delete_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response)
        return response
