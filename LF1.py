import re
import boto3
import json
import datetime


def lambda_handler(event, context):
    print(event)
    sqs = boto3.client('sqs')
    
    slots = event['sessionState']['intent']['slots']
    # intent = event['sessionState']['intent']['name']
    # Check the Cloudwatch logs to understand data inside event and
    # parse it to handle logic to validate user input and send it to Lex
    
    # Lex called LF1 with the user message and previous related state so
    # you can verify the user input. Validate and let Lex know what to do next.
    resp = {"statusCode": 200, "sessionState": event["sessionState"]}
    validation_result = validate(slots)
    # Lex will propose a next state if available but if user input is not valid,
    # you will modify it to tell Lex to ask the same question again (meaning ask
    # the current slot question again)
    if validation_result['isValid']:
        # If the input is valid, continue with the conversation.
        if slots['email'] != None:
            # push to SQS.
            message_body = push_message(slots)
            print(message_body)
            # print(sqs.send_message(QueueUrl=queue_url, MessageBody="suggestion information", MessageAttributes=message))
            print(sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/867536750101/Q1', MessageBody=json.dumps(message_body)))
            resp["sessionState"]["dialogAction"] = {"type": "Close"}
        if "proposedNextState" not in event:
            resp["sessionState"]["dialogAction"] = {"type": "Close"}
        else:
            resp["sessionState"]["dialogAction"] = event["proposedNextState"]["dialogAction"]
    else:
        # If the input is not valid, ask the user for input again.
        resp["sessionState"]["dialogAction"] = {
            "type": "ElicitSlot",
            "slotToElicit": validation_result['violatedSlot'],
            "message": {
                "contentType": "PlainText",
                "content": validation_result['message']
            }
        }
    
    print(slots)
    

    return resp
def push_message(slots):
    # message = {}
    # message['city'] = {'DataType': "String", 'StringValue': slots['city']['value']['interpretedValue']}
    # message['cuisine'] = {'DataType': "String", 'StringValue': slots['cuisine']['value']['interpretedValue']}
    # message['people_num'] = {'DataType': "String", 'StringValue': slots['people_num']['value']['interpretedValue']}
    # message['date'] = {'DataType': "String", 'StringValue': slots['date']['value']['interpretedValue']}
    # message['time'] = {'DataType': "String", 'StringValue': slots['time']['value']['interpretedValue']}
    # message['email'] = {'DataType': "String", 'StringValue': slots['email']['value']['interpretedValue']}
    message_body = {
            "city": slots['city']['value']['interpretedValue'],
            "cuisine": slots['cuisine']['value']['interpretedValue'],
            "people_num": slots['people_num']['value']['interpretedValue'],
            "date": slots['date']['value']['interpretedValue'],
            "time": slots['time']['value']['interpretedValue'],
            "email": slots['email']['value']['interpretedValue']
            }
    return message_body
    
def validate(slots):
    valid_cities = ['manhattan','new york']
    
        
    if slots['city'] != None:
        # it may be a sentence.
        city_value = slots['city']['value']['interpretedValue'].lower()
        for valid_city in valid_cities:
            if valid_city not in city_value:
                print("Not Valide location")
                return {
                'isValid': False,
                'violatedSlot': 'city',
                'message': 'We currently support only {} as a valid destination.?'.format(", ".join(valid_cities))
                }
            else:
                slots['city']['value']['interpretedValue'] = valid_city
                break
            
        
    if slots['people_num'] != None and int(slots['people_num']['value']['interpretedValue']) <= 0:
        print("The num is less than or equal to 0.")
        
        return {
        'isValid': False,
        'violatedSlot': 'people_num',
        'message': 'The number of people is negative number.'
        }
        
        
    # cusine type - only chinese, japanese, indian, seafood, korean, mexican, thai
    valid_cuisine = ['chinese', 'japanese', 'indian', 'seafood', 'korean', 'mexican', 'thai']
    if slots['cuisine'] != None and slots['cuisine']['value']['interpretedValue'].lower() not in valid_cuisine:
        print("The cuisine type is incorrect.")
        
        return {
        'isValid': False,
        'violatedSlot': 'cuisine',
        'message': 'The cuisine is incorrect.'
        }
    
    # date and time
    if slots['date'] != None:
        # Assuming the date format from Lex is YYYY-MM-DD
        date = slots['date']['value']['interpretedValue']
        year, month, day = map(int, date.split('-'))
        provided_date = datetime.datetime(year, month, day).date()
        if provided_date < datetime.date.today():
            print('The date is on the past.')
            return {
            'isValid': False,
            'violatedSlot': 'date',
            'message': 'The date is on the past.'
            }

    time_mappings = {
        'MO': datetime.time(11, 59),  # Morning ends at 11:59 AM
        'AF': datetime.time(17, 59),  # Afternoon ends at 5:59 PM
        'EV': datetime.time(20, 59),  # Evening ends at 8:59 PM
        'NI': datetime.time(23, 59)   # Night ends at 11:59 PM
    }
    
    if slots['time'] != None:
        # Assuming time format from Lex is HH:MM
        time = slots['time']['value']['interpretedValue']
        if time in time_mappings:
            provided_time = time_mappings[time]
        else:
            hour, minute = map(int, time.split(':'))
            provided_time = datetime.time(hour, minute)

        # If date is today, check if the time is in the future
        if date == datetime.date.today() and provided_time <= datetime.datetime.now().time():
            if provided_date < datetime.date.today():
                print('The time is on the past.')
                return {
                'isValid': False,
                'violatedSlot': 'time',
                'message': 'The time is on the past.'
                }
    
    return {'isValid': True}