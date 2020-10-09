import json
import boto3
import os

"""
Instructions to update this function in Lamda, run in terminal:

```
zip function.zip lambda_function.py
```

```
aws lambda update-function-code --function-name test --zip-file fileb://function.zip
```
"""

def lambda_handler(event, context):

    try:
        for messages_wrapper in event:
            messages_body = json.loads(messages_wrapper['Body'])
            
            message_timestamp = messages_body['Timestamp'] if 'Timestamp' in messages_body else None
            message_id = messages_body['MessageId'] if 'MessageId' in messages_body else None
            topic_arn = messages_body['TopicArn'] if 'TopicArn' in messages_body else None
        
            message = json.loads(messages_body['Message'])
            
            key = message['key']
            ttl = message['ttl'] 
        
            object_size = message['object_size']
        
            bucket = message['bucket']
            model = message['model']
            name = message['name']
            long_name = message['long_name'] if 'long_name' in message else None
            created_time = message['created_time'] if 'created_time' in message else None
            time = message['time']
            height = message['height'] if 'height' in message else None
            depth = message['depth'] if 'depth' in message else None
            height = message['height'] if 'height' in message else None
            height_units = message['height_units'] if 'height_units' in message else None
            depth_units = message['depth_units'] if 'depth_units' in message else None
            forecast_reference_time = message['forecast_reference_time'] if 'forecast_reference_time' in message else None 
            forecast_period_bounds = message['forecast_period_bounds'] if 'forecast_period_bounds' in message else None 
            forecast_period_units = message['forecast_period_units'] if 'forecast_period_units' in message else None 
            forecast_period = message['forecast_period'] if 'forecast_period' in message else None 
            cell_methods = message['cell_methods'] if 'cell_methods' in message else None
        
            details = {
                'key': key,
                'message_id': message_id,
                'topic_arn': topic_arn,
                'object_size': object_size,
                'model': model,
                'bucket': bucket,
                'long_name': long_name,
                'created_time': created_time,
                'time': time,
                'height': [float(h) for h in height.split(' ')] if height else None,
                'height_units': height_units,        
                'depth': depth,
                'depth_units': depth_units,        
                'forecast_reference_time': forecast_reference_time,
                'forecast_period_bounds': [float(p) for p in forecast_period_bounds.split(' ')] if forecast_period_bounds else None,
                'forecast_period_units': forecast_period_units,
                'forecast_period': int(forecast_period),
                'ttl': ttl,
                'cell_methods': cell_methods,
                'message_timestamp': message_timestamp,
            }
            
            print(details)


            
    except Exception as e:
        # Send some context about this error to Lambda Logs
        print(e)
        # throw exception, do not handle. Lambda will make message visible again.
        raise e



