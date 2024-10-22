import boto3
import time
import json
import logging

# Set up logging configuration
logging.basicConfig(filename='consumer.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

class S3Client:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        logging.info(f"S3Client initialized for bucket: {bucket_name}")

    def get_next_widget_request(self):
        response = self.s3.list_objects_v2(Bucket=self.bucket_name)
        if 'Contents' not in response:
            logging.info("No more widget requests found in the bucket.")
            return None
        obj = response['Contents'][0]
        key = obj['Key']
        logging.info(f"Found widget request: {key}")
        obj_data = self.s3.get_object(Bucket=self.bucket_name, Key=key)
        request_data = obj_data['Body'].read().decode('utf-8')
        self.s3.delete_object(Bucket=self.bucket_name, Key=key)
        logging.info(f"Processed and deleted widget request: {key}")
        return json.loads(request_data)

    def save_widget(self, widget, owner, widget_id):
        s3_key = f'widgets/{owner}/{widget_id}'
        self.s3.put_object(Bucket=self.bucket_name, Key=s3_key, Body=json.dumps(widget))
        logging.info(f"Saved widget {widget_id} for owner {owner} in S3 under {s3_key}")


class DynamoDBClient:
    def __init__(self, table_name):
        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(table_name)
        logging.info(f"DynamoDBClient initialized for table: {table_name}")

    def save_widget(self, widget):
        dynamo_item = {
            'widget_id': widget['widgetId'],
            'owner': widget['owner'],
            'label': widget['label'],
            'description': widget['description'],
        }
        for attribute in widget.get('otherAttributes', []):
            dynamo_item[attribute['name']] = attribute['value']
        self.table.put_item(Item=dynamo_item)
        logging.info(f"Saved widget {widget['widgetId']} in DynamoDB")


class Consumer:
    def __init__(self, s3_client, dynamodb_client):
        self.s3_client = s3_client
        self.dynamodb_client = dynamodb_client
        logging.info("Consumer initialized.")

    def process_widget_request(self, request):
        if request['type'] == 'create':
            # Process create request
            owner = request['owner'].replace(' ', '-').lower()
            widget_id = request['widgetId']

            logging.info(f"Processing create request for widget: {widget_id}, owner: {owner}")

            # Save widget to S3 and DynamoDB
            self.s3_client.save_widget(request, owner, widget_id)
            self.dynamodb_client.save_widget(request)

            logging.info(f"Successfully processed widget creation: {widget_id}")
        else:
            # Ignore non-create requests for now
            logging.info(f"Unhandled request type: {request['type']}")

    def run(self):
        logging.info("Consumer started running.")
        while True:
            request = self.s3_client.get_next_widget_request()
            if request:
                self.process_widget_request(request)
            else:
                time.sleep(0.1)  # Short sleep to avoid busy waiting
                logging.debug("No widget request found, sleeping for a bit.")
