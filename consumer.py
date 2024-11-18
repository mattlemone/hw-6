import boto3
import time
import json
import logging
import argparse
from typing import Optional, List, Dict, Any

# Set up logging configuration
logging.basicConfig(filename='consumer.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

class SQSClient:
    def __init__(self, queue_name: str):
        self.sqs = boto3.client('sqs')
        self.queue_url = self.get_queue_url(queue_name)
        self.message_cache: List[Dict] = []
        logging.info(f"SQSClient initialized for queue: {queue_name}")

    def get_queue_url(self, queue_name: str) -> str:
        response = self.sqs.get_queue_url(QueueName=queue_name)
        return response['QueueUrl']

    def get_next_widget_request(self) -> Optional[Dict]:
        # Return cached message if available
        if self.message_cache:
            message = self.message_cache.pop(0)
            return {
                'request': json.loads(message['Body']),
                'receipt_handle': message['ReceiptHandle']
            }

        # Fetch new batch of messages if cache is empty
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1
        )

        if 'Messages' not in response:
            logging.info("No messages found in the queue")
            return None

        # Cache received messages except the first one
        messages = response['Messages']
        self.message_cache.extend(messages[1:])

        # Return the first message
        message = messages[0]
        return {
            'request': json.loads(message['Body']),
            'receipt_handle': message['ReceiptHandle']
        }

    def delete_message(self, receipt_handle: str):
        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )
        logging.info("Deleted message from queue")

class S3Client:
    def __init__(self, bucket_name: str):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        logging.info(f"S3Client initialized for bucket: {bucket_name}")

    def get_next_widget_request(self) -> Optional[Dict]:
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
        return {'request': json.loads(request_data)}

    def save_widget(self, widget: Dict, owner: str, widget_id: str):
        s3_key = f'widgets/{owner}/{widget_id}'
        self.s3.put_object(Bucket=self.bucket_name, Key=s3_key, Body=json.dumps(widget))
        logging.info(f"Saved widget {widget_id} for owner {owner} in S3 under {s3_key}")

    def delete_widget(self, owner: str, widget_id: str):
        s3_key = f'widgets/{owner}/{widget_id}'
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logging.info(f"Deleted widget {widget_id} for owner {owner} from S3")
            return True
        except Exception as e:
            logging.error(f"Failed to delete widget from S3: {str(e)}")
            return False

    def update_widget(self, widget: Dict, owner: str, widget_id: str) -> bool:
        s3_key = f'widgets/{owner}/{widget_id}'
        try:
            # Check if widget exists
            self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            # Update widget
            self.s3.put_object(Bucket=self.bucket_name, Key=s3_key, Body=json.dumps(widget))
            logging.info(f"Updated widget {widget_id} for owner {owner} in S3")
            return True
        except Exception as e:
            logging.error(f"Failed to update widget in S3: {str(e)}")
            return False

class DynamoDBClient:
    def __init__(self, table_name: str):
        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(table_name)
        logging.info(f"DynamoDBClient initialized for table: {table_name}")

    def save_widget(self, widget: Dict):
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

    def delete_widget(self, widget_id: str, owner: str) -> bool:
        try:
            self.table.delete_item(
                Key={
                    'widget_id': widget_id,
                    'owner': owner
                }
            )
            logging.info(f"Deleted widget {widget_id} from DynamoDB")
            return True
        except Exception as e:
            logging.error(f"Failed to delete widget from DynamoDB: {str(e)}")
            return False

    def update_widget(self, widget: Dict) -> bool:
        try:
            # Check if widget exists
            self.table.get_item(
                Key={
                    'widget_id': widget['widgetId'],
                    'owner': widget['owner']
                }
            )
            # Update widget
            self.save_widget(widget)
            logging.info(f"Updated widget {widget['widgetId']} in DynamoDB")
            return True
        except Exception as e:
            logging.error(f"Failed to update widget in DynamoDB: {str(e)}")
            return False

class Consumer:
    def __init__(self, source_client, s3_client: S3Client, dynamodb_client: DynamoDBClient):
        self.source_client = source_client
        self.s3_client = s3_client
        self.dynamodb_client = dynamodb_client
        logging.info("Consumer initialized.")

    def process_widget_request(self, request_data: Dict):
        request = request_data['request']
        request_type = request['type']
        owner = request['owner'].replace(' ', '-').lower()
        widget_id = request['widgetId']

        logging.info(f"Processing {request_type} request for widget: {widget_id}, owner: {owner}")

        if request_type == 'create':
            self.s3_client.save_widget(request, owner, widget_id)
            self.dynamodb_client.save_widget(request)
            logging.info(f"Successfully processed widget creation: {widget_id}")
        
        elif request_type == 'delete':
            s3_success = self.s3_client.delete_widget(owner, widget_id)
            dynamo_success = self.dynamodb_client.delete_widget(widget_id, owner)
            if s3_success and dynamo_success:
                logging.info(f"Successfully processed widget deletion: {widget_id}")
            else:
                logging.error(f"Failed to process widget deletion: {widget_id}")

        elif request_type == 'update':
            s3_success = self.s3_client.update_widget(request, owner, widget_id)
            dynamo_success = self.dynamodb_client.update_widget(request)
            if s3_success and dynamo_success:
                logging.info(f"Successfully processed widget update: {widget_id}")
            else:
                logging.error(f"Failed to process widget update: {widget_id}")

        # Delete message from SQS if applicable
        if isinstance(self.source_client, SQSClient) and 'receipt_handle' in request_data:
            self.source_client.delete_message(request_data['receipt_handle'])

    def run(self):
        logging.info("Consumer started running.")
        while True:
            request = self.source_client.get_next_widget_request()
            if request:
                self.process_widget_request(request)
            else:
                time.sleep(0.1)  # Short sleep to avoid busy waiting
                logging.debug("No widget request found, sleeping for a bit.")

def main():
    parser = argparse.ArgumentParser(description='Widget Request Consumer')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--s3-bucket', help='S3 bucket name for widget requests')
    group.add_argument('--sqs-queue', help='SQS queue name for widget requests')
    parser.add_argument('--dynamodb-table', required=True, help='DynamoDB table name for widgets')
    args = parser.parse_args()

    # Initialize appropriate source client
    if args.s3_bucket:
        source_client = S3Client(args.s3_bucket)
    else:
        source_client = SQSClient(args.sqs_queue)

    # Initialize S3 and DynamoDB clients
    s3_client = S3Client(args.s3_bucket if args.s3_bucket else 'widgets-bucket')
    dynamodb_client = DynamoDBClient(args.dynamodb_table)

    # Create and run consumer
    consumer = Consumer(source_client, s3_client, dynamodb_client)
    consumer.run()

if __name__ == '__main__':
    main()