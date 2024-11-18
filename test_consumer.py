import unittest
from unittest.mock import patch, MagicMock
from consumer import Consumer, SQSClient, S3Client, DynamoDBClient

class TestSQSClient(unittest.TestCase):
    def setUp(self):
        self.mock_sqs = MagicMock()
        with patch('boto3.client') as mock_boto3:
            mock_boto3.return_value = self.mock_sqs
            self.sqs_client = SQSClient('test-queue')
            self.mock_sqs.get_queue_url.return_value = {'QueueUrl': 'test-url'}

    def test_get_next_widget_request_empty_queue(self):
        self.mock_sqs.receive_message.return_value = {}
        result = self.sqs_client.get_next_widget_request()
        self.assertIsNone(result)

    def test_get_next_widget_request_with_messages(self):
        messages = [
            {
                'Body': '{"type": "create", "widgetId": "123"}',
                'ReceiptHandle': 'receipt1'
            },
            {
                'Body': '{"type": "update", "widgetId": "456"}',
                'ReceiptHandle': 'receipt2'
            }
        ]
        self.mock_sqs.receive_message.return_value = {'Messages': messages}
        
        # First call should return first message
        result1 = self.sqs_client.get_next_widget_request()
        self.assertEqual(result1['receipt_handle'], 'receipt1')
        
        # Second call should return cached message
        result2 = self.sqs_client.get_next_widget_request()
        self.assertEqual(result2['receipt_handle'], 'receipt2')

    def test_delete_message(self):
        self.sqs_client.delete_message('test-receipt')
        self.mock_sqs.delete_message.assert_called_with(
            QueueUrl='test-url',
            ReceiptHandle='test-receipt'
        )

class TestConsumer(unittest.TestCase):
    def setUp(self):
        self.mock_source = MagicMock()
        self.mock_s3 = MagicMock()
        self.mock_dynamodb = MagicMock()
        self.consumer = Consumer(self.mock_source, self.mock_s3, self.mock_dynamodb)

    @patch('consumer.logging')
    def test_process_create_request(self, mock_logging):
        request = {
            'request': {
                'type': 'create',
                'widgetId': '123',
                'owner': 'John Doe',
                'label': 'Test Label',
                'description': 'Test Description',
                'otherAttributes': [{'name': 'color', 'value': 'red'}]
            },
            'receipt_handle': 'test-receipt'
        }

        self.consumer.process_widget_request(request)

        owner = 'john-doe'
        widget_id = '123'
        self.mock_s3.save_widget.assert_called_with(request['request'], owner, widget_id)
        self.mock_dynamodb.save_widget.assert_called_with(request['request'])

    @patch('consumer.logging')
    def test_process_delete_request(self, mock_logging):
        request = {
            'request': {
                'type': 'delete',
                'widgetId': '123',
                'owner': 'John Doe'
            },
            'receipt_handle': 'test-receipt'
        }

        # Configure mocks to indicate successful deletion
        self.mock_s3.delete_widget.return_value = True
        self.mock_dynamodb.delete_widget.return_value = True

        self.consumer.process_widget_request(request)

        owner = 'john-doe'
        widget_id = '123'
        self.mock_s3.delete_widget.assert_called_with(owner, widget_id)
        self.mock_dynamodb.delete_widget.assert_called_with(widget_id, owner)

    @patch('consumer.logging')
    def test_process_update_request(self, mock_logging):
        request = {
            'request': {
                'type': 'update',
                'widgetId': '123',
                'owner': 'John Doe',
                'label': 'Updated Label',
                'description': 'Updated Description'
            },
            'receipt_handle': 'test-receipt'
        }

        # Configure mocks to indicate successful update
        self.mock_s3.update_widget.return_value = True
        self.mock_dynamodb.update_widget.return_value = True

        self.consumer.process_widget_request(request)

        owner = 'john-doe'
        widget_id = '123'
        self.mock_s3.update_widget.assert_called_with(request['request'], owner, widget_id)
        self.mock_dynamodb.update_widget.assert_called_with(request['request'])

    @patch('consumer.logging')
    def test_failed_update_request(self, mock_logging):
        request = {
            'request': {
                'type': 'update',
                'widgetId': '123',
                'owner': 'John Doe',
                'label': 'Updated Label',
                'description': 'Updated Description'
            },
            'receipt_handle': 'test-receipt'
        }

        # Configure mocks to indicate failed update
        self.mock_s3.update_widget.return_value = False
        self.mock_dynamodb.update_widget.return_value = False

        self.consumer.process_widget_request(request)

        # Verify error was logged
        mock_logging.error.assert_called_with("Failed to process widget update: 123")

    @patch('consumer.logging')
    def test_failed_delete_request(self, mock_logging):
        request = {
            'request': {
                'type': 'delete',
                'widgetId': '123',
                'owner': 'John Doe'
            },
            'receipt_handle': 'test-receipt'
        }

        # Configure mocks to indicate failed deletion
        self.mock_s3.delete_widget.return_value = False
        self.mock_dynamodb.delete_widget.return_value = False

        self.consumer.process_widget_request(request)

        # Verify error was logged
        mock_logging.error.assert_called_with("Failed to process widget deletion: 123")

    @patch('consumer.logging')
    def test_sqs_message_deletion(self, mock_logging):
        # Create a mock SQS source client
        mock_sqs_source = MagicMock(spec=SQSClient)
        consumer = Consumer(mock_sqs_source, self.mock_s3, self.mock_dynamodb)

        request = {
            'request': {
                'type': 'create',
                'widgetId': '123',
                'owner': 'John Doe'
            },
            'receipt_handle': 'test-receipt'
        }

        consumer.process_widget_request(request)

        # Verify that delete_message was called with the correct receipt handle
        mock_sqs_source.delete_message.assert_called_with('test-receipt')

    def test_run_method(self):
        # Set up mock requests
        request1 = {
            'request': {
                'type': 'create',
                'widgetId': '123',
                'owner': 'John Doe'
            },
            'receipt_handle': 'receipt1'
        }
        request2 = None  # Simulate no more messages

        # Configure source client to return one request then None
        self.mock_source.get_next_widget_request.side_effect = [request1, request2]

        # Mock time.sleep to avoid actual sleeping
        with patch('time.sleep') as mock_sleep:
            # Run the consumer for a few iterations
            with patch.object(self.consumer, 'run', side_effect=lambda: None) as mock_run:
                self.consumer.run()
                
                # Verify that get_next_widget_request was called
                self.mock_source.get_next_widget_request.assert_called()

class TestS3Client(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = MagicMock()
        with patch('boto3.client') as mock_boto3:
            mock_boto3.return_value = self.mock_s3
            self.s3_client = S3Client('test-bucket')

    def test_update_widget_success(self):
        widget = {
            'widgetId': '123',
            'owner': 'john-doe',
            'label': 'Test Widget'
        }
        
        # Mock successful head_object (widget exists)
        self.mock_s3.head_object.return_value = {}
        
        result = self.s3_client.update_widget(widget, 'john-doe', '123')
        self.assertTrue(result)
        self.mock_s3.put_object.assert_called_once()

    def test_update_widget_failure(self):
        widget = {
            'widgetId': '123',
            'owner': 'john-doe',
            'label': 'Test Widget'
        }
        
        # Mock widget not found
        self.mock_s3.head_object.side_effect = Exception('Widget not found')
        
        result = self.s3_client.update_widget(widget, 'john-doe', '123')
        self.assertFalse(result)
        self.mock_s3.put_object.assert_not_called()

class TestDynamoDBClient(unittest.TestCase):
    def setUp(self):
        self.mock_table = MagicMock()
        with patch('boto3.resource') as mock_boto3:
            mock_boto3.return_value.Table.return_value = self.mock_table
            self.dynamodb_client = DynamoDBClient('test-table')

    def test_update_widget_success(self):
        widget = {
            'widgetId': '123',
            'owner': 'john-doe',
            'label': 'Test Widget',
            'description': 'Test Description'
        }
        
        # Mock successful get_item (widget exists)
        self.mock_table.get_item.return_value = {'Item': widget}
        
        result = self.dynamodb_client.update_widget(widget)
        self.assertTrue(result)
        self.mock_table.put_item.assert_called_once()

    def test_update_widget_failure(self):
        widget = {
            'widgetId': '123',
            'owner': 'john-doe',
            'label': 'Test Widget'
        }
        
        # Mock widget not found
        self.mock_table.get_item.side_effect = Exception('Widget not found')
        
        result = self.dynamodb_client.update_widget(widget)
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()