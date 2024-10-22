import unittest
from unittest.mock import patch, MagicMock
from consumer import Consumer  # Adjust the import according to your file structure


class TestConsumer(unittest.TestCase):

    def setUp(self):
        # Mock the S3 and DynamoDB clients
        self.mock_s3 = MagicMock()
        self.mock_dynamodb = MagicMock()

        # Create an instance of the Consumer with the mock clients
        self.consumer = Consumer(self.mock_s3, self.mock_dynamodb)

    @patch('consumer.logging')  # Adjust the path to your logging module
    def test_process_create_request(self, mock_logging):
        # Create a sample widget request
        request = {
            'type': 'create',
            'widgetId': '123',
            'owner': 'John Doe',
            'label': 'Test Label',
            'description': 'Test Description',
            'otherAttributes': [{'name': 'color', 'value': 'red'}]
        }

        # Test the process_widget_request method
        self.consumer.process_widget_request(request)

        # Check if S3 client and DynamoDB client were called correctly
        owner = 'john-doe'
        widget_id = '123'
        self.mock_s3.save_widget.assert_called_with(request, owner, widget_id)
        self.mock_dynamodb.save_widget.assert_called_with(request)

        # Verify the log entries
        mock_logging.info.assert_any_call(f"Processing create request for widget: {widget_id}, owner: {owner}")
        mock_logging.info.assert_any_call(f"Successfully processed widget creation: {widget_id}")

    @patch('consumer.logging')  # Adjust the path to your logging module
    def test_process_unhandled_request(self, mock_logging):
        # Create a sample delete request
        request = {
            'type': 'delete',
            'widgetId': '123',
            'owner': 'John Doe'
        }

        # Test the process_widget_request method
        self.consumer.process_widget_request(request)

        # Ensure save_widget was not called for non-create requests
        self.mock_s3.save_widget.assert_not_called()
        self.mock_dynamodb.save_widget.assert_not_called()

        # Verify logging for unhandled request types
        mock_logging.info.assert_called_with(f"Unhandled request type: {request['type']}")

    @patch('consumer.logging')  # Adjust the path to your logging module
    def test_process_update_request(self, mock_logging):
        # Create a sample update request
        request = {
            'type': 'update',
            'widgetId': '123',
            'owner': 'John Doe'
        }

        # Test the process_widget_request method
        self.consumer.process_widget_request(request)

        # Ensure save_widget was not called for non-create requests
        self.mock_s3.save_widget.assert_not_called()
        self.mock_dynamodb.save_widget.assert_not_called()

        # Verify logging for unhandled request types
        mock_logging.info.assert_called_with(f"Unhandled request type: {request['type']}")

if __name__ == '__main__':
    unittest.main()
