import os
import io
import sys
import time
import shutil
import unittest
from typing import Optional, Iterator, Callable
from unittest.mock import Mock, patch


import boto3
from moto import mock_sqs


from fluke.auth import AWSAuth, AzureAuth
from fluke.queues import AWSSQSQueue, AzureStorageQueue


QUEUE_NAME = "test-queue"


class TestAWSSQSQueue(unittest.TestCase):

    MOCK_SQS = mock_sqs()
    QUEUE = None

    @staticmethod
    def build_queue() -> AWSSQSQueue:
        return AWSSQSQueue(**{
            'auth': AWSAuth(
                aws_access_key_id='',
                aws_secret_access_key='',
                region=AWSAuth.Region.EUROPE_WEST_1),
            'queue': QUEUE_NAME
        })
    
    def get_num_messages(self) -> int:
        self.QUEUE.load()
        return int(self.QUEUE.attributes['ApproximateNumberOfMessages'])

    def fetch_messages(self) -> list[str]:

        messages = []
        while len(batch := self.QUEUE.receive_messages(
            AttributeNames=['All'],
            MaxNumberOfMessages=10,
            VisibilityTimeout=60,
            WaitTimeSeconds=0)
        ) > 0:
            resp = self.QUEUE.delete_messages(Entries=[
                {
                    'Id': str(i),
                    'ReceiptHandle': msg.receipt_handle
                } for i, msg in enumerate(batch)])
            
            for i in map(lambda d: int(d['Id']), resp['Successful']):
                messages.append(batch[i].body)
            
        return messages
    
    def send_message(self, message: str) -> None:
        self.QUEUE.send_message(
            MessageBody=message,
            DelaySeconds=0)

    def send_messages(self, messages: list[str]) -> None:
        for msg in messages:
            self.send_message(msg)

    def purge(self) -> None:
        self.QUEUE.purge()

    def setUp(self):
        self.MOCK_SQS.start()
        self.QUEUE = boto3.resource(
            'sqs',
            endpoint_url="https://sqs.eu-west-1.amazonaws.com",
            region_name="eu-west-1"
        ).create_queue(QueueName=QUEUE_NAME)

    def tearDown(self):
        self.MOCK_SQS.stop()

    def test_is_open_on_True(self):
        with self.build_queue() as queue:
            self.assertEqual(queue.is_open(), True)

    def test_is_open_on_False(self):
        queue = self.build_queue()
        queue.close()
        self.assertEqual(queue.is_open(), False)

    def test_get_name(self):
        with self.build_queue() as queue:
            self.assertEqual(queue.get_name(), QUEUE_NAME)

    def test_count(self):
        num_messages = 5
        self.send_messages([str(i) for i in range(num_messages)])
        with self.build_queue() as queue:
            self.assertEqual(queue.count(), num_messages)

    def test_push(self):
        message = "Hello"
        with self.build_queue() as queue:
            self.assertTrue(queue.push(message))
            self.assertEqual(self.fetch_messages()[0], message)

    def test_peek(self):
        messages = set(str(i) for i in range(5))
        self.send_messages(messages)
        with self.build_queue() as queue:
            self.assertTrue(set(queue.peek()).issubset(messages))
        self.purge()

    def test_pull(self):
        messages = set(str(i) for i in range(5))
        self.send_messages(messages)
        with self.build_queue() as queue:
            fetched = set(msg for batch in queue.pull() for msg in batch)
            self.assertSetEqual(fetched, messages)
            self.assertEqual(self.get_num_messages(), 0)
        self.purge()

    def test_pull_on_num_messages(self):
        messages = set(str(i) for i in range(5))
        num_messages = 3
        self.send_messages(messages)
        with self.build_queue() as queue:
            fetched = set(msg for batch in queue.pull(num_messages=num_messages) for msg in batch)
            self.assertTrue(fetched.issubset(messages))
            self.assertEqual(self.get_num_messages(), len(messages) - num_messages)
        self.purge()

    def test_pull_on_batch_size(self):
        messages = set(str(i) for i in range(29))
        batch_size = 10
        batch_sizes = [10, 10, 9]
        self.send_messages(messages)
        counter = 0
        with self.build_queue() as queue:
            for batch in queue.pull(batch_size=batch_size):
                self.assertEqual(len(batch), batch_sizes[counter])
                counter += 1
        self.purge()

    def test_pull_on_clear(self):
        self.send_messages(set(str(i) for i in range(50)))
        with self.build_queue() as queue:
            queue.clear()
            self.assertEqual(self.get_num_messages(), 0)
        self.purge()

if __name__=="__main__":
    unittest.main()
