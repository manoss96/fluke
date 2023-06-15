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


QUEUE = "test-queue"
STORAGE_ACCOUNT = "account"
STORAGE_ACCOUNT_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/"
STORAGE_ACCOUNT_CONN_STRING = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};..."


def get_azure_auth_instance(from_conn_string: bool) -> AzureAuth:
    '''
    Returns a dummy ``AzureAuth`` instance.

    :param bool from_conn_string: Indicates whether \
        to construct the instance using a connection \
        string, or the regular Azure credentials.
    '''
    return (
        AzureAuth.from_conn_string(conn_string=STORAGE_ACCOUNT_CONN_STRING)
        if from_conn_string else 
        AzureAuth(
            account_url=STORAGE_ACCOUNT_URL,
            tenant_id='',
            client_id='',
            client_secret='')
    )


class MockQueueClient():

    def simulate_latency(func: Callable):
        '''
        This function is to be used as a decorator \
        function in order to simulate network latency \
        in mocked classes, so that cache-related methods \
        can be effectively tested.
        '''
        def wrapper(*args, **kwargs):
            time.sleep(0.02)
            return func(*args, **kwargs)
        return wrapper
    
    class MockQueueProperties():

        def __init__(self, name: str) -> None:
            self.name = name
            self.approximate_message_count = 0

    class MockItemPages(list):

        class MockQueueMessage():

            def __init__(self, id: str, content: str) -> None:
                self.id = id
                self.content = content
                self.pop_receipt = None

        def __init__(
            self,
            messages: list[MockQueueMessage],
            messages_per_page: int,
            max_messages: int
        ) -> None:
            self.__messages = messages
            self.__messages_per_page = messages_per_page
            self.__max_messages = len(messages) if max_messages is None else max_messages

        def by_page(self) -> Iterator[MockQueueMessage]:
            k = 0
            for i in range(0, self.__max_messages, self.__messages_per_page):
                # This means that messages are being pulled, therefore deleted.
                if len(self.__messages[i*self.__messages_per_page:(i+1)*self.__messages_per_page]) == 0:
                    k += self.__messages_per_page
                idx = i-k
                for j in range(i, len(self.__messages[idx*self.__messages_per_page:(idx+1)*self.__messages_per_page])):
                    self.__messages[j].pop_receipt = str(j)
                yield (msg for msg in self.__messages[idx*self.__messages_per_page:(idx+1)*self.__messages_per_page])
                

    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.properties = __class__.MockQueueProperties(queue_name)
        self.messages: list[__class__.MockItemPages.MockQueueMessage] = []


    @staticmethod
    def get_mock_methods() -> dict[str, Mock]:
        '''
        Returns a dictionary containing all the \
        methods that are to be mocked.
        '''
        return {
            'azure.identity.ClientSecretCredential.__init__': Mock(return_value=None),
            'azure.storage.queue.QueueClient.__init__': Mock(return_value=None),
            'azure.storage.queue.QueueClient.from_connection_string': Mock(
                return_value=MockQueueClient(QUEUE)
            )
        }

    @simulate_latency
    def get_queue_properties(self) -> MockQueueProperties:
        return self.properties
    
    @simulate_latency
    def send_message(self, content: str) -> None:
        self.messages.append(__class__.MockItemPages.MockQueueMessage(
            id=str(len(self.messages)),
            content=content))
        self.properties.approximate_message_count += 1

    @simulate_latency
    def peek_messages(self, max_messages: int) -> None:
        return self.messages[:max_messages]
    
    @simulate_latency
    def receive_messages(
        self,
        messages_per_page: int,
        max_messages: int,
        visibility_timeout: int
    ) -> MockItemPages[MockItemPages.MockQueueMessage]:
        return __class__.MockItemPages(
            self.messages,
            messages_per_page,
            max_messages
        )
    
    @simulate_latency
    def delete_message(self, id: str, pop_receipt: str) -> None:
        delete_idx = None
        for i, msg in enumerate(self.messages):
            if msg.id == id and msg.pop_receipt == pop_receipt:
                delete_idx = i
        
        if delete_idx is None:
            raise Exception("Message not found")
        
        self.messages.pop(delete_idx)
        self.properties.approximate_message_count -= 1

    @simulate_latency
    def clear_messages(self) -> None:
        self.messages = list()
        self.properties.approximate_message_count = 0

    @simulate_latency
    def close(self):
        pass


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
            'queue': QUEUE
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
        ).create_queue(QueueName=QUEUE)

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
            self.assertEqual(queue.get_name(), QUEUE)

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


class TestAzureStorageQueue(unittest.TestCase):

    def setUp(self):
        for k, v in MockQueueClient.get_mock_methods().items():
            patch(k, v).start()

    def tearDown(self):
        patch.stopall()

    @staticmethod
    def build_queue(
        from_conn_string: str = STORAGE_ACCOUNT_CONN_STRING
    ) -> AzureStorageQueue:
        return AzureStorageQueue(**{
            'auth': get_azure_auth_instance(from_conn_string),
            'queue': QUEUE
        })

    def test_is_open_on_True(self):
        with self.build_queue() as queue:
            self.assertEqual(queue.is_open(), True)

    def test_is_open_on_False(self):
        queue = self.build_queue()
        queue.close()
        self.assertEqual(queue.is_open(), False)

    def test_get_name(self):
        with self.build_queue() as queue:
            self.assertEqual(queue.get_name(), QUEUE)

    def test_count(self):
        with self.build_queue() as queue:
            self.assertEqual(queue.count(), 0)

    def test_push(self):
        message = "Hello"
        with self.build_queue() as queue:
            self.assertTrue(queue.push(message))
            self.assertEqual(queue.peek()[0], message)

    def test_peek(self):
        messages = set(str(i) for i in range(5))
        with self.build_queue() as queue:
            for msg in messages:
                queue.push(msg)
            self.assertTrue(set(queue.peek()).issubset(messages))

    def test_pull(self):
        messages = set(str(i) for i in range(5))
        with self.build_queue() as queue:
            for msg in messages:
                queue.push(msg)
            fetched = set(msg for batch in queue.pull() for msg in batch)
            self.assertSetEqual(fetched, messages)
            self.assertEqual(queue.count(), 0)

    def test_pull_on_num_messages(self):
        messages = set(str(i) for i in range(5))
        num_messages = 3
        with self.build_queue() as queue:
            for msg in messages:
                queue.push(msg)
            fetched = set(msg for batch in queue.pull(num_messages=num_messages) for msg in batch)
            self.assertTrue(fetched.issubset(messages))
            self.assertEqual(queue.count(), len(messages) - num_messages)

    def test_pull_on_batch_size(self):
        messages = set(str(i) for i in range(29))
        batch_size = 10
        batch_sizes = [10, 10, 9]
        counter = 0
        with self.build_queue() as queue:
            for msg in messages:
                queue.push(msg)
            for batch in queue.pull(batch_size=batch_size):
                self.assertEqual(len(batch), batch_sizes[counter])
                counter += 1

    def test_pull_on_clear(self):
        with self.build_queue() as queue:
            for i in range(50):
                queue.push(i)
            queue.clear()
            self.assertEqual(queue.count(), 0)


if __name__=="__main__":
    unittest.main()