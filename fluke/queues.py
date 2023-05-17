from abc import ABC as _ABC
from abc import abstractmethod as _absmethod
from typing import Iterator as _Iterator
from typing import Optional as _Optional


import boto3 as _boto3
from azure.identity import ClientSecretCredential as _CSC
from azure.storage.queue import QueueClient as _QueueClient


from .auth import AWSAuth as _AWSAuth
from .auth import AzureAuth as _AzureAuth


class Queue(_ABC):
    '''
    An abstract class which serves as the \
    base class for all queue classes.
    '''

    @_absmethod
    def get_queue_name(self) -> str:
        '''
        Returns the name of the queue to which \
        a connection has been established.
        '''
        pass


    @_absmethod
    def is_open(self) -> bool:
        '''
        Returns a value indicating whether \
        this handler's underlying client connection \
        is open or not.
        '''
        pass


    @_absmethod
    def open(self) -> None:
        '''
        Opens all necessary connections.
        '''
        pass


    @_absmethod
    def close(self) -> None:
        '''
        Close all open connections.
        '''
        pass


    @_absmethod
    def push(
        self,
        message: str,
        suppress_output: bool = False
    ) -> bool:
        '''
        Pushes the provided message into the queue. \
        Returns ``True`` if said message was successfully \
        pushed into the queue, else returns ``False``.

        :param str message: A string message.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        pass


    @_absmethod
    def peek(self) -> list[str]:
        '''
        Returns a list containing at most 10 messages \
        currently residing within the queue.
        '''

    @_absmethod
    def pull(
        self,
        num_messages: _Optional[int] = None,
        batch_size: int = 10
    ) -> _Iterator[list[str]]:
        '''
        Iterates through the messages available in the queue \
        in distinct batches.

        :param int | None num_messages: The number of messages to \
            iterate through. If set to ``None``, then the queue \
            is constantly querried for new messages until there \
            are none. Defaults to ``None``.
        :param int batch_size: The maximum number of messages \
            a single batch may contain. Cannot be greater than \
            ``10``. Deafults to ``10``.

        :note: As soon as a batch of messages is received, all \
            messages within the batch are considered to have been \
            deleted from the queue.
        '''
        pass


class AWSSQSQueue(Queue):
    '''
    This class represents an Amazon SQS queue.

    :param AWSAuth auth: An ``AWSAuth`` instance \
        used in authenticating with AWS.
    :param str queue: The name of the Amazon SQS queue \
        to which a connection is to be established.
    '''

    def __init__(self, auth: _AWSAuth, queue: str):
        '''
        This class represents an Amazon SQS queue.

        :param AWSAuth auth: An ``AWSAuth`` instance \
            used in authenticating with AWS.
        :param str queue: The name of the Amazon SQS queue \
            to which a connection is to be established.
        '''
        self.__auth = auth
        self.__queue_name = queue
        self.__queue = None


    def get_queue_name(self) -> str:
        '''
        Returns the name of the queue to which \
        a connection has been established.
        '''
        return self.__queue_name
    

    def is_open(self) -> bool:
        '''
        Returns a value indicating whether \
        this handler's underlying client connection \
        is open or not.
        '''
        return self.__queue is not None


    def open(self) -> None:
        '''
        Opens an HTTP connection to \
        the Amazon SQS queue.
        '''

        if self.is_open():
            return

        self.__queue = _boto3.resource(
            service_name='sqs',
            **self.__auth.get_credentials()
        ).get_queue_by_name(QueueName=self.__queue_name)


    def close(self):
        '''
        Closes the HTTP connection to \
        the Amazon SQS queue.
        '''
        if self.__queue is not None:
            self.__queue.meta.client.close()
            self.__queue = None


    def push(
        self,
        message: str,
        suppress_output: bool = False
    ) -> bool:
        '''
        Pushes the provided message into the queue. \
        Returns ``True`` if said message was successfully \
        pushed into the queue, else returns ``False``.

        :param str message: A string message.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        try:
            self.__queue.send_message(
                MessageBody=message,
                DelaySeconds=0)
            if not suppress_output:
                print("Message sent successfully!")
            return True
        except Exception as e:
            if not suppress_output:
                print(f"Failed to send message: {e}")
            return False
    

    def peek(self) -> list[str]:
        '''
        Returns a list containing at most 10 messages \
        currently residing within the queue.
        '''
        return [
            msg.body for msg in self.__queue.receive_messages(
                AttributeNames=['QueueUrl'],
                VisibilityTimeout=0,
                MaxNumberOfMessages=10)
        ]

    
    def pull(
        self,
        num_messages: _Optional[int] = None,
        batch_size: int = 10
    ) -> _Iterator[list[str]]:
        '''
        Iterates through the messages available in the queue \
        in distinct batches.

        :param int | None num_messages: The number of messages to \
            iterate through. If set to ``None``, then the queue \
            is constantly querried for new messages until there \
            are none. Defaults to ``None``.
        :param int batch_size: The maximum number of messages \
            a single batch may contain. Cannot be greater than \
            ``10``. Deafults to ``10``.

        :note: As soon as a batch of messages is received, all \
            messages within the batch are considered to have been \
            deleted from the queue.
        '''

        num_messages_fetched = 0

        while num_messages is None or num_messages_fetched < num_messages:

            batch = self.__queue.receive_messages(
                AttributeNames=['QueueUrl'],
                VisibilityTimeout=0,
                MaxNumberOfMessages=min(
                    batch_size,
                    10 if num_messages is None
                    else num_messages-num_messages_fetched,
                    10))

            if len(batch) == 0:
                return

            resp = self.__queue.delete_messages(Entries=[
                {'Id': str(i),
                 'ReceiptHandle': msg.receipt_handle
                } for i, msg in enumerate(batch)])

            messages = []
            for i in (int(d['Id']) for d in resp['Successful']):
                num_messages_fetched += 1
                messages.append(batch[i].body)

            yield messages


class AzureStorageQueue(Queue):
    '''
    A class used in handling the HTTP \
    connection to an Azure Queue Storage \
    queue.

    :param AzureAuth auth: An ``AzureAuth`` instance \
        used in authenticating with Microsoft Azure.
    :param str queue: The name of the Azure Queue Storage \
        queue to which a connection is to be established.
    '''

    def __init__(self, auth: _AzureAuth, queue: str):
        '''
        A class used in handling the HTTP \
        connection to an Azure Queue Storage \
        queue.

        :param AzureAuth auth: An ``AzureAuth`` instance \
            used in authenticating with Microsoft Azure.
        :param str queue: The name of the Azure Queue Storage \
            queue to which a connection is to be established.
        '''
        self.__auth = auth
        self.__queue_name = queue
        self.__queue = None


    def get_queue_name(self) -> str:
        '''
        Returns the name of the queue to which \
        a connection has been established.
        '''
        return self.__queue_name
    

    def is_open(self) -> bool:
        '''
        Returns a value indicating whether \
        this handler's underlying client connection \
        is open or not.
        '''
        return self.__queue is not None


    def open(self) -> None:
        '''
        Opens an HTTP connection to the \
        Azure Queue Storage queue.
        '''

        if self.__queue is not None:
            return

        credentials = self.__auth.get_credentials()

        if 'conn_string' in credentials:
            self.__queue = _QueueClient.from_connection_string(
                conn_str=credentials['conn_string'],
                queue_name=self.__queue_name)
        else:
            self.__queue = _QueueClient(
                account_url=credentials.pop('account_url'),
                queue_name=self.__queue_name,
                credential=_CSC(**credentials))


    def close(self):
        '''
        Closes the HTTP connection to the \
        Azure Queue Storage queue.
        '''
        if self.__queue is not None:
            self.__queue.close()
            self.__queue = None


    def push(
        self,
        message: str,
        suppress_output: bool = False
    ) -> bool:
        '''
        Pushes the provided message into the queue. \
        Returns ``True`` if said message was successfully \
        pushed into the queue, else returns ``False``.

        :param str message: A string message.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        try:
            self.__queue.send_message(content=message)
            if not suppress_output:
                print("Message sent successfully!")
            return True
        except Exception as e:
            if not suppress_output:
                print(f"Failed to send message: {e}")
            return False
    

    def peek(self) -> list[str]:
        '''
        Returns a list containing at most 10 messages \
        currently residing within the queue.
        '''
        return [
            msg.content
            for batch in self.__queue.receive_messages(
                messages_per_page=10,
                max_messages=10,
                visibility_timeout=1).by_page()
            for msg in batch
        ]

    
    def pull(
        self,
        num_messages: _Optional[int] = None,
        batch_size: int = 10
    ) -> _Iterator[list[str]]:
        '''
        Iterates through the messages available in the queue \
        in distinct batches.

        :param int | None num_messages: The number of messages to \
            iterate through. If set to ``None``, then the queue \
            is constantly querried for new messages until there \
            are none. Defaults to ``None``.
        :param int batch_size: The maximum number of messages \
            a single batch may contain. Cannot be greater than \
            ``10``. Deafults to ``10``.

        :note: As soon as a batch of messages is received, all \
            messages within the batch are considered to have been \
            deleted from the queue.
        '''

        for batch in self.__queue.receive_messages(
                messages_per_page=min(batch_size, 10),
                max_messages=num_messages,
                visibility_timeout=1
        ).by_page():
            messages = []
            for msg in batch:
                try:
                    self.__queue.delete_message(msg.id, msg.pop_receipt)
                    messages.append(msg.content)
                except:
                    continue
            yield messages