import random as _rand
import warnings as _warn
from abc import ABC as _ABC
from abc import abstractmethod as _absmethod
from typing import Iterator as _Iterator
from typing import Optional as _Optional


import boto3 as _boto3
from azure.identity import ClientSecretCredential as _CSC
from azure.storage.queue import QueueClient as _QueueClient


from .auth import AWSAuth as _AWSAuth
from .auth import AzureAuth as _AzureAuth


class _Queue(_ABC):
    '''
    An abstract class which serves as the \
    base class for all queue classes.
    '''

    def __enter__(self) -> '_Queue':
        '''
        Enter the runtime context related to this instance.
        '''
        return self


    def __exit__(self, exc_type, exc_value, traceback) -> None:
        '''
        Exit the runtime context related to this object. 
        '''
        self.close()


    def __del__(self) -> None:
        '''
        The class destructor method.
        '''
        if self.is_open():
            # Display warning.
            msg = f'You might want to consider instantiating class "{self.__class__.__name__}"'
            msg += " through the use of a context manager by utilizing Python's"
            msg += ' "with" statement, or by simply invoking an instance\'s'
            msg += ' "close" method after being done using it.'
            _warn.warn(msg, ResourceWarning)
            # Close connections.
            self.close()


    @_absmethod
    def get_name(self) -> str:
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
    def peek(self, suppress_output: bool = False) -> list[str]:
        '''
        Returns a list containing at most ten messages \
        currently residing within the queue.

        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.

        :note: This method does not go on to explicitly \
            remove messages from the queue. However, any \
            message returned by this method will have its \
            "receive count" increased, which in turn might \
            result in said message being removed from the \
            queue in case the queue's maximum receive count \
            threshold is exceeded.
        '''
        pass

    @_absmethod
    def pull(
        self,
        num_messages: _Optional[int] = None,
        batch_size: int = 10,
        suppress_output: bool = False
    ) -> _Iterator[list[str]]:
        '''
        Iterates through the messages available in the queue \
        in distinct batches.

        :param int | None num_messages: The number of messages to \
            iterate through. If set to ``None``, then the queue \
            is constantly querried for new messages until there \
            are none. Defaults to ``None``.
        :param int batch_size: The maximum number of messages \
            a single batch may contain. Deafults to ``10``.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.

        :note: As soon as a batch of messages is received, all \
            messages within the batch are considered to have \
            been deleted from the queue.
        '''
        pass


class AWSSQSQueue(_Queue):
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
        self.open()


    def get_name(self) -> str:
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
        
        # Fix deprecated endpoint.
        creds = self.__auth.get_credentials()
        if (region := creds['region_name']) is not None:
            creds.update({'endpoint_url': f"https://sqs.{region}.amazonaws.com"})

        self.__queue = _boto3.resource(
            service_name='sqs', **creds
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
        if not suppress_output:
            print(f'\nPushing message "{message}" to queue "{self.get_name()}".')
        
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
    

    def peek(self, suppress_output: bool = False) -> list[str]:
        '''
        Returns a list containing at most ten messages \
        currently residing within the queue.

        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.

        :note: This method does not go on to explicitly \
            remove messages from the queue. However, any \
            message returned by this method will have its \
            "receive count" increased, which in turn might \
            result in said message being removed from the \
            queue in case the queue's maximum receive count \
            threshold is exceeded.
        '''
        if not suppress_output:
            print(f'\nPeeking messages from queue "{self.get_name()}".')

        return [
            msg.body for msg in self.__queue.receive_messages(
                AttributeNames=['QueueUrl'],
                VisibilityTimeout=1,
                MaxNumberOfMessages=_rand.randint(1, 10))
        ]

    
    def pull(
        self,
        num_messages: _Optional[int] = None,
        batch_size: int = 10,
        suppress_output: bool = False
    ) -> _Iterator[list[str]]:
        '''
        Iterates through the messages available in the queue \
        in distinct batches.

        :param int | None num_messages: The number of messages to \
            iterate through. If set to ``None``, then the queue \
            is constantly querried for new messages until there \
            are none. Defaults to ``None``.
        :param int batch_size: The maximum number of messages \
            a single batch may contain. Deafults to ``10``.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.

        :note: As soon as a batch of messages is received, all \
            messages within the batch are considered to have \
            been deleted from the queue.
        '''
        if not suppress_output:
            print(f'\nPulling messages from queue "{self.get_name()}".')

        num_messages_fetched = 0

        while num_messages is None or num_messages_fetched < num_messages:

            batch = []
            while len(batch) < batch_size:
                microbatch = self.__queue.receive_messages(
                    AttributeNames=['QueueUrl'],
                    VisibilityTimeout=30,
                    MaxNumberOfMessages=min(
                        batch_size - len(batch),
                        10 if num_messages is None
                        else num_messages-num_messages_fetched,
                        10))
                
                if len(microbatch) == 0:
                    break

                batch += microbatch

            if len(batch) == 0:
                return

            messages = []
            for i in range(0, len(batch), 10):

                resp = self.__queue.delete_messages(Entries=[
                    {'Id': str(i+j),
                    'ReceiptHandle': msg.receipt_handle
                    } for j, msg in enumerate(batch[i:i+10])])

                for j in map(lambda d: int(d['Id']), resp['Successful']):
                    num_messages_fetched += 1
                    messages.append(batch[j].body)

                if not suppress_output and 'Failed' in resp:
                    for msg in map(lambda d: d['Message'], resp['Failed']):
                        print(f'Failed to delete message "{msg}".')

            yield messages


    def __enter__(self) -> 'AWSSQSQueue':
        '''
        Enter the runtime context related to this instance.
        '''
        return super().__enter__()


class AzureStorageQueue(_Queue):
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
        self.open()


    def get_name(self) -> str:
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
        if not suppress_output:
            print(f'\nPushing message "{message}" to queue "{self.get_name()}".')

        try:
            self.__queue.send_message(content=message)
            if not suppress_output:
                print("Message sent successfully!")
            return True
        except Exception as e:
            if not suppress_output:
                print(f"Failed to send message: {e}")
            return False
    

    def peek(self, suppress_output: bool = False) -> list[str]:
        '''
        Returns a list containing at most ten messages \
        currently residing within the queue.

        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.

        :note: This method does not go on to explicitly \
            remove messages from the queue. However, any \
            message returned by this method will have its \
            "receive count" increased, which in turn might \
            result in said message being removed from the \
            queue in case the queue's maximum receive count \
            threshold is exceeded.
        '''
        if not suppress_output:
            print(f'\nPeeking messages from queue "{self.get_name()}".')

        return [
            msg.content for msg in
            self.__queue.peek_messages(
                max_messages=_rand.randint(1, 10))
        ]

    
    def pull(
        self,
        num_messages: _Optional[int] = None,
        batch_size: int = 10,
        suppress_output: bool = False
    ) -> _Iterator[list[str]]:
        '''
        Iterates through the messages available in the queue \
        in distinct batches.

        :param int | None num_messages: The number of messages to \
            iterate through. If set to ``None``, then the queue \
            is constantly querried for new messages until there \
            are none. Defaults to ``None``.
        :param int batch_size: The maximum number of messages \
            a single batch may contain. Deafults to ``10``.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.

        :note: As soon as a batch of messages is received, all \
            messages within the batch are considered to have \
            been deleted from the queue.
        '''
        if not suppress_output:
            print(f'\nPulling messages from queue "{self.get_name()}".')

        for batch in self.__queue.receive_messages(
                messages_per_page=min(batch_size, 10),
                max_messages=num_messages,
                visibility_timeout=30
        ).by_page():
            messages = []
            for msg in batch:
                try:
                    self.__queue.delete_message(msg.id, msg.pop_receipt)
                    messages.append(msg.content)
                except:
                    if not suppress_output:
                        print(f'Failed to delete message "{msg}".')
            yield messages


    def __enter__(self) -> 'AzureStorageQueue':
        '''
        Enter the runtime context related to this instance.
        '''
        return super().__enter__()