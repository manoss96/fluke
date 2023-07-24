import time as _time
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

    :param str name: The name of the queue.
    '''

    def __init__(self, name: str) -> None:
        '''
        An abstract class which serves as the \
        base class for all queue classes.

        :param str name: The name of the queue.
        '''
        self.__name = name
        self.open()


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


    def get_name(self) -> str:
        '''
        Returns the name of the queue to which \
        a connection has been established.
        '''
        return self.__name


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
    def count(self) -> int:
        '''
        Returns the total number of messages that \
        are residing within the queue at the time \
        of the request.
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
            messages returned by this method will have their \
            "receive count" increased, which in turn might \
            result in said messages being removed from the \
            queue in case the queue's maximum receive count \
            threshold is exceeded.
        '''
        pass


    @_absmethod
    def poll(
        self,
        num_messages: _Optional[int] = None,
        batch_size: int = 10,
        polling_frequency: _Optional[int] = None,
        pre_delivery_delete: bool = False,
        suppress_output: bool = False
    ) -> _Iterator[list[str]]:
        '''
        Iterates through the messages available in the queue \
        in distinct batches, deleting them in the process of \
        doing so.

        :param int | None num_messages: The number of messages to \
            iterate through. If set to ``None``, then the queue \
            is constantly querried for new messages until there \
            are none left. Defaults to ``None``.
        :param int batch_size: The maximum number of messages \
            a single batch may contain. Deafults to ``10``.
        :param int | None polling_frequency: If set to an integer \
            value, then the queue is going to be being polled at \
            regular time intervals equal to said value in seconds. \
            If set to ``None``, then the queue is only polled once. \
            Defaults to ``None``.
        :param bool pre_delivery_delete: Indicates whether a \
            batch of messages is to be removed from the queue \
            before or after its delivery. If set to ``True``, \
            then it is guaranteed that any delivered messages will \
            have already been removed from the queue, thus reducing \
            the likelihood of fetching the same message twice. If set \
            to ``False``, then any delivered messages are only deleted \
            just before the delivery of the next batch of messages, \
            thus preventing from any messages being lost in case \
            an error occurs during their processing. Defaults \
            to ``False``.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        pass


    @_absmethod
    def clear(self, suppress_output: bool = False) -> None:
        '''
        Empties the queue by deleting all messages.

        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        pass


class AmazonSQSQueue(_Queue):
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
        self.__queue = None
        super().__init__(name=queue)
    

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

        print(f"\nEstablishing connection to '{self.get_name()}' Amazon SQS queue...")
        self.__queue = _boto3.resource(
            service_name='sqs', **creds
        ).get_queue_by_name(QueueName=self.get_name())
        print("Connection established.")


    def close(self):
        '''
        Closes the HTTP connection to \
        the Amazon SQS queue.
        '''
        if self.__queue is not None:
            self.__queue.meta.client.close()
            self.__queue = None


    def count(self) -> int:
        '''
        Returns the total number of messages that \
        are residing within the queue at the time \
        of the request.
        '''
        self.__queue.reload()
        return (
            int(self.__queue.attributes['ApproximateNumberOfMessages']) +
            int(self.__queue.attributes['ApproximateNumberOfMessagesNotVisible'])
        )


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
            print(f'\nPushing message "{message}" into queue "{self.get_name()}".')
        
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
            messages returned by this method will have their \
            "receive count" increased, which in turn might \
            result in said messages being removed from the \
            queue in case the queue's maximum receive count \
            threshold is exceeded.
        '''
        if not suppress_output:
            print(f'\nPeeking messages in queue "{self.get_name()}".')

        return [
            msg.body for msg in self.__queue.receive_messages(
                AttributeNames=['QueueUrl'],
                VisibilityTimeout=1,
                MaxNumberOfMessages=_rand.randint(1, 10))
        ]

    
    def poll(
        self,
        num_messages: _Optional[int] = None,
        batch_size: int = 10,
        polling_frequency: _Optional[int] = None,
        pre_delivery_delete: bool = False,
        suppress_output: bool = False
    ) -> _Iterator[list[str]]:
        '''
        Iterates through the messages available in the queue \
        in distinct batches, deleting them in the process of \
        doing so.

        :param int | None num_messages: The number of messages to \
            iterate through. If set to ``None``, then the queue \
            is constantly querried for new messages until there \
            are none left. Defaults to ``None``.
        :param int batch_size: The maximum number of messages \
            a single batch may contain. Deafults to ``10``.
        :param int | None polling_frequency: If set to an integer \
            value, then the queue is going to be being polled at \
            regular time intervals equal to said value in seconds. \
            If set to ``None``, then the queue is only polled once. \
            Defaults to ``None``.
        :param bool pre_delivery_delete: Indicates whether a \
            batch of messages is to be removed from the queue \
            before or after its delivery. If set to ``True``, \
            then it is guaranteed that any delivered messages will \
            have already been removed from the queue, thus reducing \
            the likelihood of fetching the same message twice. If set \
            to ``False``, then any delivered messages are only deleted \
            just before the delivery of the next batch of messages, \
            thus preventing from any messages being lost in case \
            an error occurs during their processing. Defaults \
            to ``False``.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        while True:

            if not suppress_output:
                print(f'\nPolling messages from queue "{self.get_name()}".')

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
                    num_messages_fetched += len(microbatch)

                    if num_messages_fetched == num_messages:
                        break

                if len(batch) == 0:
                    break
                
                entries, messages = [], []

                for i in range(0, len(batch), 10):
                    for j, msg in enumerate(batch[i:i+10]):
                        entries.append({'Id': str(i+j), 'ReceiptHandle': msg.receipt_handle})
                        messages.append(msg.body)

                if pre_delivery_delete:
                    # First remove messages from queue.
                    resp = self.__queue.delete_messages(Entries=entries)
                    if not suppress_output and 'Failed' in resp:
                        for msg in map(lambda d: d['Message'], resp['Failed']):
                            print(f'Failed to delete message "{msg}".')
                    # Filter out any messages that failed to be removed.
                    deleted_messages = []
                    for j in map(lambda d: int(d['Id']), resp['Successful']):
                        deleted_messages.append(messages[j])
                    # Only deliver successfully deleted messages.
                    yield deleted_messages
                    num_messages_fetched += len(deleted_messages)
                else:
                    # First deliver messages.
                    yield messages
                    num_messages_fetched += len(messages)
                    # Then attempt to remove them from queue.
                    resp = self.__queue.delete_messages(Entries=entries)
                    if not suppress_output and 'Failed' in resp:
                        for msg in map(lambda d: d['Message'], resp['Failed']):
                            print(f'Failed to delete message "{msg}".')

            if polling_frequency is None:
                break
            else:
                _time.sleep(polling_frequency)


    def clear(self, suppress_output: bool = False) -> None:
        '''
        Empties the queue by deleting all messages.

        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        if not suppress_output:
            print(f"Deleting all messages from queue '{self.get_name()}'.")
        self.__queue.purge()


    def __enter__(self) -> 'AmazonSQSQueue':
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
        self.__queue = None
        super().__init__(name=queue)
    

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

        print(f"\nEstablishing connection to '{self.get_name()}' Azure Queue Storage queue...")
        if 'conn_string' in credentials:
            self.__queue = _QueueClient.from_connection_string(
                conn_str=credentials['conn_string'],
                queue_name=self.get_name())
        else:
            self.__queue = _QueueClient(
                account_url=credentials.pop('account_url'),
                queue_name=self.get_name(),
                credential=_CSC(**credentials))
        print("Connection established.")


    def close(self):
        '''
        Closes the HTTP connection to the \
        Azure Queue Storage queue.
        '''
        if self.__queue is not None:
            self.__queue.close()
            self.__queue = None


    def count(self) -> int:
        '''
        Returns the total number of messages that \
        are residing within the queue at the time \
        of the request.
        '''
        return (self.__queue
            .get_queue_properties()
            .approximate_message_count)


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
            print(f'\nPushing message "{message}" into queue "{self.get_name()}".')

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
            messages returned by this method will have their \
            "receive count" increased, which in turn might \
            result in said messages being removed from the \
            queue in case the queue's maximum receive count \
            threshold is exceeded.
        '''
        if not suppress_output:
            print(f'\nPeeking messages in queue "{self.get_name()}".')

        return [
            msg.content for msg in self.__queue.peek_messages(
                max_messages=_rand.randint(1, 10))
        ]

    
    def poll(
        self,
        num_messages: _Optional[int] = None,
        batch_size: int = 10,
        polling_frequency: _Optional[int] = None,
        pre_delivery_delete: bool = False,
        suppress_output: bool = False
    ) -> _Iterator[list[str]]:
        '''
        Iterates through the messages available in the queue \
        in distinct batches, deleting them in the process of \
        doing so.

        :param int | None num_messages: The number of messages to \
            iterate through. If set to ``None``, then the queue \
            is constantly querried for new messages until there \
            are none left. Defaults to ``None``.
        :param int batch_size: The maximum number of messages \
            a single batch may contain. Deafults to ``10``.
        :param int | None polling_frequency: If set to an integer \
            value, then the queue is going to be being polled at \
            regular time intervals equal to said value in seconds. \
            If set to ``None``, then the queue is only polled once. \
            Defaults to ``None``.
        :param bool pre_delivery_delete: Indicates whether a \
            batch of messages is to be removed from the queue \
            before or after its delivery. If set to ``True``, \
            then it is guaranteed that any delivered messages will \
            have already been removed from the queue, thus reducing \
            the likelihood of fetching the same message twice. If set \
            to ``False``, then any delivered messages are only deleted \
            just before the delivery of the next batch of messages, \
            thus preventing from any messages being lost in case \
            an error occurs during their processing. Defaults \
            to ``False``.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        while True:

            if not suppress_output:
                print(f'\nPolling messages from queue "{self.get_name()}".')

            num_messages_fetched = 0

            while num_messages is None or num_messages_fetched < num_messages:

                no_messages_left = True

                for batch in self.__queue.receive_messages(
                        messages_per_page=min(
                            batch_size,
                            10 if num_messages is None
                            else num_messages-num_messages_fetched,
                            10),
                        max_messages=
                            None if num_messages is None
                            else num_messages - num_messages_fetched,
                        visibility_timeout=30
                ).by_page():
                    if pre_delivery_delete:
                        # First attempt to remove messages from queue.
                        messages = []
                        for msg in batch:
                            try:
                                self.__queue.delete_message(msg.id, msg.pop_receipt)
                                messages.append(msg.content)
                            except:
                                if not suppress_output:
                                    print(f'Failed to delete message "{msg}".')
                        # Then deliver messages.
                        yield messages
                        num_messages_fetched += len(messages)
                    else:
                        # First deliver messages.
                        messages = list(batch)
                        yield [msg.content for msg in messages]
                        num_messages_fetched += len(messages)
                        # Then attempt to remove messages from queue.
                        for msg in messages:
                            try:
                                self.__queue.delete_message(msg.id, msg.pop_receipt)
                            except:
                                if not suppress_output:
                                    print(f'Failed to delete message "{msg}".')
                    # Indicate there are still messages left.
                    no_messages_left = False

                if no_messages_left:
                    break

            if polling_frequency is None:
                break
            else:
                _time.sleep(polling_frequency)



    def clear(self, suppress_output: bool = False) -> None:
        '''
        Empties the queue by deleting all messages.

        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        if not suppress_output:
            print(f"Deleting all messages from queue '{self.get_name()}'.")
        self.__queue.clear_messages()


    def __enter__(self) -> 'AzureStorageQueue':
        '''
        Enter the runtime context related to this instance.
        '''
        return super().__enter__()