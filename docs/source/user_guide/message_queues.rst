.. _ug_queues:

***********************
Message Queues
***********************

Through `fluke.queues <../documentation/queues.html>`_ Fluke provides
a straightforward API that makes it easy to interact with various message
queue services in the cloud, such as Amazon SQS and Azure Queue Storage.
The simplicity of this API comes from the fact that it is based on just
three basic operations:

1. Push - Sending messages to a queue.
2. Peek - Getting a glimpse of the messages within a queue.
3. Poll - Fetching messages from a queue, thereby deleting them.

Below we'll take a closer look on each one.

===============================
Pushing messages into a queue
===============================

As already mentioned, the *push* operation is used in order
to send messages to the queue. After gaining access to a queue,
you can simply invoke its ``push`` method, providing it with
the string message you wish to send to the queue:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.queues import AWSSQSQueue

  aws_auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AWSSQSQueue(auth=aws_auth, queue='queue') as queue:
    is_delivered = queue.push(message="Hello!")

In response to invoking the ``push`` method, a boolean value
is returned which indicates whether the message has been
successfully delivered to the queue or not.

===============================
Peeking at messages in a queue
===============================

Next up, you might like to take a peek at the messages in a
queue without actually deleting them. This operation can come
quite handy when you want for example to observe the format of the
messages within a queue so that you are able to build a parser in
order to extract the information you are interested in. Whatever its use,
peeking at messages is possible by invoking a queue instance's ``peek``
method, which goes on to return a list that contains up to 10 messages
found within the queue:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.queues import AWSSQSQueue

  aws_auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AWSSQSQueue(auth=aws_auth, queue='queue') as queue:
    messages = queue.peek()

.. warning::

    Please note that even though the ``peek`` method does not explicitly
    delete messages, it can still result in a message being removed from
    the queue, as every time we fetch a message its *receive count* is
    increased by one. Most queues are configured in such a way so that if
    a message is fetched a specific number of times, then it is automatically
    deleted.

===============================
Polling messages from a queue
===============================

Finally, you want to be able to fetch messages from a queue
while at the same time removing them from it entirely. This
is where the *poll* operation comes in. By invoking a queue
instance's ``poll`` method, you essentially get access to an
iterator that is capable of going through the queue's messages
in distinct batches, removing any received messages in the process
of doing so:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.queues import AWSSQSQueue

  aws_auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AWSSQSQueue(auth=aws_auth, queue='queue') as queue:
    for batch in queue.poll(batch_size=10):
        for msg in batch:
            # Process the message...

By default, the ``poll`` method will go on to fetch all of the messages
available in the queue. If you just want to fetch a particular
number of messages, you can set the method's ``num_messages``
parameter accordingly:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.queues import AWSSQSQueue

  aws_auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AWSSQSQueue(auth=aws_auth, queue='queue') as queue:
    for batch in queue.poll(num_messages=100, batch_size=10):
        for msg in batch:
            # Process the message...


-----------------------------
Continuous polling
-----------------------------

When either all messages or a subset of them has been successfully received,
depending on the value of ``num_messages``, the iterator reaches its ending
point and the program carries on. If you wish to keep on polling the queue for
any new messages, then this is possible by setting the ``polling_frequency``
parameter:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.queues import AWSSQSQueue

  aws_auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AWSSQSQueue(auth=aws_auth, queue='queue') as queue:
    for batch in queue.poll(
        num_messages=100,
        batch_size=10,
        polling_frequency=60
    ):
        for msg in batch:
            # Process the message...

By defining a value for this parameter, each time the specified number
of messages has been fetched from the queue, the program will wait for
a total number of seconds equal to the value of ``polling_frequency``,
at which point the queue is polled again until either a total of ``num_messages``
messages are received or the queue is empty. This goes on indefinitely until
either an unhandled exception occurs or the process is explicitly killed.

-----------------------------
Pre- vs post-delivery delete
-----------------------------
