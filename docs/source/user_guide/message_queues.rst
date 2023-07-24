.. _ug_queues:

***********************
Message Queues
***********************

Through the module `fluke.queues <../documentation/queues.html>`_ Fluke
provides a straightforward API that makes it easy to interact with various
message queue services in the cloud, such as Amazon SQS and Azure Queue
Storage. The simplicity of this API comes from the fact that it is based
on just three basic operations:

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
  from fluke.queues import AmazonSQSQueue

  auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AmazonSQSQueue(auth=auth, queue='queue') as queue:
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
  from fluke.queues import AmazonSQSQueue

  auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AmazonSQSQueue(auth=auth, queue='queue') as queue:
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
  from fluke.queues import AmazonSQSQueue

  auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AmazonSQSQueue(auth=auth, queue='queue') as queue:
    for batch in queue.poll(batch_size=10):
        for msg in batch:
            # Process the message...

By default, the ``poll`` method will go on to fetch all of the messages
available in the queue. If you just want to fetch a particular
number of messages, you can set the method's ``num_messages``
parameter accordingly:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.queues import AmazonSQSQueue

  auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AmazonSQSQueue(auth=auth, queue='queue') as queue:
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
  from fluke.queues import AmazonSQSQueue

  auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AmazonSQSQueue(auth=auth, queue='queue') as queue:
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

Whenever a batch of messages is delivered, the messages within
the batch have not yet actually been deleted from the queue,
and will only be deleted just before the next batch of messages arrives,
after all messages within the current batch have already been processed.
This is the queue's default behaviour when polling messages, as defined
by the default value of parameter ``pre_delivery_delete``, and it guarantees
that no batch of messages will be lost in case something goes wrong during
their processing:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.queues import AmazonSQSQueue

  auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AmazonSQSQueue(auth=auth, queue='queue') as queue:
    for batch in queue.poll():
        # Although an exception is raised, no messages will be lost
        # as, at this point, the messages within the batch have yet
        # to be deleted.
        raise Exception()

Nevertheless, if something goes wrong after we have already processed
one or more messages within the batch, then there is always the danger
of reprocessing the exact same messages as they may be included in any
one of the subsequent batches:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.queues import AmazonSQSQueue

  auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AmazonSQSQueue(auth=auth, queue='queue') as queue:
    for batch in queue.poll(num_messages=2, batch_size=2):
        for i, msg in enumerate(batch):
            if i == 0:
                # Process message...
            else:
                raise Exception()

In the example above, the first message of the batch will be processed
as expected. However, during the processing of the second message, an
exception will be thrown, causing the program to exit. This means that
despite the second message being the only one that was not processed,
the queue still contains both messages, as the next batch of messages
never arrived.

If you want to avoid this, you can set ``pre_delivery_delete`` to ``True``.
This results in any batch of messages being removed from the queue before
they are actually delivered to you:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.queues import AmazonSQSQueue

  auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  with AmazonSQSQueue(auth=auth, queue='queue') as queue:
    for batch in queue.poll(pre_delivery_delete=True):
        # At this point, all messages within the batch
        # have already been removed from the queue.

As a general rule of thumb, set ``pre_delivery_delete`` to ``False``
if you don't mind processing the same message twice and are more concerned
in losing a message without processing it first, whereas set ``pre_delivery_delete``
to ``True`` if you cannot afford to process the same message more than once.

.. warning::

    Pre- and post-delivery are concepts related to Fluke and do not
    apply to the underlying message queue services. Note that you
    should always enforce your own rules in order to check for duplicate
    messages if this is a matter of concern to you, as such messages
    can still arrive regardless of the value of ``pre_delivery_delete``.
    For instance, the default *standard* queues offered by Amazon SQS
    support *at-least-once delivery* which dictates that any messages
    pushed to the queue will be delivered to you at least once, but there
    is always the chance that a message be re-delivered due to the distributed
    nature of the queues. In order to avoid any suprises, be sure to read the
    specifications of the type of queue you are working with and take any
    necessary precautions, no matter whether you are accessing said queue
    through its standard API or through the API provided by Fluke.

