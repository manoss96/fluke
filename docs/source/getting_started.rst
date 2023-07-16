.. _getting_started:

*******************
What is Fluke?
*******************

Fluke is a Python package that is primarily to be used as a higher-level API
to cloud services that relate to data storage and transfer. Fluke hides always
much of the complexity of said services, aiding you in achieving your task fast
and hassle-free!

*******************
Installation
*******************

You can start using Fluke by installing it via pip.
Note that *fluke* requires Python >= 3.9.

.. code-block::

    pip install fluke-api


*******************
Usage example
*******************

In this example, we will be using Fluke in order to:

1. Fetch messages from an Amazon SQS queue. Each of these messages contains the path of a newly uploaded file to an Amazon bucket.
2. Use these messages in order to access said files within the bucket.
3. Transfer these files to a remote server.

First things first, we need to be able to authenticate with both AWS
and the remote server. In order to achieve this, we will be importing from
`fluke.auth <documentation/auth.html>`_:

.. code-block:: python

  from fluke.auth import AWSAuth, RemoteAuth

  # This object will be used to authenticate
  # with AWS.
  aws_auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")

  # This object will be used to authenticate
  # with the remote server.
  rmt_auth = RemoteAuth.from_password(
      hostname="host",
      username="user",
      password="password")


Next, we just need to import from `fluke.queues <documentation/queues.html>`_
and `fluke.storage <documentation/storage.html>`_ so that we gain access to any
necessary resources in order to perform the data transfer:

.. code-block:: python

  from fluke.queues import AWSQueue
  from fluke.storage import AWSS3Dir, RemoteDir

  with (
      AWSQueue(auth=aws_auth, queue='queue') as queue,
      AWSS3Dir(auth=aws_auth, bucket='bucket') as bucket,
      RemoteDir(auth=rmt_auth, path='/home/user/dir', create_if_missing=True) as rmt_dir
  ):
      for batch in queue.pull():
        for msg in batch:
            bucket.get_file(path=msg).transfer_to(dst=rmt_dir)

And that's basically it!

You can learn more about Fluke by going through its detailed
`User Guide <user_guide/handling_auth.html>`_, or by visiting
Fluke on `Github <https://github.com/manoss96/fluke>`_
in order to check out the source code itself.