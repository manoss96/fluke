.. _getting_started:

*******************
What is Fluke?
*******************

Fluke is a Python package that is to be used as a higher-level API to
cloud services that primarily relate to object storage and message queues.
Fluke manages to hide away much of the complexity that derives from working
with said services, aiding you in completing your tasks fast and hassle-free!
Fluke achieves this by:

* Treating object storage services as traditional file systems,
  unifying the two under a single *File/Dir* API, through which
  you are able to manage your data no matter where they reside,
  be it a local/remote file system or a bucket in the cloud.

* Greatly reducing the intricacies of interacting with message queues
  by thinking of them as mere data structures that support three elementary
  operations, that is, push/peek/poll.

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

1. Poll an Amazon SQS queue every minute for new messages. Each of these messages contains the
   path of a newly uploaded file to an Amazon S3 bucket.
2. Use the content of these messages in order to locate and access said files within the bucket.
3. If a file's metadata field ``transfer`` has been set to ``True``, then transfer it to a remote server.

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

  from fluke.queues import AmazonSQSQueue
  from fluke.storage import AmazonS3Dir, RemoteDir

  with (
      AmazonSQSQueue(auth=aws_auth, queue='queue') as queue,
      AmazonS3Dir(auth=aws_auth, bucket='bucket') as bucket,
      RemoteDir(auth=rmt_auth, path='/home/user/dir', create_if_missing=True) as rmt_dir
  ):
    for batch in queue.poll(polling_frequency=60):
        for msg in batch:
            file = bucket.get_file(path=msg)
            file.load_metadata()
            metadata = file.get_metadata()
            if bool(metadata['transfer']):
                file.transfer_to(dst=rmt_dir)

And that's basically it!

You can learn more about Fluke by going through its detailed
`User Guide <user_guide/handling_auth.html>`_, or by visiting
Fluke on `Github <https://github.com/manoss96/fluke>`_
in order to check out the source code itself.