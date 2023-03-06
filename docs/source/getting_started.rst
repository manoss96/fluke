.. _getting_started:

*******************
What is Fluke?
*******************

Fluke is a Python package that is primarily to be used as a data transfer tool.
By utilizing Fluke, moving your data between two remote locations can be
done in just a matter of seconds from the comfort of your own machine!

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


In this example, we are going to transfer an entire directory residing
within a remote machine to the cloud, more specifically, to an Amazon S3 bucket.

First things first, we need to be able to authenticate with both the remote
machine and AWS. In order to achieve this, we will be importing from
`fluke.auth <documentation/auth.html>`_:

.. code-block:: python

  from fluke.auth import RemoteAuth, AWSAuth

  # This object will be used to authenticate
  # with the remote machine.
  rmt_auth = RemoteAuth.from_password(
      hostname="host",
      username="user",
      password="password")

  # This object will be used to authenticate
  # with AWS.
  aws_auth = AWSAuth(
      aws_access_key_id="aws_access_key",
      aws_secret_access_key="aws_secret_key")


Next, we just need to import from `fluke.storage <documentation/storage.html>`_
so that we gain access to any necessary resources and perform the data transfer:

.. code-block:: python

  from fluke.storage import RemoteDir, AWSS3Dir

  with (
      RemoteDir(auth=rmt_auth, path='/home/user/dir') as rmt_dir,
      AWSS3Dir(auth=aws_auth, bucket="bucket", path='dir', create_if_missing=True) as aws_dir
  ):
      rmt_dir.transfer_to(dst=aws_dir, recursively=True)

And that's basically it!

You can learn more about Fluke by going through its detailed
`User Guide <user_guide/handling_auth.html>`_, or by visiting
Fluke on `Github <https://github.com/manoss96/fluke>`_
in order to check out the source code itself.