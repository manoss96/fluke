<!-- PROJECT BADGES -->
[![Python Version][python-shield]][python-url]
[![MIT License][license-shield]][license-url]
[![Coverage][coverage-shield]][coverage-url]

![Fluke Logo](docs/source/logo.png)

<!-- What is Fluke? -->
## What is Fluke?

Fluke is a Python package that acts as a higher-level API to
cloud services that primarily relate to object storage and messaging.
Fluke manages to hide away much of the complexity that derives from working
with said services, aiding you in completing your tasks fast and hassle-free!
Fluke achieves this by:

* Treating object storage services as traditional file systems,
  unifying the two under a single *File/Dir* API, through which
  you are able to manage your data no matter where they reside,
  be it the local file system or a bucket in the cloud.

* Greatly reducing the intricacies of interacting with message queues
  by viewing them as mere data structures that support three elementary
  operations, that is, push/peek/poll.


<!-- Installation -->
## Installation

You can start using Fluke by installing it via pip.
Note that *fluke* requires Python >= 3.9.

```sh
pip install fluke-api
```


<!-- Usage example -->
## Usage Example

In this example, we will be using Fluke in order to:

1. Poll an Amazon SQS queue every minute for new messages. Each of these messages
   contains the path of a newly uploaded file to an Amazon S3 bucket.
2. Read the messages in order to locate said files and transfer them to a remote server.

First things first, we need to be able to authenticate with both AWS
and the remote server. In order to achieve this, we will be importing
from ``fluke.auth``:

```python
from fluke.auth import AWSAuth, RemoteAuth

# This object will be used to authenticate
# with AWS.
aws_auth = AWSAuth(
    aws_access_key_id="aws_access_key",
    aws_secret_access_key="aws_secret_key")

# This object will be used to authenticate
# with the remote machine.
rmt_auth = RemoteAuth.from_password(
    hostname="host",
    username="user",
    password="password")
```

Next, we just need to import from ``fluke.queues`` and ``fluke.storage``
so that we gain access to any necessary resources in order to perform
the data transfer:

```python
from fluke.queues import AmazonSQSQueue
from fluke.storage import AmazonS3Dir, RemoteDir

with (
    AmazonSQSQueue(auth=aws_auth, queue='queue') as queue,
    AmazonS3Dir(auth=aws_auth, bucket='bucket') as bucket,
    RemoteDir(auth=rmt_auth, path='/home/user/dir', create_if_missing=True) as rmt_dir
):
    for batch in queue.poll(polling_frequency=60):
        for msg in batch:
            bucket.get_file(path=msg).transfer_to(dst=rmt_dir)
```

And that's basically it!

You can learn more about Fluke by visiting the [Fluke Documentation Page][docs-url].


<!-- MARKDOWN LINKS & IMAGES -->
[python-shield]: https://img.shields.io/badge/python-3.9+-blue
[python-url]: https://www.python.org/downloads/release/python-390/
[license-shield]: https://img.shields.io/badge/license-MIT-red
[license-url]: https://github.com/manoss96/fluke/blob/main/LICENSE
[coverage-shield]: https://coveralls.io/repos/github/manoss96/fluke/badge.svg?branch=main&service=github
[coverage-url]: https://coveralls.io/github/manoss96/fluke?branch=main
[docs-url]: https://fluke.readthedocs.io/en/latest/
