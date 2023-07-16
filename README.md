<!-- PROJECT BADGES -->
[![Python Version][python-shield]][python-url]
[![MIT License][license-shield]][license-url]
[![Coverage][coverage-shield]][coverage-url]

![Fluke Logo](docs/source/logo.png)

<!-- What is Fluke? -->
## What is Fluke?

Fluke is a Python package that is primarily to be used as a higher-level API
to cloud services that relate to data storage and transfer. Fluke hides always
much of the complexity of said services, aiding you in achieving your task fast
and hassle-free!


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

1. Fetch messages from an Amazon SQS queue. Each of these messages contains the path of a newly uploaded file to an Amazon bucket.
2. Use these messages in order to access said files within the bucket.
3. Transfer these files to a remote server.

First things first, we need to be able to authenticate with both AWS
and the remote server. In order to achieve this, we will be importing from ``fluke.auth``:

```python
from fluke.auth import RemoteAuth, AWSAuth

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
