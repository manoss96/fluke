<!-- PROJECT BADGES -->
[![Python Version][python-shield]][python-url]
[![MIT License][license-shield]][license-url]
[![Coverage][coverage-shield]][coverage-url]

![Fluke Logo](docs/source/logo.png)

<!-- What is Fluke? -->
## What is Fluke?

Fluke is a Python package that is primarily to be used as a data transfer tool.
By utilizing Fluke, moving your data between two remote locations can be
done in just a matter of seconds from the comfort of your own machine!


<!-- Installation -->
## Installation

You can start using Fluke by installing it via pip. Note that *fluke* requires Python >= 3.9.

```sh
pip install fluke-api
```


<!-- Usage example -->
## Usage Example

In this example, we are going to transfer an entire directory residing
within a remote machine to the cloud, more specifically, to an Amazon S3 bucket.

First things first, we need to be able to authenticate with both the remote
machine and AWS. In order to achieve this, we will be importing from ``fluke.auth``:

```python
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
```

Next, we just need to import from ``fluke.storage`` so that we
gain access to any necessary resources and perform the data transfer:

```python
from fluke.storage import RemoteDir, AWSS3Dir

with (
    RemoteDir(auth=rmt_auth, path='/home/user/dir') as rmt_dir,
    AWSS3Dir(auth=aws_auth, bucket="bucket", path='dir', create_if_missing=True) as aws_dir
):
    rmt_dir.transfer_to(dst=aws_dir, recursively=True)
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
