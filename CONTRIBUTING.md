
Contributing to Fluke
============================

Whether you wish to report a bug or propose a new feature,
please proceed by [raising an issue][raise-issue].

Setting up a development environment
-------------------------------------
Regardless of whether you want to work on fixing a bug or implementing a new feature,
you should be able to set up a separate development environment just for Fluke. The
fastest way to do this would be the following:

1. Make sure that Python 3.9 or later is installed on your machine.
2. Create a virtual environment just for Fluke (by running ``python3 -m venv env``) and activate it.
3. Either clone or download the Fluke Github repository.
4. Navigate to the repository and run ``pip install -e .[tests]``.
   This results in fluke being installed in development mode,
   which means that any Python script that imports from Fluke
   will be referencing the modules within the cloned/downloaded
   repository. You can read more about "development mode" in
   [Local project installs](https://pip.pypa.io/en/stable/topics/local-project-installs/).


Mocking out remote entities
-------------------------------------

Interacting with various remote services and resources is a big part
of Fluke. It is only natural that you want to test your code on without
having to actually pay for access to these resources. Here's what
you can do in order to test each non-local component:

- **Remote Server**: In order to test access to a remote file system,
  you may simply run an SSH server as a Docker container. Fluke uses
  the [linuxserver/openssh-server](https://hub.docker.com/r/linuxserver/openssh-server)
  Docker image.

- **Amazon S3 / Amazon SQS**: In order to test these services you can utilize
  the [moto](https://pypi.org/project/moto/) package, which, if you followed
  the steps in "Setting up a development environment", should already be
  installed to your Python virtual environment.

- **Google Cloud Storage**: In order to mock out this service, you'll
  want to run a dummy GCS server locally. The easiest way to do this
  would be to run such a server as a Docker container. Fluke uses the
  [fsouza/fake-gcs-server](https://hub.docker.com/r/fsouza/fake-gcs-server)
  Docker image.

- **Azure Blob Storage / Azure Queue Storage**: Unfortunately, there is not a way
  of mocking out Azure services as of yet, though it is being looked into.
  For now, an Azure storage account is required in order to truly test out
  any code that interacts with these services.

Testing
-------------------------------------
For a pull request to be merged, it is important that it passes all required
tests. In this section we'll see how to do this.

### Prerequisites

First things first, Fluke mocks out certain resources by using Docker
containers. More specifically, it mocks out an SSH server so as to test
classes ``fluke.storage.{RemoteFile,RemoteDir}``, as well as a GCS server
for classes ``fluke.storage.{GCPStorageFile,GCPStorageDir}``. Therefore,
in order to run all tests successfully, you need to have these services
running as Docker containers on your system. Assuming that you have Docker
installed, you can achieve this by executing the following commands:

- **SSH Server**:
  ```
  docker run -d \
  -p 2222:2222 \
  --env PUID=$(id -u $(whoami)) \
  --env PGID=$(id -g $(whoami)) \
  --env PASSWORD_ACCESS=true \
  --env USER_NAME=test \
  --env USER_PASSWORD=test \
  --mount type=bind,\
  src={PATH_TO_FLUKE_REPO}/tests/test_files,\
  dst=/tests/test_files \
  linuxserver/openssh-server:amd64-version-9.3_p2-r0
  ``````
  This command will pull the ``linuxserver/openssh-server`` image from Docker Hub
  and start it as a Docker container in the background, while at the same time
  mapping its port `2222` to your local port `2222` and passing all required
  environmental variables. Furthermore, it mounts the Fluke repository's ``tests/test_files``
  folder into the container. Before you execute the above command, make sure that you replace
  ``{PATH_TO_FLUKE_REPO}`` with the actual path of your local copy of the Fluke repository.

- **GCS Server**:
  ```
  docker run -d \
  -p 4443:4443 \
  --mount type=bind,\
  src={PATH_TO_FLUKE_REPO}/tests/test_files,\
  dst=/data/bucket/tests/test_files \
  fsouza/fake-gcs-server:1.47.0
  ``````
  This command will pull the ``fsouza/fake-gcs-server`` image from Docker Hub
  and start it as a Docker container in the background, while at the same time
  mapping its port `4443` to your local port `4443`, as well as mounting the
  Fluke repository's ``tests/test_files`` folder into the container, thereby
  creating a dummy bucket called ``bucket`` which will be containing this folder.
  Before you execute the above command, make sure that you replace ``{PATH_TO_FLUKE_REPO}``
  with the actual path of your local copy of the Fluke repository.


### Running the tests

After you have all necessary Docker containers up and running,
you may run the tests by executing the following command:
```
python3 -m unittest discover tests
```
You are also able to specify the test module/class/method
you wish to be executed:
```
python3 -m unittest tests.test_storage
python3 -m unittest tests.test_storage.TestRemoteDir
python3 -m unittest tests.test_storage.TestRemoteDir.test_constructor
```
Finally, you can get the test coverage by executing the following commands:
```
python -m coverage run -m unittest discover tests
python -m coverage html
```
This will produce an ``htmlcov/index.html`` file which will contain
various information regarding the test coverage.

<!-- MARKDOWN LINKS & IMAGES -->
[raise-issue]: https://github.com/manoss96/fluke/issues/new