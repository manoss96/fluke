
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


Mocking out cloud services
-------------------------------------

Interacting with various cloud services is a big part of Fluke.
It is only natural that you want to test your code on these
services without having to actually pay for them. Here's what
you can do in order to test each service:

- **Amazon S3 / Amazon SQS**: In order to test these services you can utilize
  the [moto](https://pypi.org/project/moto/) package, which, if you followed
  the steps in "Setting up a development environment", should already be
  installed to your Python virtual environment.

- **Google Cloud Storage**: In order to mock out this service, you'll
  want to run a dummy GCS server locally. The easiest way to do this
  would be to run such a server as a Docker container. In fact, this
  is a necessary step in order to run the tests as all GCS-related
  tests require a mock GCS server running locally. Fluke uses the
  [fsouza/fake-gcs-server](https://hub.docker.com/r/fsouza/fake-gcs-server)
  Docker image. Having installed Docker on your system, you can simply
  run the following command in order to start a dummy GCS server:
  ```
  docker run -d -p 4443:4443 --mount type=bind,\
  src={PATH_TO_FLUKE_REPO}/tests/test_files,\
  dst=/data/bucket/tests/test_files \
  fsouza/fake-gcs-server:1.47.0
  ``````
  This command will pull the ``fsouza-fake-gcs-server`` image from Docker Hub
  and start it as a Docker container in the background, while at the same time
  mapping its port `4443` to your local port `4443`. Furthermore, it mounts the
  repository's ``tests/test_files`` folder into the container, thereby creating
  a dummy bucket called ``bucket`` which will be containing this folder. Before
  you execute the above command, make sure that you replace ``{PATH_TO_FLUKE_REPO}``
  with the actual path of your local copy of the Fluke repository.

- **Azure Blob Storage / Azure Queue Storage**: Unfortunately, there is not a way
  of mocking out Azure services as of yet, though it is being looked into.
  For now, an Azure storage account is required in order to fully test these services.

Running the tests
-------------------------------------
For a pull request to be merged, it is important that it passes all required
tests. In order to ensure that, you can run the tests yourself by executing
the following command:
```
python3 -m unittest discover tests
```
You are also able to specify the testing modules that
you wish to be executed:
```
python3 -m unittest tests.test_storage
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