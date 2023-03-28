
Contributing to Fluke
============================

No matter whether you wish to report a bug or propose a new feature,
please raise a new issue by visiting the [Issues Page][issues-page].

Setting up a development environment
-------------------------------------
Regardless of whether you want to work on fixing a bug or implementing a new feature,
you should be able to set up a separate development environment just for Fluke. The
fastest way to do this would be the following:

1. Make sure that Python 3.9 or later is installed on your machine.
2. Either clone or download the "fluke" repository.
3. Create a virtual environment just for Fluke (by running ``python3 -m venv env``)
4. Install all dependencies (you can find a ``requirements.txt`` file in ``docs/source/``)

At this point you may want to create a simple script, e.g. ``main.py``
and place it directly within the cloned repo. In this script, you are
free to import from "fluke" so that you can experiment by executing
various bits of code. Just make sure that, when running your script,
your working directory is the "fluke" repo, and you are good to go!

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
[issues-page]: https://github.com/manoss96/fluke/issues