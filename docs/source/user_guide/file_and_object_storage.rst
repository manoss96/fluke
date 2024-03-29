.. _ug_storage:

***********************
File & Object Storage
***********************

Fluke makes it easy to interact with your data no matter
where they reside! In this section, we will look at all
aspects of working with data through Fluke.

========================================
Accessing data
========================================

Fluke treats object storage in the cloud as if it were an
ordinary file system where files are organized in directories,
which is the type of storage most people are familiar with.
Therefore, Fluke offers two distinct, yet interconnected, APIs
in order to interact with data:

#. **File API**: Used for handling either individual files in a file system,
   be it local or remote, or objects in the cloud. This API includes:
   
     * `AmazonS3File <../documentation/storage.html#fluke.storage.AmazonS3File>`_
     * `AzureBlobFile <../documentation/storage.html#fluke.storage.AzureBlobFile>`_
     * `GCPStorageFile <../documentation/storage.html#fluke.storage.GCPStorageFile>`_
     * `LocalFile <../documentation/storage.html#fluke.storage.LocalFile>`_
     * `RemoteFile <../documentation/storage.html#fluke.storage.RemoteFile>`_

#. **Dir API**: Used for handling entire directories of files/objects. This API includes:

     * `AmazonS3Dir <../documentation/storage.html#fluke.storage.AmazonS3Dir>`_
     * `AzureBlobDir <../documentation/storage.html#fluke.storage.AzureBlobDir>`_
     * `GCPStorageDir <../documentation/storage.html#fluke.storage.GCPStorageDir>`_
     * `LocalDir <../documentation/storage.html#fluke.storage.LocalDir>`_
     * `RemoteDir <../documentation/storage.html#fluke.storage.RemoteDir>`_

----------------------------------------
Accessing local entities
----------------------------------------

No matter the type of entity with which you wish to interact, you need to know
its path within the storage system in which it resides. Regarding local entities,
this is actually the only piece of information required in order to access them:

.. code-block:: python

  from fluke.storage import LocalFile

  file = LocalFile(path='/home/user/path/to/file.txt')

----------------------------------------
Accessing remote entities
----------------------------------------

For any entity that is not local to our machine's file system, we usually
need a bit more information. This is typically an *Auth* instance used
for authenticating with the remote host in order to gain access to its resources
(see `Authentication <authentication.html>`_), as well as some
more information regarding an entity's exact location. For example, in the
case of a directory which resides within an Amazon S3 bucket, we also need
the name of said bucket:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.storage import AmazonS3Dir

  # Create an "AWSAuth" instance.
  auth = AWSAuth(**credentials)

  # Gain access to an Amazon S3 directory.
  s3_dir = AmazonS3Dir(
    auth=auth,
    bucket='bucket_name',
    path='path/within/amazon/s3/bucket/dir')

  # Interact with "s3_dir".
  ...

  # Close any open connections.
  s3_dir.close()

Notice how at the end we invoked the instance's ``close`` method? This is
done in order to prevent any connection leaks from occurring, and is in fact
required when interacting with any non-local entity. Alternatively, you can
avoid having to invoke this method by instantiating your object through the
class's context manager:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.storage import AmazonS3Dir

  # Create an "AWSAuth" instance.
  auth = AWSAuth(**credentials)

  # Gain access to an S3 directory through a context manager.
  with AmazonS3Dir(
    auth=auth,
    bucket='bucket_name',
    path='path/within/amazon/s3/bucket/dir'
  ) as s3_dir:

    # Interact with "s3_dir".
    ...

.. _intermediate-access:

----------------------------------------
Intermediate access
----------------------------------------

After having gained access to a directory, you may also
access its files and/or subdirectories as individual
*File*/*Dir* instances through the following methods:

* ``get_file(path: str) -> File``
* ``get_subdir(path: str) -> Dir``

Consider the following example in which we use a directory's
``get_file`` method in order to access a file which resides
directly within it, namely ``file.txt``:

.. code-block:: python

  from fluke.storage import LocalDir, LocalFile

  # Access directory.
  local_dir: LocalDir = LocalDir(path='dir')

  # Access file through directory.
  local_file: LocalFile = local_dir.get_file('file.txt')

This is almost equivalent to:

.. code-block:: python

  from fluke.storage import LocalFile

  # Access file directly.
  local_file: LocalFile = LocalFile(path='dir/file.txt')

The only difference between these two methods of accessing a file
is that when doing so through a directory, this results in the two
sharing a bunch of resources with each other, them being:

* The file metadata
* The underlying connection (in case of remote entities)
* The cache storage (in case of remote entities)

Regarding remote entities, seeing that a file shares
the same connection with the directory from which it came,
you should restrain from invoking the file's ``close`` method,
as this would also impact your ability to interact with the
directory:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.storage import AmazonS3Dir, AmazonS3File

  # Create an "AWSAuth" instance.
  auth = AWSAuth(**credentials)

  # Access directory.
  s3_dir = AmazonS3Dir(auth=auth, bucket='bucket_name', path='dir')

  # Access file through directory.
  s3_file: AmazonS3File = s3_dir.get_file('file.txt')

  # Close connection through file.
  s3_file.close()

  # This would now cause an exception as
  # the underlying connection has been closed.
  dir_size = s3_dir.get_size()

Therefore, by utilizing intermediate access, you are able to
interact with many remote entities without having to open
multiple connections:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.storage import AmazonS3Dir, AmazonS3File

  # Create an "AWSAuth" instance.
  auth = AWSAuth(**credentials)

  # Gain access to the entire bucket.
  with AmazonS3Dir(auth=auth, bucket='bucket_name') as bucket:

    # Interact with its files.
    s3_file: AmazonS3File = bucket.get_file('file.txt')
    file_size = s3_file.get_size()
    
    # Interact with its subdirectories.
    s3_dir: AmazonS3Dir = bucket.get_subdir('dir')
    s3_dir.ls()


========================================
Interacting with data
========================================

After having gained access to an entity, you are then
free to interact with it in various ways, like for example
fetch its size in bytes:

.. code-block:: python

  from fluke.storage import LocalFile

  file = LocalFile(path='/home/user/path/to/file.txt')
  size = file.get_size()

or even read its entire contents as raw bytes and store them in memory:

.. code-block:: python

  from fluke.storage import LocalFile

  file = LocalFile(path='/home/user/path/to/file.txt')
  file_bytes = file.read()

There is even the possibility of reading files partially. This is especially
useful in cases where a file is too large to keep in memory, however, you are
aware of the position of the information that you seek within the file. Partially
reading a file is achieved through the *File* API's ``read_range`` method.

.. code-block:: python

  from fluke.storage import LocalFile

  file = LocalFile(path='/home/user/path/to/file.txt')

  # Read a chunk of bytes containing only 
  # the first Kilobyte of data.
  chunk = file.read_range(start=0, end=1024)

  # Extract info from said chunk...
  text = chunk.decode('utf-8')
  ...

Finally, even if you don't know the exact position of the information
you are after, you can always read a large file in smaller chunks,
which can be examined on the spot:

.. code-block:: python

  from fluke.storage import LocalFile

  file = LocalFile(path='/home/user/path/to/file.txt')

  # Go through the file in 1MB chunks...
  for chunk in file.read_chunks(chunk_size=1024*1024)
    # Decode bytes to text.
    text = chunk.decode('utf-8')
    # Stop if you found what you were looking for.
    if valuable_info in text:
        break

  # Work on "text"...
  ...


Since all *File* API methods are pretty self-explanatory, for the rest
of this section we will focus on the *Dir* API, and more specifically,
on parameter ``recursively``, as its value directly determines the result
of most of its methods. In essence, this parameter dictates whether a
directory is going to be traversed recursively or not, or in other words,
whether we are going to take into consideration its top-level files only,
or all its files, no matter whether they reside directly within the directory
or within one of its subdirectories.

Consider for example the following directory:

.. code-block::

  dir/
  |___file1.txt
  |___subdir/
      |___file2.txt
      |___file3.txt

Now consider the following code and try to guess what will be
printed onto the console after executing it:

.. code-block:: python

  from fluke.storage import LocalDir

  local_dir = LocalDir(path='dir')

  print(f"Ordinary count: {local_dir.count()}")
  print(f"Recursive count: {local_dir.count(recursively=True)}")

This is the output we get after executing the above code block:

.. code-block::

  Ordinary count: 2
  Recursive count: 3

That is because when ``recursively`` has been set to its default value,
namely the value ``False``, the ``count`` method will only consider those
entities that reside within the directory's top-level, which in our case
are the file ``file1.txt`` and the directory ``subdir``. On the other hand, when
``recursively`` has been set to ``True``, all existing subdirectories
will be traversed as well for any files they may contain, resulting in
counting three separate entities within the context of our example, namely
``file1.txt``, ``file2.txt`` and ``file3.txt``. 

Note that whenever ``recursively`` is set to ``True``,
subdirectories are not considered to be additional entities,
and are only searched for any files that may reside within them.
If, for example, ``subdir`` were empty, then
``local_dir.count(recursively=True)``
would merely return the value ``1``.


========================================
Transfering data
========================================

Being able to move data between various locations is arguably
Fluke's predominant feature, and it is rendered possible
through the use of the ``transfer_to`` method, which is part of
both APIs *File* and *Dir*. Below is a complete example in which
we transfer the contents of a virtual directory residing within an
Amazon S3 bucket to a virtual directory of an Azure blob container,
all in just a few lines of code:

.. code-block:: python

  from fluke.auth import AWSAuth, AzureAuth
  from fluke.storage import AmazonS3Dir, AzureBlobDir

  # This object will be used to authenticate with AWS.
  aws_auth = AWSAuth(**aws_credentials)

  # This object will be used to authenticate with Azure.
  azr_auth = AzureAuth.from_service_principal(**azr_credentials)

  with (
      AmazonS3Dir(auth=aws_auth, bucket='bucket', path='dir') as s3_dir,
      AzureBlobDir(auth=azr_auth, container='container', path='dir') as azr_dir
  ):
      s3_dir.transfer_to(dst=azr_dir, recursively=True)

Unless you set parameter ``suppress_output`` to ``True``, Fluke will go
on to print the progress of the transfer onto the console:

.. image:: transfer_files_without_chunk_size.jpg
  :alt: Data transfer progress (without chunk_size)

Furthermore, if you set parameter ``chunk_size``, the method will
produce an even more verbose output, as files will be transfered
in distinct chunks instead of all at once:

.. image:: transfer_files_with_chunk_size.jpg
  :alt: Data transfer progress (with chunk_size)


Finally, it is important to note that if anything goes wrong during
the transfer of one or more entities, then an appropriate message
will be displayed after the method is done with being executed:

.. image:: transfer_files_with_error.jpg
  :width: 700
  :alt: Data transfer error

In the above case, this error could be eliminated by setting the
``transfer_to`` method's ``overwrite`` parameter to ``True``.

========================================
Managing metadata
========================================

You might have noticed that both *File* and *Dir* APIs offer a ``get_metadata/set_metadata``
method pair. Although these methods serve no purpose when transfering data to either the local
or a remote file system, as standard file systems have no way of associating files with custom
metadata, they are quite important when uploading files to the cloud in case you wish to assign
any metadata to them.

..  _assigning-metadata-to-files:

--------------------------------------------
Assigning metadata to files
--------------------------------------------

Consider the following example in which we set parameter ``include_metadata``
to ``True`` while transfering a file to Amazon S3, after we have first assigned
some metadata to it through the ``set_metadata`` method:


.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.storage import LocalFile, AmazonS3Dir

  # Instantiate a local file and assign some metadata to it.
  file = LocalFile(path='/home/user/path/to/file.txt')
  file.set_metadata({'id': '12345', 'type': 'txt'})

  # Transfer file to Amazon S3 along with its metadata.
  with AmazonS3Dir(auth=AWSAuth(**aws_credentials), bucket='bucket', path='dir') as s3_dir:
      file.transfer_to(dst=s3_dir, include_metadata=True)

Along with *file.txt* being uploaded to Amazon S3, any metadata that
were defined via the ``set_metadata`` method will be associated with it.
In fact, we can easily confirm this by executing the following code:

.. code-block:: python

  print(s3_dir.get_metadata('file.txt'))

which results in the following output being printed onto the console:

.. code-block::

  {'id': '12345', 'type': 'txt'}

Finally, note that when accessing a file through a directory,
any modification made to its metadata through either API,
will be reflected in the other. Consider the following example:

.. code-block:: python

  from fluke.storage import LocalDir, LocalFile

  # Access directory.
  local_dir: LocalDir = LocalDir(path='dir')

  # Access file through directory.
  file_name = 'file.txt'
  local_file: LocalFile = local_dir.get_file(file_name)

  # Set file metadata through the "File" API..
  local_file.set_metadata(metadata={'id': '12345', 'type': 'txt'})

  # Get file metadata through the "Dir" API.
  print(local_dir.get_metadata(file_path=file_name))

Executing the above code produces the following output:

.. code-block::

  {'id': '12345', 'type': 'txt'}

Even though ``local_dir.set_metadata`` was never invoked,
``local_dir.get_metadata(file_path=file_name)`` returns the
metadata dictionary that was set via ``local_file.set_metadata``.
Naturally, the reverse is also possible:

.. code-block:: python

  from fluke.storage import LocalDir, LocalFile

  # Access directory.
  local_dir: LocalDir = LocalDir(path='dir')

  # Access file through directory.
  file_name = 'file.txt'
  local_file: LocalFile = local_dir.get_file(file_name)

  # Set file metadata through the "Dir" API..
  local_dir.set_metadata(file_path=file_name, metadata={'id': '12345', 'type': 'txt'})

  # Get file metadata through the "File" API.
  print(local_file.get_metadata())

After being executed, this produces the same output as before:

.. code-block::

  {'id': '12345', 'type': 'txt'}

--------------------------------------------
Loading metadata
--------------------------------------------

It would make sense to assume that if we were to access *file.txt*
on Amazon S3 by using Fluke's *File* API, we could then invoke
``get_metadata`` so that we fetch the metadata that we previously
assigned to it. Let's do just that and see what happens:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.storage import AmazonS3File

  # Gain access to 'file.txt' on Amazon S3 and print its metadata.
  with AmazonS3File(
    auth=AWSAuth(**aws_credentials),
    bucket='bucket',
    path='dir/file.txt'
  ) as aws_obj:
      print(aws_obj.get_metadata())

By executing the above code, we get the following output:

.. code-block::

  {}

That's strange. Shouldn't we see a dictionary containing the metadata we just assigned to
the object while transfering it to Amazon S3? Actually, the answer is no, and the reason
for this is quite simple: both methods ``get_metadata`` and ``set_metadata`` only interact
with the instance through which they are being invoked, which is merely an object stored
within our machine's local RAM. This consequently means that if we were to invoke an instance's
``set_metadata`` method in order to assign metadata to it, it would have absolutely no effect
on the actual metadata of the instance's underlying object entity that is stored on Amazon S3.
Similarly, invoking an instance's ``get_metadata`` method won't fetch the object's actual metadata,
though it will search for any metadata we may have assigned to it locally.

So how can we inspect the actual metadata of an object? This can be easily done
by setting the constructor's parameter ``load_metadata``. On top of opening a
connection to the file, setting this parameter to ``True`` will result in
the object's actual metadata being fetched via HTTPS and stored in memory.
Thus, going back to our example, we would be able to display the object's
metadata if we would just set the aformentioned parameter:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.storage import AmazonS3File

  # Gain access to 'file.txt' on Amazon S3 and print its metadata.
  with AmazonS3File(
    auth=AWSAuth(**aws_credentials),
    bucket='bucket',
    path='dir/file.txt',
    load_metadata=True
  ) as aws_obj:
      # Then print it.
      print(aws_obj.get_metadata())

Note that even if you don't set ``load_metadata`` to ``True`` during the
file's instantiation, you can always invoke method ``load_metadata``
manually at a later time and achieve the same result:

.. code-block:: python

  from fluke.auth import AWSAuth
  from fluke.storage import AmazonS3File

  # Gain access to 'file.txt' on Amazon S3 and print its metadata.
  with AmazonS3File(
    auth=AWSAuth(**aws_credentials),
    bucket='bucket',
    path='dir/file.txt',
    load_metadata=False
  ) as aws_obj:
      # Load metadata.
      aws_obj.load_metadata()
      # Then print it.
      print(aws_obj.get_metadata())

By executing the above code, we now get the expected output:

.. code-block::

  {'id': '12345', 'type': 'txt'}

As a final note, whenever setting ``include_metadata`` to ``True``, ``transfer_to`` will
always look first for any local metadata that can be assigned to the file(s) resulting from
the transfer. Nevertheless, if no metadata have been assigned to a file through
``set_metadata``, then ``transfer_to`` will actually go on to fetch any potentially
existing metadata associated with the file so that it may assign them to the resulting file.
This means that in the following example, any actual metadata associated with *file.txt*
will actually be carried over from Amazon S3 to Azure Blob Storage despite the fact that
they have not been loaded beforehand.

.. code-block:: python

  from fluke.auth import AWSAuth, AzureAuth
  from fluke.storage import AmazonS3File, AzureBlobFile

  with (
      AmazonS3File(
        auth=AWSAuth(**aws_credentials),
        bucket='bucket',
        path='dir/file.txt'
      ) as aws_obj,
      AzureBlobDir(
        auth=AzureAuth.from_service_principal(**azr_credentials),
        container='container',
        path='file.txt'
      ) as azr_dir
  ):
      aws_obj.transfer_to(dst=azr_dir, include_metadata=True)

..  _caching_data:

========================================
Catching data
========================================

Whenever you request some piece of information about a remote entity,
what happens under the hood is that Fluke will use an open connection
to the resource which possesses said information in order to transfer
it to our local machine so that you are able to access it. However,
fetching certain types of information can be quite time-consuming.
Consider for instance requesting the size of a virtual directory on
the cloud. In order to compute this value, Fluke must not only list
all individual files within the directory, but also fetch their
respective sizes so that it computes the total size of the directory.
As directories grow larger and larger, tasks similar to this require
more and more time to complete.

Due to the above stated reasons, Fluke offers the ability to cache
certain bits of information about remote entities after you've requested
them once, so that accessing them a second time doesn't take nearly as long
as it did the first time. You can declare a remote entity as cacheable by
setting parameter ``cache`` to ``True`` during its instantiation:

.. code-block:: python

  import time
  
  from fluke.auth import AWSAuth
  from fluke.storage import AmazonS3File

  auth = AWSAuth(**aws_credentials)

  with AmazonS3Dir(auth=auth, bucket='bucket', path='dir', cache=True) as s3_dir:
    # Fetch metadata via HTTPS.
    t = time.perf_counter()
    s3_dir.load_metadata()
    print(f"Fetched metadata in {time.perf_counter() - t:.2f} seconds!")

    # Fetch metadata from cache.
    t = time.perf_counter()
    s3_dir.load_metadata()
    print(f"Fetched metadata in {time.perf_counter() - t:.2f} seconds!")

Executing the above code block outputs the following:

.. code-block::

  Fetched metadata in 7.91 seconds!
  Fetched metadata in 0.01 seconds!

Note, however, that after caching information about a remote entity
you are going to be missing on any potential updates it receives,
as said information would be retrieved straight from the cache.
Be that as it may, you can always clear an instance's cache
by invoking ``purge``:

.. code-block:: python
  
  from fluke.auth import AWSAuth
  from fluke.storage import AmazonS3File

  with AmazonS3Dir(auth=AWSAuth(**aws_credentials), bucket='bucket', path='dir', cache=True) as s3_dir:
    # Count number of items in directory.
    print(f"Directory count: {s3_dir.count()}")

    # At this point, assume that one more
    # file is uploaded to the directory.

    # Re-count number of items in directory
    # without purging the cache.
    print(f"Directory count: {s3_dir.count()}")

    # Re-count number of items in directory
    # after purging the cache.
    s3_dir.purge()
    print(f"Directory count: {s3_dir.count()}")

By executing the above code, we get the following output:

.. code-block::

  Directory count: 1
  Directory count: 1
  Directory count: 2

Lastly, as mentioned in
:ref:`Intermediate access <intermediate-access>`,
all files and subdirectories that are accessed through a directory share the
same cache storage with it. This means that fetching some information about
them might speed up fetching information about the directory which spawned
them. Consider the following example:

.. code-block:: python

  from fluke.storage import AmazonS3Dir

  # This object will be used to authenticate with AWS.
  aws_auth = AWSAuth(**aws_credentials)

  # Access an AWS S3 directory and render it "cacheable".
  with AmazonS3Dir(auth=aws_auth, bucket="bucket", path='dir', cache=True) as s3_dir:
    # Fetch the directory's total size and time it.
    t = time.perf_counter()
    _ = s3_dir.get_size(recursively=True)
    print(f"Fetched size in {time.perf_counter() - t:.2f} seconds!")

    # Now purge the directory's cache.
    s3_dir.purge()

    # Fetch the sizes of the directory's file and subdirectory.
    _ = s3_dir.get_file('file.txt').get_size()
    _ = s3_dir.get_subdir('subdir').get_size(recursively=True)

    # Fetch the directory's total size and time it again.
    t = time.perf_counter()
    _ = s3_dir.get_size(recursively=True)
    print(f"Fetched size in {time.perf_counter() - t:.2f} seconds!")

The above code produces the following output when executed:

.. code-block::

  Fetched size in 21.17 seconds!
  Fetched size in 0.03 seconds!

The first time we requested the total size of the directory,
Fluke had to list the directory recursively in order to fetch
the respective sizes of all its files, and then finally
compute their sum. However, the second time we requested the same
value, we had already fetched all respective file sizes independently,
resulting in these values being cached. Therefore, Fluke could just grab
them from the cache and simply add them up so as to return the total
size of the directory.
