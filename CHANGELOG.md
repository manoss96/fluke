
# Change Log
All notable changes to this project will be documented in this file.

## [0.5.0] - UNRELEASED

### Added

- There now exist classes ``fluke.storage.GCPStorageFile``
  and ``fluke.storage.GCPStorageDir`` in order to interact
  with Google Cloud Storage buckets. Furthermore, a class
  ``fluke.auth.GCPAuth`` has been added in order to be able
  to authenticate with GCP.
  (https://github.com/manoss96/fluke/issues/10)

- Added a new dependency: ``google-cloud-storage==2.10.0``
  (https://github.com/manoss96/fluke/issues/10)

- Class constructors ``fluke.storage.{AmazonS3File,AzureBlobFile,GCPStorageFile}``
  now receive a ``load_metadata`` parameter that can be used in order to load
  an object's metadata during instantiation.
  (https://github.com/manoss96/fluke/issues/46)

- Added method ``cat`` to *File* API that can be used
  in order to print the contents of a file as text.
  (https://github.com/manoss96/fluke/issues/58)

### Changed

- Methods ``Dir/File.transfer_to`` will now throw an ``InvalidChunkSizeError``
  exceptions if a chunk size that is not supported by the "destination"
  directory has been specified.
  (https://github.com/manoss96/fluke/issues/48)

- Method ``Dir.ls`` will now print a prettier output when
  parameter ``recursively`` has been set to ``True``.
  (https://github.com/manoss96/fluke/issues/50)

- Class constructor ``fluke.auth.RemoteAuth`` no longer receives
  string parameters ``public_key`` and ``key_type``. Instead, it
  receives a single ``public_key`` parameter of type
  ``RemoteAuth.PublicKey`` which can be generated via one of
  the ``RemoteAuth.PublicKey`` class' "generate" methods.
  (https://github.com/manoss96/fluke/issues/53)

- Any contents of ``fluke.storage.RemoteDir`` will now be
  displayed sorted by an ascending order no matter the
  underlying remote system.
  (https://github.com/manoss96/fluke/issues/53)

- ``fluke.storage.RemoteDir`` will not follow any symbolic
  links from now on.
  (https://github.com/manoss96/fluke/issues/60)


### Fixed

- Fixed issue where referencing the root directory ``/``
  through either ``fluke.storage.LocalDir`` or
  ``fluke.storage.RemoteDir`` resulted in an
  ``InvalidPathError`` exception being raised.
  (https://github.com/manoss96/fluke/issues/52)

- Fixed issue where it was impossible to establish a connection
  to a remote host if their public key had been previously
  added to the system's "known_hosts" file, though their
  public key had been changed since then.
  (https://github.com/manoss96/fluke/issues/53)

- Fixed bug that occurred when caching the root directory
  of any storage system.
  (https://github.com/manoss96/fluke/issues/56)

- Fixed bug where activating the cache on ``fluke.storage.RemoteDir``
  would sometimes result in getting different contents due to
  symbolic linked being followed.
  (https://github.com/manoss96/fluke/issues/60)
  

## [0.4.1] - 2023/07/25

### Changed

- A ``BucketNotFound`` exception will now be thrown during
  instantiation of either the ``fluke.storage.AmazonS3File`` or
  a ``fluke.storage.AmazonS3Dir`` class, if the bucket provided
  to the constructor via parameter ``bucket`` does not exist.
  (https://github.com/manoss96/fluke/issues/42)

- Class ``fluke.exceptions.AzureBlobContainerNotFoundError``
  has been renamed to ``ContainerNotFoundError``.
  (https://github.com/manoss96/fluke/issues/42)

### Fixed

- Fixed issue where method ``fluke.queues.AmazonSQSQueue.poll``
  would sometimes return fewer messages than the number of
  messages specified via parameter ``num_messages``.
  (https://github.com/manoss96/fluke/issues/40)


## [0.4.0] - 2023/07/24

### Added

- *Dir* API ``transfer_to`` method now receives a filter
  param in order to filter out files during the transfer.
  (https://github.com/manoss96/fluke/issues/30)

- There now exists a module ``fluke.queues`` which contains a
  number of classes used in order to access message queues
  provided by various message queue services in the cloud.
  (https://github.com/manoss96/fluke/issues/31)

- Added a new dependency: ``azure-storage-queue==12.6.0``
  (https://github.com/manoss96/fluke/issues/31)

### Changed

- Class ``fluke.auth.AzureAuth`` no longer has a constructor
  for creating an ``AzureAuth`` instance via an Azure
  service principal. Instead, this is now achieved
  via the ``AzureAuth.from_service_principal`` method.
  (https://github.com/manoss96/fluke/issues/31)

- Classes ``fluke.storage.{AWSS3File, AWSS3Dir}``
  have been renamed to ``fluke.storage.{AmazonS3File, AmazonS3Dir}``.
  (https://github.com/manoss96/fluke/issues/34)

- Updated dependency versions:
  + azure-identity: 1.12.0 -> 1.13.0
  + azure-storage-blob: 12.15.0 -> 12.17.0
  + boto3: 1.26.84 -> 1.28.0
  + paramiko: 3.0.0 -> 3.2.0

### Fixed

- Fixed issue where type of objects could not be inferred
  when they were instantiate via a context manager.
  (https://github.com/manoss96/fluke/issues/35)


## [0.3.0] - 2023/04/16

### Added

- *File* API now provides a ``read_range`` method in order
  to be able to partially read a file.
  (https://github.com/manoss96/fluke/issues/20)

- *File* API now provides a ``read_chunks`` method in order
  to be able to read files in chunks of bytes.
  (https://github.com/manoss96/fluke/issues/20)

- *File* API now provides a ``read_text`` method in order
  to be able to read files as text.
  (https://github.com/manoss96/fluke/issues/21)

- *File* API now provides a ``read_lines`` method in order
  to be able to read a text file line-by-line.
  (https://github.com/manoss96/fluke/issues/21)

- *Dir* API now provides a ``get_subdir`` method in order
  to be able to access any subdirectory of a directory
  as a *Dir* instance. (https://github.com/manoss96/fluke/issues/25)
   
### Changed

- *File/Dir* API method ``transfer_to`` now returns a ``bool`` value
  instead  of ``None`` depending on whether the transfer was successful
  or not. (https://github.com/manoss96/fluke/issues/20)

- *File/Dir* API ``transfer_to`` now receives a ``chunk_size``
  parameter in order to specify the size of the file chunk
  that is transfered at a given time.
  (https://github.com/manoss96/fluke/issues/20)

- *Dir* API method ``transfer_to`` now receives a ``suppress_output``
  parameter in place of ``show_progress``. (https://github.com/manoss96/fluke/issues/20)

- *File* API method ``get_file`` method now throws an ``InvalidPathError``
  exception if the specified file path does not exist.
  (https://github.com/manoss96/fluke/issues/25)

- *File* API method ``get_file`` method now throws an ``InvalidFileError``
  exception if the specified path exists, but points to a directory.
  (https://github.com/manoss96/fluke/issues/25)

- *File* API method ``get_file`` method's ``file_path`` param
  has been renamed to ``path``.
  (https://github.com/manoss96/fluke/issues/25)

- Removed *Dir* API methods ``get_files`` and ``traverse_files``.
  (https://github.com/manoss96/fluke/issues/25)

- Method ``AzureBlobDir.get_path`` now returns the
  empty string when no path (or a ``None`` path)
  has been provided via the class constructor.
  (https://github.com/manoss96/fluke/issues/25)
 
### Fixed

- Fixed bug where listing the contents of a cacheable directory
  after having fetched either the size or metadata of one of its
  files, would result in only listing said file.
  (https://github.com/manoss96/fluke/issues/25)


## [0.2.0] - 2023/03/25

### Added

- *File* API now provides a ``read`` method
  in order to read a file's bytes. (https://github.com/manoss96/fluke/issues/11)

- *Dir* API now provides an ``is_file`` method
  used for determining whether the specified
  path points to a file or not. (https://github.com/manoss96/fluke/issues/16)

- *Dir* API now provides a set of methods in order to gain
  access to the directory's files via the *File* API. These
  methods are the following:
  + ``get_file(file_path: str) -> File``
  + ``traverse_files(recursively: bool = False) -> Iterator[File]``
  + ``get_files(recursively: bool = False, show_abs_path: bool = False) -> dict[str, File]``
  
  All files spawned by a dictionary share the same metadata dictionaries
  with the directory that spawned them. In the case of remote files, they
  also share the same client and cache. (https://github.com/manoss96/fluke/issues/16)

- ``fluke.storage.AmazonS3File`` now has a ``get_bucket_name`` method.
  (https://github.com/manoss96/fluke/issues/16)
   
### Changed

- Parameter ``path`` in ``fluke.storage.AmazonS3Dir`` and ``fluke.storage.AzureBlobDir``
  constructors now defaults to ``None`` in order to gain access to the whole
  container and bucket respectively (https://github.com/manoss96/fluke/issues/15)

- Providing a path that starts with a separator, e.g. ``/``, to constructors
  ``fluke.storage.AmazonS3Dir`` and ``fluke.storage.AzureBlobDir`` will now throw
  an ``InvalidPathError`` exception. (https://github.com/manoss96/fluke/issues/15)

- Both *File* and *Dir* API method ``get_metadata`` will now return
  an empty dictionary instead of ``None`` when no metadata has been
  set (https://github.com/manoss96/fluke/issues/16).

- *Dir* API method ```iterate_contents`` has been renamed to ``traverse``.
  (https://github.com/manoss96/fluke/issues/16)
 
### Fixed

- Bug in ``tests.test_storage.MockSFTPClient.getfo``. (https://github.com/manoss96/fluke/issues/11)

- Explicitly sort the results of ``os.listdir`` and ``os.walk``
  due to inconsistent order. (https://github.com/manoss96/fluke/issues/12)

- Bug where the recursive cache was considered empty if the directory
  contained no sub-directories (https://github.com/manoss96/fluke/issues/16)

- Bug where both *File* and *Dir* API method ``get_metadata`` returned
  the reference to the private metadata dictionary reference, instead
  of a copy of said dictionary (https://github.com/manoss96/fluke/issues/16).

 
## [0.1.0] - 2023/03/06
 
First release