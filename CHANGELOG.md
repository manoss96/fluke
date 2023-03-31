
# Change Log
All notable changes to this project will be documented in this file.

## [0.3.0] - UNRELEASED

### Added

- *File/Dir* API *transfer_to* now receives a *chunk_size*
  parameter in order to specify the size of the file chunk
  that is transfered at a given time.

- *File* API *transfer_to* now receives a *suppress_output*
  parameter in order to suppress the method's output.
   
### Changed

- *File* API method *transfer_to* now returns bool instead of ``None``
  depending on whether the transfer was successful or not.
  (https://github.com/manoss96/fluke/issues/20)

- *Dir* API method *transfer_to* now receives a *suppress_output*
  parameter in place of *show_progress*.
 
### Fixed


## [0.2.0] - 2023/03/25

### Added

- *File* API now provides a *read* method
  in order to read a file's bytes. (https://github.com/manoss96/fluke/issues/11)

- *Dir* API now provides an *is_file* method
  used for determining whether the specified
  path points to a file or not. (https://github.com/manoss96/fluke/issues/16)

- *Dir* API now provides a set of methods in order to gain
  access to the directory's files via the *File* API. These
  methods are the following:
  + *get_file(file_path: str) -> File*
  + *traverse_files(recursively: bool = False) -> Iterator[File]*
  + *get_files(recursively: bool = False, show_abs_path: bool = False) -> dict[str, File]*
  
  All files spawned by a dictionary share the same metadata dictionaries
  with the directory that spawned them. In the case of remote files, they
  also share the same client and cache. (https://github.com/manoss96/fluke/issues/16)

- *fluke.storage.AWSS3File* now has a *get_bucket_name* method.
  (https://github.com/manoss96/fluke/issues/16)
   
### Changed

- Parameter *path* in *fluke.storage.AWSS3Dir* and *fluke.storage.AzureBlobDir*
  constructors now defaults to ``None`` in order to reference the whole
  container and bucket respectively (https://github.com/manoss96/fluke/issues/15)

- Providing a path that starts with a separator, e.g. ``/``, to constructors
  *fluke.storage.AWSS3Dir* and *fluke.storage.AzureBlobDir* will now throw
  an ``InvalidPathError`` exception. (https://github.com/manoss96/fluke/issues/15)

- Both *File* and *Dir* API methods *get_metadata* will now return
  an empty dictionary instead of ``None`` when no metadata has been
  set (https://github.com/manoss96/fluke/issues/16).

- *Dir* API method *iterate_contents* has been renamed to *traverse*.
  (https://github.com/manoss96/fluke/issues/16)
 
### Fixed

- Bug in *test_storage.MockSFTPClient.getfo*. (https://github.com/manoss96/fluke/issues/11)

- Explicitly sort the results of ``os.listdir`` and ``os.walk``
  due to inconsistent order. (https://github.com/manoss96/fluke/issues/12)

- Bug where the recursive cache was considered empty if the directory
  contained no sub-directories (https://github.com/manoss96/fluke/issues/16)

- Bug where both *File* and *Dir* API methods *get_metadata* returned
  the reference to the private metadata dictionary reference, instead
  of a copy of said dictionary (https://github.com/manoss96/fluke/issues/16).

 
## [0.1.0] - 2023/03/06
 
First release