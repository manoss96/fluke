
# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased] - DATE

### Added

- *File* API now provides a *read* method
  in order to read a file's bytes. Added
  corresponding tests. (https://github.com/manoss96/fluke/issues/11)
   
### Changed
 
### Fixed

- Bug in *test_storage.MockSFTPClient.getfo*. (https://github.com/manoss96/fluke/issues/11)

- Explicitly sort the results of ``os.listdir`` and ``os.walk``
due to inconsistent order.

 
## [0.1.0] - 2023-03-06
 
First release