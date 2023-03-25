import os
import io
import sys
import time
import shutil
import unittest
from typing import Optional, Iterator, Callable
from unittest.mock import Mock, patch


import boto3
import paramiko
from moto import mock_s3


from fluke.auth import RemoteAuth, AWSAuth, AzureAuth
from fluke.storage import LocalFile, LocalDir, \
    RemoteFile, RemoteDir, AWSS3File, AWSS3Dir, \
    AzureBlobFile, AzureBlobDir
from fluke._exceptions import OverwriteError
from fluke._exceptions import InvalidPathError
from fluke._exceptions import InvalidFileError
from fluke._exceptions import InvalidDirectoryError
from fluke._exceptions import NonStringMetadataKeyError
from fluke._exceptions import NonStringMetadataValueError


'''
Helper Functions
'''
def join_paths(*paths: str) -> str:
    '''
    Joins the provided paths by using \
    the defined separator ``SEPARATOR``.
    '''
    from fluke._helper import join_paths
    return join_paths(SEPARATOR, *(p.replace(os.sep, SEPARATOR) for p in paths))


def to_abs(path: str) -> str:
    '''
    Convert a relative path to an absolute path.
    '''
    return join_paths(os.getcwd().replace(os.sep, SEPARATOR), path)


def get_remote_auth_instance(hostname: str) -> RemoteAuth:
    '''
    Returns a dummy ``RemoteAuth`` instance.

    :param str hostname: The instance's hostname.
    '''
    return RemoteAuth.from_password(
        hostname=hostname,
        username='',
        password='')


def get_aws_auth_instance() -> AWSAuth:
    '''
    Returns a dummy ``AWSAuth`` instance.
    '''
    return AWSAuth(
        aws_access_key_id='',
        aws_secret_access_key='')


def get_azure_auth_instance(from_conn_string: bool) -> AzureAuth:
    '''
    Returns a dummy ``AzureAuth`` instance.

    :param bool from_conn_string: Indicates whether \
        to construct the instance using a connection \
        string, or the regular Azure credentials.
    '''
    return (
        AzureAuth.from_conn_string(conn_string=STORAGE_ACCOUNT_CONN_STRING)
        if from_conn_string else 
        AzureAuth(
            account_url=STORAGE_ACCOUNT_URL,
            tenant_id='',
            client_id='',
            client_secret='')
    )


def create_aws_s3_bucket(mock_s3, bucket_name: str, metadata: dict[str, str]):
    '''
    Mocks an Amazon S3 bucket that contains a \
    copy of the ``TEST_FILES_DIR`` directory.

    :param moto.s3.mock_s3 mock_s3: A ``mock_s3`` instance.
    :param str bucket_name: The name of the Amazon S3 bucket.
    :param dict[str, str] metadata: A dictionary containing \
        the metadata that are to be assigned to every object \
        within the Amazon S3 bucket.
    '''

    mock_s3.start()

    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=bucket_name)
    bucket = s3.Bucket(bucket_name)
    # Structure it like "test_files".
    for dp, dn, fn in os.walk(TEST_FILES_DIR):
        dn.sort()
        for file in sorted(fn):
            file_path = join_paths(dp.replace(os.sep, SEPARATOR), file)
            with open(file_path, "rb") as file:
                bucket.upload_fileobj(
                    Key=file_path.replace(os.sep, SEPARATOR),
                    Fileobj=file,
                    ExtraArgs={ "Metadata": metadata })

        
def get_aws_s3_object(bucket_name: str, path: str):
    '''
    Returns an object within an Amazon S3 bucket.

    :param str bucket_name: The name of the Amazon S3 bucket.
    :param str path: The object's path within the bucket.
    '''
    return boto3.resource("s3").Bucket(bucket_name).Object(path)


def simulate_latency(func: Callable):
    '''
    This function is to be used as a decorator \
    function in order to simulate network latency \
    in mocked classes, so that cache-related methods \
    can be effectively tested.
    '''
    def wrapper(*args, **kwargs):
        time.sleep(0.02)
        return func(*args, **kwargs)
    return wrapper


'''
CONSTANTS
'''
SEPARATOR = '/'
TEST_FILES_DIR = "tests/test_files"
METADATA = {'1': '1'}

'''
FILE/OBJECT-RELATED CONSTANTS
'''
FILE_NAME = "file1.txt"
REL_FILE_PATH = f"{TEST_FILES_DIR}{SEPARATOR}{FILE_NAME}"
ABS_FILE_PATH = to_abs(REL_FILE_PATH)

'''
DIR-RELATED CONSTANTS
'''
DIR_NAME = "dir"
REL_DIR_PATH = f"{TEST_FILES_DIR}{SEPARATOR}{DIR_NAME}{SEPARATOR}"
ABS_DIR_PATH = to_abs(REL_DIR_PATH)
DIR_FILE_NAME = "file2.txt"
REL_DIR_FILE_PATH = f"{REL_DIR_PATH}{DIR_FILE_NAME}"
ABS_DIR_FILE_PATH = to_abs(REL_DIR_FILE_PATH)
CONTENTS = [DIR_FILE_NAME, f'subdir{SEPARATOR}']
RECURSIVE_CONTENTS = [DIR_FILE_NAME, f'subdir{SEPARATOR}file3.txt', f'subdir{SEPARATOR}file4.txt']
def get_abs_contents(recursively: bool):
    return [
        join_paths(ABS_DIR_PATH, p) for p in (
            RECURSIVE_CONTENTS if recursively else CONTENTS
        )
    ]

'''
REMOTE-RELATED CONSTANTS
'''
HOST = "host"

'''
AWS-RELATED CONSTANTS
'''
BUCKET = "bucket"

'''
AZURE-RELATED CONSTANTS
'''
CONTAINER = "container"
STORAGE_ACCOUNT = "account"
STORAGE_ACCOUNT_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/"
STORAGE_ACCOUNT_CONN_STRING = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};..."


'''
Helper Mock Classes
'''
class MockSFTPClient():

    @staticmethod
    def get_mock_methods() -> dict[str, Mock]:
        '''
        Returns a dictionary containing all the \
        methods that are to be mocked.
        '''
        return {
            'paramiko.SSHClient.load_host_keys': Mock(),
            'paramiko.SSHClient.get_host_keys': Mock(),
            'paramiko.SSHClient.connect': Mock(),
            'paramiko.SSHClient.close': Mock(),
            'paramiko.SSHClient.open_sftp': Mock(return_value=(MockSFTPClient())),
        }

    @simulate_latency
    def stat(self, path: str) -> paramiko.SFTPAttributes:

        if not os.path.exists(path):
            raise FileNotFoundError()
        
        os_stats = os.stat(path=path)
        stats = paramiko.SFTPAttributes()
        stats.filename = path.split(SEPARATOR)[-1]
        stats.st_mode = os_stats.st_mode
        stats.st_size = os_stats.st_size
        return stats

    @simulate_latency
    def mkdir(self, path: str):
        os.makedirs(name=path)

    @simulate_latency
    def listdir_attr(self, path: str) -> list[paramiko.SFTPAttributes]:
        return [self.stat(join_paths(path, f)) for f in sorted(os.listdir(path=path))]
    
    @simulate_latency
    def getfo(self, remotepath, fl, callback=None, prefetch=True):
        with open(remotepath, "rb") as fr:
            fl.write(fr.read())
            return fr.tell()
        
    @simulate_latency
    def putfo(self, fl, remotepath, file_size=0, callback=None, confirm=True):
        with open(remotepath, "wb") as fr:
            fr.write(fl.read())

    @simulate_latency
    def close(self):
        pass


class MockContainerClient():

    class MockBlobProperties():

        def __init__(self, name: str, metadata: dict[str, str], size: int):
            self.name = name
            self.metadata = metadata
            self.size = size

        def __getitem__(self, item):
            if item == 'name':
                return self.name
            elif item == 'size':
                return self.size
            raise ValueError()

    class MockStreamStorageDownloader():

        def __init__(self, name: str, metadata: dict[str, str], size: int):
            self.name = name
            self.size = size
            self.properties = MockContainerClient.MockBlobProperties(
                name=name,
                metadata=metadata,
                size=size)

        @simulate_latency
        def readinto(self, stream: io.BytesIO):
            with open(file=self.name, mode="rb") as file:
                stream.write(file.read())

    class MockBlobClient():

        def __init__(self, blob_name: str):
            self.blob_name: str = blob_name

        @simulate_latency
        def exists(self) -> bool:
            return self.blob_name == '' or os.path.exists(self.blob_name)

        @simulate_latency  
        def create_append_blob(self):
            os.makedirs(join_paths(*self.blob_name.split(SEPARATOR)[:-1]))
            with (
                open(file=self.blob_name, mode='wb') as file,
                io.BytesIO() as empty_buffer
            ):
                file.write(empty_buffer.read())

        @simulate_latency
        def delete_blob(self):
            os.remove(self.blob_name)

        @simulate_latency
        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            pass


    def __init__(self, container_name: str, path: str):
        self.container_name = container_name
        self.metadata: dict[str, dict[str, str]] = dict()
        # Add metadata for each file.
        for dp, dn, fn in os.walk(path):
            dn.sort()
            for file in sorted(fn):
                file_path = join_paths(dp, file)
                self.metadata.update({file_path : METADATA})


    @staticmethod
    def get_mock_methods() -> dict[str, Mock]:
        '''
        Returns a dictionary containing all the \
        methods that are to be mocked.
        '''

        return {
            'azure.identity.ClientSecretCredential.__init__': Mock(return_value=None),
            'azure.storage.blob.ContainerClient.__new__': Mock(
                return_value=MockContainerClient(CONTAINER, path=TEST_FILES_DIR)
            ),
            'azure.storage.blob.ContainerClient.__init__': Mock(return_value=None),
            'azure.storage.blob.ContainerClient.from_connection_string': Mock(
                return_value=MockContainerClient(CONTAINER, path=TEST_FILES_DIR)
            )
        }

    @simulate_latency
    def exists(self) -> bool:
        return True
    
    @simulate_latency    
    def get_blob_client(self, blob: str) -> MockBlobClient:
        return MockContainerClient.MockBlobClient(blob_name=blob)

    @simulate_latency
    def list_blobs(
        self,
        name_starts_with: str
    ) -> Iterator[MockBlobProperties]:
        for dp, dn, fn in os.walk(name_starts_with):
            dn.sort()
            for file in sorted(fn):
                obj_name = join_paths(dp, file)
                file_path = to_abs(obj_name)
                yield MockContainerClient.MockBlobProperties(
                    name = obj_name,
                    metadata=self.metadata[obj_name],
                    size=os.stat(file_path).st_size)

    @simulate_latency
    def list_blob_names(self, name_starts_with: str) -> Iterator[str]:
        for properties in self.list_blobs(name_starts_with):
            yield properties['name']

    @simulate_latency
    def walk_blobs(
        self,
        name_starts_with: str,
        delimiter: str
    ) -> Iterator[MockBlobProperties]:
        if delimiter == '':
            yield from self.list_blobs(name_starts_with)       
        elif delimiter == SEPARATOR:
            for name in sorted(os.listdir(name_starts_with)):
                obj_name = join_paths(name_starts_with, name)

                if os.path.isdir(obj_name):
                    obj_name += SEPARATOR

                file_path = to_abs(obj_name)

                yield MockContainerClient.MockBlobProperties(
                    name = obj_name,
                    metadata=(
                        None if obj_name.endswith(SEPARATOR)
                        else self.metadata[obj_name]
                    ),
                    size=os.stat(file_path).st_size) 
        else:
            raise ValueError()
                

    @simulate_latency    
    def download_blob(self, blob: str):
        file_path = to_abs(blob)
        return MockContainerClient.MockStreamStorageDownloader(
            name=blob,
            metadata=self.metadata[blob],
            size=os.stat(file_path).st_size)
    
    @simulate_latency   
    def upload_blob(
        self,
        name: str,
        data: io.BytesIO,
        metadata: Optional[dict[str, str]],
        overwrite: bool
    ):
        # Raise error if file exists and overwrite
        # has not been set to True.
        if os.path.exists(name) and not overwrite:
            raise OverwriteError(file_path=name)
        # Update metadata.
        self.metadata.update({name: metadata})
        # Write contents to file.
        with open(file=name, mode="wb") as file:
            file.write(data.read())

    @simulate_latency
    def close(self):
        pass


class TestLocalFile(unittest.TestCase):

    @staticmethod
    def build_file(path: str = ABS_FILE_PATH):
        return LocalFile(path=path)

    def test_constructor_on_relative_path(self):
        file = self.build_file()
        self.assertEqual(file.get_path(), ABS_FILE_PATH)

    def test_constructor_on_absolute_path(self):
        file = self.build_file()
        self.assertEqual(file.get_path(), ABS_FILE_PATH)

    def test_constructor_on_invalid_path_error(self):
        self.assertRaises(InvalidPathError, self.build_file, path="NON_EXISTING_PATH")

    def test_constructor_on_invalid_file_error(self):
        self.assertRaises(InvalidFileError, self.build_file, path=ABS_DIR_PATH)

    def test_get_name(self):
        file = self.build_file()
        self.assertEqual(file.get_name(), FILE_NAME)

    def test_get_uri(self):
        file = self.build_file()
        self.assertEqual(file.get_uri(), f"file:///{ABS_FILE_PATH.lstrip(SEPARATOR)}")

    def test_get_metadata(self):
        file = self.build_file()
        self.assertEqual(file.get_metadata(), {})

    def test_set_and_get_metadata(self):
        file = self.build_file()
        metadata = {'a': '1'}
        file.set_metadata(metadata=metadata)
        self.assertEqual(file.get_metadata(), metadata)

    def test_set_metadata_on_non_string_metadata_key_error(self):
        file = self.build_file()
        metadata = {1: '1'}
        self.assertRaises(NonStringMetadataKeyError, file.set_metadata, metadata=metadata)

    def test_set_metadata_on_non_string_metadata_value_error(self):
        file = self.build_file()
        metadata = {'1': 1}
        self.assertRaises(NonStringMetadataValueError, file.set_metadata, metadata=metadata)

    def test_get_size(self):
        file = self.build_file()
        self.assertEqual(file.get_size(), 4)

    def test_read(self):
        file = self.build_file()
        self.assertEqual(file.read(), b"TEXT")

    def test_transfer_to(self):
        file = self.build_file()
        dir = TestLocalDir.build_dir(path=ABS_DIR_PATH)

        # Copy file into dir.
        file.transfer_to(dst=dir)
        
        # Confirm that file was indeed copied.
        copy_path = join_paths(ABS_DIR_PATH, FILE_NAME)
        self.assertTrue(os.path.exists(copy_path))

        # Remove copy of the file.
        os.remove(copy_path)

    def test_transfer_to_on_overwrite_error(self):
        file = self.build_file()
        dir = TestLocalDir.build_dir(path=ABS_DIR_PATH)

        # Copy file into dir.
        file.transfer_to(dst=dir)
        # Ensure OverwriteError is raised when trying
        # to copy file a second time.
        self.assertRaises(OverwriteError, file.transfer_to, dst=dir)
        
        # Remove copy of the file.
        copy_path = join_paths(ABS_DIR_PATH, FILE_NAME)
        os.remove(copy_path)


class TestRemoteFile(unittest.TestCase):

    @staticmethod
    def build_file(path: str = ABS_FILE_PATH, cache: bool = False) -> RemoteFile:
        return RemoteFile(**{
            'auth': get_remote_auth_instance(hostname=HOST),
            'path': path,
            'cache': cache
        })

    def setUp(self):        
        for k, v in MockSFTPClient.get_mock_methods().items():
            patch(k, v).start()

    def tearDown(self):
        patch.stopall()

    def test_constructor(self):
        with self.build_file() as file:
            self.assertEqual(file.get_path(), ABS_FILE_PATH)

    def test_constructor_on_invalid_path_error(self):
        self.assertRaises(InvalidPathError, self.build_file, path="NON_EXISTING_PATH")

    def test_constructor_on_invalid_file_error(self):
        self.assertRaises(InvalidFileError, self.build_file, path=to_abs(REL_DIR_PATH))

    def test_get_name(self):
        with self.build_file() as file:
            self.assertEqual(file.get_name(), FILE_NAME)

    def test_get_hostname(self):
        with self.build_file() as file:
            self.assertEqual(file.get_hostname(), HOST)

    def test_get_uri(self):
        with self.build_file() as file:
            self.assertEqual(file.get_uri(), f"sftp://{HOST}/{ABS_FILE_PATH.lstrip(os.sep)}")

    def test_get_metadata(self):
        with self.build_file() as file:
            self.assertEqual(file.get_metadata(), {})

    def test_set_and_get_metadata(self):
        with self.build_file() as file:
            metadata = {'a': '1'}
            file.set_metadata(metadata=metadata)
            self.assertEqual(file.get_metadata(), metadata)

    def test_set_metadata_on_non_string_metadata_key_error(self):
        with self.build_file() as file:
            metadata = {1: '1'}
            self.assertRaises(NonStringMetadataKeyError, file.set_metadata, metadata=metadata)

    def test_set_metadata_on_non_string_metadata_value_error(self):
        with self.build_file() as file:
            metadata = {'1': 1}
            self.assertRaises(NonStringMetadataValueError, file.set_metadata, metadata=metadata)

    def test_get_size(self):
        with self.build_file() as file:
            self.assertEqual(file.get_size(), 4)

    def test_read(self):
        with self.build_file() as file:
            self.assertEqual(file.read(), b"TEXT")

    def test_transfer_to(self):
        with self.build_file() as file:
            # Copy file into dir.
            file.transfer_to(dst=TestLocalDir.build_dir(path=ABS_DIR_PATH))
            # Confirm that file was indeed copied.
            copy_path = join_paths(ABS_DIR_PATH, FILE_NAME)
            self.assertTrue(os.path.exists(copy_path))
            # Remove copy of the file.
            os.remove(copy_path)

    def test_transfer_to_on_overwrite_error(self):
        with self.build_file() as file:
            dir = TestLocalDir.build_dir(path=ABS_DIR_PATH)
            # Copy file into dir.
            file.transfer_to(dst=dir)
            # Ensure OverwriteError is raised when trying
            # to copy file a second time.
            self.assertRaises(OverwriteError, file.transfer_to, dst=dir)
            # Remove copy of the file.
            copy_path = join_paths(ABS_DIR_PATH, FILE_NAME)
            os.remove(copy_path)

    def test_transfer_to_on_include_metadata_set_to_false(self):
        with (
            mock_s3() as mocks3,
            self.build_file() as file
        ):
            # Create AWS bucket and contents.
            create_aws_s3_bucket(
                mocks3,
                BUCKET,
                metadata=METADATA)
           # Copy file into dir, including its metadata.
            file.set_metadata(metadata=METADATA)
            dir_path = REL_DIR_PATH
            with (AWSS3Dir(
                    auth=get_aws_auth_instance(),
                    bucket=BUCKET,
                    path=dir_path) as aws_dir
            ):
                file.transfer_to(dst=aws_dir, include_metadata=False)
            # Gain access to the uploaded object.
            file_path = join_paths(dir_path, FILE_NAME)
            obj = get_aws_s3_object(BUCKET, file_path)
            # Assert that metadata were copied to the object.
            self.assertEqual(obj.metadata, {})
            # Delete object.
            obj.delete()

    def test_transfer_to_on_include_metadata_set_to_true(self):
        with (
            mock_s3() as mocks3,
            self.build_file() as file
        ):
            # Create AWS bucket and contents.
            create_aws_s3_bucket(
                mocks3,
                BUCKET,
                metadata=METADATA)
           # Copy file into dir, including its metadata.
            new_metadata = {'2': '2'}
            file.set_metadata(metadata=new_metadata)
            dir_path = REL_DIR_PATH
            with (AWSS3Dir(
                    auth=get_aws_auth_instance(),
                    bucket=BUCKET,
                    path=dir_path) as aws_dir
            ):
                file.transfer_to(dst=aws_dir, include_metadata=True)
            # Gain access to the uploaded object.
            file_path = join_paths(dir_path, FILE_NAME)
            obj = get_aws_s3_object(BUCKET, file_path)
            # Assert that metadata were copied to the object.
            self.assertEqual(obj.metadata, new_metadata)
            # Delete object.
            obj.delete()

    '''
    Test connection methods.
    '''
    def test_open(self):
        file = self.build_file()
        file.close()
        file.open()
        self.assertTrue(file._get_handler().is_open())
        file.close()

    def test_close(self):
        file = self.build_file()
        file.close()
        self.assertFalse(file._get_handler().is_open())

    '''
    Test cache methods.
    '''
    def test_is_cachable_on_false(self):
        with self.build_file(cache=False) as file:
            self.assertEqual(file.is_cacheable(), False)

    def test_is_cachable_on_true(self):
        with self.build_file(cache=True) as file:
            self.assertEqual(file.is_cacheable(), True)

    def test_purge(self):
        with self.build_file(cache=True) as file:
            # Fetch size via HTTP.
            _ = file.get_size()
            # Fetch size from cache and time it.
            t = time.time()
            _ = file.get_size()
            cache_time = time.time() - t
            # Purge cache.
            file.purge()
            # Re-fetch size via HTTP and time it.
            t = time.time()
            _ = file.get_size()
            normal_time = time.time() - t
            self.assertGreater(normal_time, cache_time)

    def test_get_size_from_cache_on_value(self):
        with self.build_file(cache=True) as file:
            _ = file.get_size()
            size = file.get_size()
            self.assertEqual(size, 4)

    def test_get_size_from_cache_on_time(self):
        with self.build_file(cache=True) as file:
            # Fetch object size via HTTP.
            t = time.time()
            _ = file.get_size()
            normal_time = time.time() - t
            # Fetch object size from cache.
            t = time.time()
            _ = file.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)


class TestAWSS3File(unittest.TestCase):

    MOCK_S3 = mock_s3()

    def setUp(self):
        create_aws_s3_bucket(self.MOCK_S3, BUCKET, METADATA)

        from fluke._handlers import AWSClientHandler

        m1 = patch.object(AWSClientHandler, '_get_file_size_impl', autospec=True)
        m2 = patch.object(AWSClientHandler, '_get_file_metadata_impl', autospec=True)
        m3 = patch.object(AWSClientHandler, '_traverse_dir_impl', autospec=True)

        def simulate_latency_1(*args, **kwargs):
            time.sleep(0.2)
            return m1.temp_original(*args, **kwargs)
        
        def simulate_latency_2(*args, **kwargs):
            time.sleep(0.2)
            return m2.temp_original(*args, **kwargs)
        
        def simulate_latency_3(*args, **kwargs):
            time.sleep(0.2)
            return m3.temp_original(*args, **kwargs)

        m1.start().side_effect = simulate_latency_1
        m2.start().side_effect = simulate_latency_2
        m3.start().side_effect = simulate_latency_3


    def tearDown(self):
        self.MOCK_S3.stop()
        patch.stopall()
    
    @staticmethod
    def build_file(path: str = REL_FILE_PATH, cache: bool = False) -> AWSS3File:
        return AWSS3File(**{
            'auth': get_aws_auth_instance(),
            'bucket': BUCKET,
            'path': path,
            'cache': cache
        })

    def test_constructor(self):
        with self.build_file() as file:
            self.assertEqual(file.get_path(), REL_FILE_PATH)

    def test_constructor_on_invalid_path_error(self):
        self.assertRaises(InvalidPathError, self.build_file, path="NON_EXISTING_PATH")

    def test_constructor_on_invalid_file_error(self):
        self.assertRaises(InvalidFileError, self.build_file, path=REL_DIR_PATH)

    def test_get_name(self):
        with self.build_file() as file:
            self.assertEqual(file.get_name(), FILE_NAME)

    def test_get_uri(self):
        with self.build_file() as file:
            self.assertEqual(file.get_uri(), f"s3://{BUCKET}/{REL_FILE_PATH}")

    def test_get_bucket_name(self):
        with self.build_file() as file:
            self.assertEqual(file.get_bucket_name(), BUCKET)

    def test_get_metadata(self):
        with self.build_file() as file:
            self.assertEqual(file.get_metadata(), {})

    def test_set_and_get_metadata(self):
        with self.build_file() as file:
            metadata = {'a': '1'}
            file.set_metadata(metadata=metadata)
            self.assertEqual(file.get_metadata(), metadata)

    def test_set_metadata_on_non_string_metadata_key_error(self):
        with self.build_file() as file:
            metadata = {1: '1'}
            self.assertRaises(NonStringMetadataKeyError, file.set_metadata, metadata=metadata)

    def test_set_metadata_on_non_string_metadata_value_error(self):
        with self.build_file() as file:
            metadata = {'1': 1}
            self.assertRaises(NonStringMetadataValueError, file.set_metadata, metadata=metadata)

    def test_load_metadata(self):
        with self.build_file() as file:
            file.load_metadata()
            self.assertEqual(file.get_metadata(), METADATA)

    def test_load_metadata_after_set_metadata(self):
        with self.build_file() as file:
            file.set_metadata({'a': 'a'})
            file.load_metadata()
            self.assertEqual(file.get_metadata(), METADATA)

    def test_set_metadata_after_load_metadata(self):
        with self.build_file() as file:
            file.load_metadata()
            new_metadata = {'a': 'a'}
            file.set_metadata(new_metadata)
            self.assertEqual(file.get_metadata(), new_metadata)

    def test_get_size(self):
        with self.build_file() as file:
            self.assertEqual(file.get_size(), 4)

    def test_read(self):
        with self.build_file() as file:
            self.assertEqual(file.read(), b"TEXT")

    def test_transfer_to(self):
        with self.build_file() as file:
            # Copy file into dir.
            file.transfer_to(dst=TestLocalDir.build_dir(path=ABS_DIR_PATH))
            # Confirm that file was indeed copied.
            copy_path = join_paths(ABS_DIR_PATH, FILE_NAME)
            self.assertTrue(os.path.exists(copy_path))
            # Remove copy of the file.
            os.remove(copy_path)

    def test_transfer_to_on_overwrite_error(self):
        with self.build_file() as file:
            dir = TestLocalDir.build_dir(path=ABS_DIR_PATH)
            # Copy file into dir.
            file.transfer_to(dst=dir)
            # Ensure OverwriteError is raised when trying
            # to copy file a second time.
            self.assertRaises(OverwriteError, file.transfer_to, dst=dir)
            # Remove copy of the file.
            copy_path = join_paths(ABS_DIR_PATH, FILE_NAME)
            os.remove(copy_path)

    def test_transfer_to_on_include_metadata_set_to_false(self):
        with self.build_file() as file:
            dir_path = REL_DIR_PATH
            file_path = join_paths(dir_path, FILE_NAME)
            # Copy file into dir, including its metadata.
            with (AWSS3Dir(
                    auth=get_aws_auth_instance(),
                    bucket=BUCKET,
                    path=dir_path)
            as aws_dir):
                file.transfer_to(dst=aws_dir, include_metadata=False)
            # Gain access to the uploaded object.
            obj = get_aws_s3_object(BUCKET, file_path)
            # Assert that metadata were copied to the object.
            self.assertEqual(obj.metadata, {})
            # Delete object.
            obj.delete()

    def test_transfer_to_on_include_metadata_set_to_true(self):
        with self.build_file() as file:
            dir_path = REL_DIR_PATH
            file_path = join_paths(dir_path, FILE_NAME)
            # Copy file into dir, including its metadata.
            with (AWSS3Dir(
                    auth=get_aws_auth_instance(),
                    bucket=BUCKET,
                    path=dir_path)
            as aws_dir):
                new_metadata = {'2': '2'}
                file.set_metadata(new_metadata)
                file.transfer_to(dst=aws_dir, include_metadata=True)
            # Gain access to the uploaded object.
            obj = get_aws_s3_object(BUCKET, file_path)
            # Assert that metadata were copied to the object.
            self.assertEqual(obj.metadata, new_metadata)
            # Delete object.
            obj.delete()

    '''
    Test connection methods.
    '''
    def test_open(self):
        file = self.build_file()
        file.close()
        file.open()
        self.assertTrue(file._get_handler().is_open())
        file.close()

    def test_close(self):
        file = self.build_file()
        file.close()
        self.assertFalse(file._get_handler().is_open())

    '''
    Test cache methods.
    '''
    def test_is_cachable_on_false(self):
        with self.build_file() as file:
            self.assertEqual(file.is_cacheable(), False)

    def test_is_cachable_on_true(self):
        with self.build_file(cache=True) as file:
            self.assertEqual(file.is_cacheable(), True)

    def test_purge(self):
        with self.build_file(cache=True) as file:
            # Load metadata via HTTP.
            file.load_metadata()
            # Load metadata via cache and time it.
            t = time.time()
            file.load_metadata()
            cache_time = time.time() - t
            # Purge cache.
            file.purge()
            # Load metadata via HTTP and time it.
            t = time.time()
            file.load_metadata()
            normal_time = time.time() - t
            self.assertGreater(normal_time, cache_time)

    def test_load_metadata_from_cache_on_value(self):
        with self.build_file(cache=True) as file:
            # Load metadata via HTTP.
            file.load_metadata()
            # Load metadata from cache.
            file.load_metadata()
            self.assertEqual(file.get_metadata(), METADATA)

    def test_load_metadata_from_cache_on_time(self):
        with self.build_file(cache=True) as file:
            # Fetch object metadata via HTTP.
            t = time.time()
            _ = file.load_metadata()
            normal_time = time.time() - t
            # Fetch object metadata from cache.
            t = time.time()
            _ = file.load_metadata()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_get_size_from_cache_on_value(self):
        with self.build_file(cache=True) as file:
            _ = file.get_size()
            self.assertEqual(file.get_size(), 4)

    def test_get_size_from_cache_on_time(self):
        with self.build_file(cache=True) as file:
            # Fetch object size via HTTP.
            t = time.time()
            _ = file.get_size()
            normal_time = time.time() - t
            # Fetch object size from cache.
            t = time.time()
            _ = file.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)


class TestAzureBlobFile(unittest.TestCase):

    @staticmethod
    def build_file(
        path: str = REL_FILE_PATH,
        cache: bool = False,
        from_conn_string: bool = False
    ) -> AzureBlobFile:
        return AzureBlobFile(**{
            'auth': get_azure_auth_instance(from_conn_string),
            'container': CONTAINER,
            'path': path,
            'cache': cache
        })

    def setUp(self):
        for k, v in MockContainerClient.get_mock_methods().items():
            patch(k, v).start()

    def tearDown(self):
        patch.stopall()

    def test_constructor(self):
        with self.build_file() as file:
            self.assertEqual(file.get_path(), REL_FILE_PATH)

    def test_constructor_from_conn_string(self):
        with self.build_file(from_conn_string=True) as file:
            self.assertEqual(file.get_path(), REL_FILE_PATH)

    def test_constructor_on_invalid_path_error(self):
        self.assertRaises(InvalidPathError, self.build_file, path="NON_EXISTING_PATH")

    def test_constructor_on_invalid_file_error(self):
        self.assertRaises(InvalidFileError, self.build_file, path=REL_DIR_PATH)

    def test_get_name(self):
        with self.build_file() as file:
            self.assertEqual(file.get_name(), FILE_NAME)

    def test_get_container_name(self):
        with self.build_file() as file:
            self.assertEqual(file.get_container_name(), CONTAINER)

    def test_get_uri(self):
        with self.build_file() as file:
            uri = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}"
            uri += f".dfs.core.windows.net/{REL_FILE_PATH}"
            self.assertEqual(file.get_uri(), uri)

    def test_get_metadata(self):
        with self.build_file() as file:
            self.assertEqual(file.get_metadata(), {})

    def test_set_and_get_metadata(self):
        with self.build_file() as file:
            metadata = {'a': '1'}
            file.set_metadata(metadata=metadata)
            self.assertEqual(file.get_metadata(), metadata)

    def test_set_metadata_on_non_string_metadata_key_error(self):
        with self.build_file() as file:
            metadata = {1: '1'}
            self.assertRaises(NonStringMetadataKeyError, file.set_metadata, metadata=metadata)

    def test_set_metadata_on_non_string_metadata_value_error(self):
        with self.build_file() as file:
            metadata = {'1': 1}
            self.assertRaises(NonStringMetadataValueError, file.set_metadata, metadata=metadata)

    def test_load_metadata(self):
        with self.build_file() as file:
            file.load_metadata()
            self.assertEqual(file.get_metadata(), METADATA)

    def test_load_metadata_after_set_metadata(self):
        with self.build_file() as file:
            file.set_metadata({'a': 'a'})
            file.load_metadata()
            self.assertEqual(file.get_metadata(), METADATA)

    def test_set_metadata_after_load_metadata(self):
        with self.build_file() as file:
            file.load_metadata()
            new_metadata = {'a': 'a'}
            file.set_metadata(new_metadata)
            self.assertEqual(file.get_metadata(), new_metadata)

    def test_get_size(self):
        with self.build_file() as file:
            self.assertEqual(file.get_size(), 4)


    def test_read(self):
        with self.build_file() as file:
            self.assertEqual(file.read(), b"TEXT")

    def test_transfer_to(self):
        with self.build_file() as file:
            # Copy file into dir.
            file.transfer_to(dst=TestLocalDir.build_dir(path=ABS_DIR_PATH))
            # Confirm that file was indeed copied.
            copy_path = join_paths(ABS_DIR_PATH, FILE_NAME)
            self.assertTrue(os.path.exists(copy_path))
            # Remove copy of the file.
            os.remove(copy_path)

    def test_transfer_to_on_overwrite_error(self):
        with self.build_file() as file:
            dir = TestLocalDir.build_dir(path=ABS_DIR_PATH)
            # Copy file into dir.
            file.transfer_to(dst=dir)
            # Ensure OverwriteError is raised when trying
            # to copy file a second time.
            self.assertRaises(OverwriteError, file.transfer_to, dst=dir)
            # Remove copy of the file.
            copy_path = join_paths(ABS_DIR_PATH, FILE_NAME)
            os.remove(copy_path)

    def test_transfer_to_on_include_metadata_set_to_false(self):
        with (
            mock_s3() as mocks3,
            self.build_file() as file
        ):
            # Create AWS bucket and contents.
            create_aws_s3_bucket(
                mocks3,
                BUCKET,
                metadata=METADATA)
           # Copy file into dir, including its metadata.
            file.set_metadata(metadata=METADATA)
            dir_path = REL_DIR_PATH
            with (AWSS3Dir(
                    auth=get_aws_auth_instance(),
                    bucket=BUCKET,
                    path=dir_path) as aws_dir
            ):
                file.transfer_to(dst=aws_dir, include_metadata=False)
            # Gain access to the uploaded object.
            file_path = join_paths(dir_path, FILE_NAME)
            obj = get_aws_s3_object(BUCKET, file_path)
            # Assert that metadata were copied to the object.
            self.assertEqual(obj.metadata, {})
            # Delete object.
            obj.delete()

    def test_transfer_to_on_include_metadata_set_to_true(self):
        with (
            mock_s3() as mocks3,
            self.build_file() as file
        ):
            # Create AWS bucket and contents.
            create_aws_s3_bucket(
                mocks3,
                BUCKET,
                metadata=METADATA)
            # Copy file into dir, including its metadata.
            new_metadata = {'2': '2'}
            file.set_metadata(metadata=new_metadata)
            dir_path = REL_DIR_PATH
            with (AWSS3Dir(
                    auth=get_aws_auth_instance(),
                    bucket=BUCKET,
                    path=dir_path) as aws_dir
            ):
                file.transfer_to(dst=aws_dir, include_metadata=True)
            # Gain access to the uploaded object.
            file_path = join_paths(dir_path, FILE_NAME)
            obj = get_aws_s3_object(BUCKET, file_path)
            # Assert that metadata were copied to the object.
            self.assertEqual(obj.metadata, new_metadata)
            # Delete object.
            obj.delete()

    '''
    Test connection methods.
    '''
    def test_open(self):
        file = self.build_file()
        file.close()
        file.open()
        self.assertTrue(file._get_handler().is_open())
        file.close()

    def test_close(self):
        file = self.build_file()
        file.close()
        self.assertFalse(file._get_handler().is_open())

    '''
    Test cache methods.
    '''
    def test_is_cachable_on_false(self):
        with self.build_file() as file:
            self.assertEqual(file.is_cacheable(), False)

    def test_is_cachable_on_true(self):
        with self.build_file(cache=True) as file:
            self.assertEqual(file.is_cacheable(), True)

    def test_purge(self):
        with self.build_file(cache=True) as file:
            # Load metadata via HTTP.
            file.load_metadata()
            # Load metadata via cache and time it.
            t = time.time()
            file.load_metadata()
            cache_time = time.time() - t
            # Purge cache.
            file.purge()
            # Load metadata via HTTP and time it.
            t = time.time()
            file.load_metadata()
            normal_time = time.time() - t
            self.assertGreater(normal_time, cache_time)

    def test_load_metadata_from_cache_on_value(self):
        with self.build_file(cache=True) as file:
            # Load metadata via HTTP.
            file.load_metadata()
            # Load metadata from cache.
            file.load_metadata()
            self.assertEqual(file.get_metadata(), METADATA)

    def test_load_metadata_from_cache_on_time(self):
        with self.build_file(cache=True) as file:
            # Fetch object metadata via HTTP.
            t = time.time()
            _ = file.load_metadata()
            normal_time = time.time() - t
            # Fetch object metadata from cache.
            t = time.time()
            _ = file.load_metadata()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_get_size_from_cache_on_value(self):
        with self.build_file(cache=True) as file:
            _ = file.get_size()
            self.assertEqual(file.get_size(), 4)

    def test_get_size_from_cache_on_time(self):
        with self.build_file(cache=True) as file:
            # Fetch object size via HTTP.
            t = time.time()
            _ = file.get_size()
            normal_time = time.time() - t
            # Fetch object size from cache.
            t = time.time()
            _ = file.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)


class TestLocalDir(unittest.TestCase):
    
    @staticmethod
    def build_dir(
        path: str = ABS_DIR_PATH,
        create_if_missing: bool = False
    ) -> LocalDir:
        return LocalDir(
            path=path,
            create_if_missing=create_if_missing)

    
    def test_constructor_on_create_if_missing(self):
        path = to_abs("NON_EXISTING_DIR")
        _ = self.build_dir(path, create_if_missing=True)
        self.assertTrue(os.path.isdir(path))
        os.rmdir(path)

    def test_constructor_on_invalid_path_error(self):
        self.assertRaises(InvalidPathError, self.build_dir, path="NON_EXISTING_PATH")

    def test_constructor_on_invalid_directory_error(self):
        self.assertRaises(InvalidDirectoryError, self.build_dir, path=ABS_DIR_FILE_PATH)

    def test_get_path_on_relative_path(self):
        dir = self.build_dir(path=REL_DIR_PATH)
        self.assertEqual(dir.get_path(), ABS_DIR_PATH)

    def test_get_path_on_absolute_path(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        self.assertEqual(dir.get_path(), ABS_DIR_PATH)

    def test__get_separator(self):
        self.assertEqual(self.build_dir(path=os.getcwd())._get_separator(), os.sep)

    def test_get_name(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        self.assertEqual(dir.get_name(), DIR_NAME.rstrip(SEPARATOR))

    def test_get_uri(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        self.assertEqual(dir.get_uri(), f"file:///{ABS_DIR_PATH.lstrip(SEPARATOR)}")

    def test_get_metadata_on_invalid_file_error(self):
        dir = self.build_dir()
        self.assertRaises(InvalidFileError, dir.get_metadata, file_path="NON_EXISTING_PATH")

    def test_get_metadata(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        self.assertEqual(dir.get_metadata(file_path=DIR_FILE_NAME), {})

    def test_set_and_get_metadata_on_relative_path(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        file_path, metadata = DIR_FILE_NAME, {'a': '1'}
        dir.set_metadata(file_path=file_path, metadata=metadata)
        self.assertEqual(dir.get_metadata(file_path=file_path), metadata)

    def test_set_and_get_metadata_on_absolute_path(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        file_path = ABS_DIR_FILE_PATH
        metadata = {'a': '1'}
        dir.set_metadata(file_path=file_path, metadata=metadata)
        self.assertEqual(dir.get_metadata(file_path=file_path), metadata)

    def test_set_metadata_on_non_string_metadata_key_error(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        args = {
            'file_path': DIR_FILE_NAME,
            'metadata': {1: '1'}
        }
        self.assertRaises(NonStringMetadataKeyError, dir.set_metadata, **args)

    def test_set_metadata_on_non_string_metadata_value_error(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        args = {
            'file_path': DIR_FILE_NAME,
            'metadata': {'1': 1}
        }
        self.assertRaises(NonStringMetadataValueError, dir.set_metadata, **args)

    def test_set_metadata_on_invalid_file_error(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        args = {
            'file_path': 'NON_EXISTING_FILE',
            'metadata': {'1': '1'}
        }
        self.assertRaises(InvalidFileError, dir.set_metadata, **args)

    def test_path_exists_on_abs_path(self):
        self.assertEqual(self.build_dir(
            path=ABS_DIR_PATH).path_exists(ABS_DIR_FILE_PATH), True)

    def test_path_exists_on_relative_path(self):
        self.assertEqual(self.build_dir(
            path=ABS_DIR_PATH).path_exists(DIR_FILE_NAME), True)

    def test_path_not_exists_on_abs_path(self):
        file_path = join_paths(ABS_DIR_PATH, 'NON_EXISTING_FILE')
        self.assertEqual(self.build_dir(
            path=ABS_DIR_PATH).path_exists(file_path), False)

    def test_path_not_exists_on_relative_path(self):
        file_path = 'NON_EXISTING_FILE'
        self.assertEqual(self.build_dir(
            path=ABS_DIR_PATH).path_exists(file_path), False)

    def test_get_contents(self):
        self.assertEqual(self.build_dir(
            path=ABS_DIR_PATH).get_contents(), CONTENTS)

    def test_get_contents_on_show_abs_path(self):
        self.assertEqual(
            self.build_dir(path=ABS_DIR_PATH).get_contents(show_abs_path=True),
            get_abs_contents(recursively=False))
        
    def test_get_contents_on_recursively(self):
        self.assertEqual(
            self.build_dir(path=ABS_DIR_PATH).get_contents(recursively=True),
            RECURSIVE_CONTENTS)
        
    def test_get_contents_on_show_abs_path_and_recursively(self):
        self.assertEqual(
            self.build_dir(path=ABS_DIR_PATH).get_contents(
                show_abs_path=True, recursively=True),
            get_abs_contents(recursively=True))
        
    def test_traverse(self):
        self.assertEqual(list(self.build_dir(
            path=ABS_DIR_PATH).traverse()), CONTENTS)

    def test_traverse_on_show_abs_path(self):
        self.assertEqual(
            list(self.build_dir(
                path=ABS_DIR_PATH).traverse(show_abs_path=True)),
            get_abs_contents(recursively=False))
        
    def test_traverse_on_recursively(self):
        self.assertEqual(
            list(self.build_dir(
                path=ABS_DIR_PATH).traverse(recursively=True)),
            RECURSIVE_CONTENTS)
        
    def test_traverse_on_show_abs_path_and_recursively(self):
        self.assertEqual(
            list(self.build_dir(
                path=ABS_DIR_PATH).traverse(show_abs_path=True, recursively=True)),
            get_abs_contents(recursively=True))

    def test_ls(self):
        with io.StringIO() as stdo:
            sys.stdout = stdo

            self.build_dir(path=ABS_DIR_PATH).ls()

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(CONTENTS) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_ls_on_show_abs_path(self):
        with io.StringIO() as stdo:
            sys.stdout = stdo

            self.build_dir(path=ABS_DIR_PATH).ls(show_abs_path=True)

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(get_abs_contents(recursively=False)) + '\n'

            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_ls_on_recursively(self):
        with io.StringIO() as stdo:
            sys.stdout = stdo

            self.build_dir(path=ABS_DIR_PATH).ls(recursively=True)

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(RECURSIVE_CONTENTS) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_ls_on_show_abs_path_and_recursively(self):
        with io.StringIO() as stdo:
            sys.stdout = stdo

            self.build_dir(path=ABS_DIR_PATH).ls(
                show_abs_path=True,
                recursively=True)
            
            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(get_abs_contents(recursively=True)) + '\n'

            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_count(self):
        self.assertEqual(self.build_dir(path=ABS_DIR_PATH).count(), 2)

    def test_count_on_recursively(self):
        self.assertEqual(self.build_dir(path=ABS_DIR_PATH).count(recursively=True), 3)

    def test_get_size(self):
        self.assertEqual(self.build_dir(path=ABS_DIR_PATH).get_size(), 4)

    def test_get_size_on_recursively(self):
        self.assertEqual(self.build_dir(path=ABS_DIR_PATH).get_size(recursively=True), 16)

    def test_transfer_to(self):
        # Create a temporary dictionary.
        tmp_dir_path = to_abs(REL_DIR_PATH.replace('dir', 'TMP_DIR'))
        os.mkdir(tmp_dir_path)
        # Copy the directory's contents into this tmp directory.
        self.build_dir(path=ABS_DIR_PATH).transfer_to(
            dst=self.build_dir(path=tmp_dir_path))
        # Assert that the two directories contains the same contents
        self.assertEqual(
            ''.join(s for s in sorted(os.listdir(ABS_DIR_PATH)) if s.endswith('.txt')),
            ''.join(s for s in sorted(os.listdir(tmp_dir_path))))
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_recursively(self):
        # Create a temporary dictionary.
        tmp_dir_name = 'TMP_DIR'
        tmp_dir_path = to_abs(REL_DIR_PATH.replace('dir', tmp_dir_name))
        os.mkdir(tmp_dir_path)
        # Recursively copy the directory's contents
        # into this tmp directory.
        self.build_dir(path=ABS_DIR_PATH).transfer_to(
            dst=self.build_dir(path=tmp_dir_path),
            recursively=True)
        # Assert that the two directories contain the same contents.
        self.assertEqual(
            ''.join(join_paths(dp, f).replace(
                f'{SEPARATOR}dir{SEPARATOR}',
                f'{SEPARATOR}{tmp_dir_name}{SEPARATOR}')
                for dp, _, fn in os.walk(ABS_DIR_PATH) for f in fn),
            ''.join(join_paths(dp, f)
                for dp, _, fn in os.walk(tmp_dir_path) for f in fn))
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_overwrite_set_to_false(self):
        # Create a copy of the directory.
        tmp_dir_name = 'TMP_DIR'
        tmp_dir_path = to_abs(REL_DIR_PATH.replace('dir', tmp_dir_name))
        shutil.copytree(src=ABS_DIR_PATH, dst=tmp_dir_path)
        # While capturing stdout...
        with io.StringIO() as stdo:
            sys.stdout = stdo
            # Copy directory with "overwrite" set to "False".
            self.build_dir(path=ABS_DIR_PATH).transfer_to(
                dst=LocalDir(path=tmp_dir_path),
                overwrite=False)

            sys.stdout = sys.__stdout__

            self.assertTrue("Operation unsuccessful" in stdo.getvalue())
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_overwrite_set_to_true(self):
        # Create a copy of the directory.
        tmp_dir_name = 'TMP_DIR'
        tmp_dir_path = to_abs(REL_DIR_PATH.replace('dir', tmp_dir_name))
        shutil.copytree(src=ABS_DIR_PATH, dst=tmp_dir_path)
        # While capturing stdout...
        with io.StringIO() as stdo:
            sys.stdout = stdo
            # Copy directory with "overwrite" set to "True".
            self.build_dir(path=ABS_DIR_PATH).transfer_to(
                dst=self.build_dir(path=tmp_dir_path),
                overwrite=True)

            sys.stdout = sys.__stdout__

            self.assertTrue("Operation successful" in stdo.getvalue())
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_set_to_false(self):
        # Set metadata for a directory's file.
        filename, metadata = DIR_FILE_NAME, {'1': '1'}
        dir = self.build_dir(path=ABS_DIR_PATH)
        dir.set_metadata(file_path=filename, metadata=metadata)
        # Create a temporary dictionary.
        tmp_dir_path = to_abs(REL_DIR_PATH.replace('dir', 'TMP_DIR'))
        os.mkdir(tmp_dir_path)
        tmp_dir = self.build_dir(path=tmp_dir_path)
        # Copy the directory's contents into this
        # tmp directory without including metadata.
        dir.transfer_to(dst=tmp_dir, include_metadata=False)
        # Assert that no metadata were transfered.
        self.assertEqual(tmp_dir.get_metadata(file_path=filename), {})
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_set_to_true(self):
        # Set metadata for a directory's file.
        filename, metadata = DIR_FILE_NAME, {'1': '1'}
        dir = self.build_dir(path=ABS_DIR_PATH)
        dir.set_metadata(file_path=filename, metadata=metadata)
        # Create a temporary dictionary.
        tmp_dir_path = to_abs(REL_DIR_PATH.replace('dir', 'TMP_DIR'))
        os.mkdir(tmp_dir_path)
        tmp_dir = self.build_dir(path=tmp_dir_path)
        # Copy the directory's contents into this
        # tmp directory while including metadata.
        dir.transfer_to(dst=tmp_dir, include_metadata=True)
        # Assert that the two directories contains the same contents.
        self.assertEqual(
            tmp_dir.get_metadata(file_path=filename),
            metadata)
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_cloud_dir_include_metadata_set_to_true(self):
        # Set metadata for a directory's file.
        with mock_s3() as mocks3:
            # Create AWSS3 bucket.
            create_aws_s3_bucket(mocks3, BUCKET, METADATA)
            # Copy directory contents including metadata
            filename, metadata = DIR_FILE_NAME, {'2': '2'}
            dir = self.build_dir(path=ABS_DIR_PATH)
            with (TestAWSS3Dir.build_dir() as aws_dir):
                dir.set_metadata(file_path=filename, metadata=metadata)
                dir.transfer_to(dst=aws_dir, overwrite=True, include_metadata=True)
                # Assert that the object's metadata have been modified.
                self.assertEqual(
                    get_aws_s3_object(
                        aws_dir.get_bucket_name(),
                        REL_DIR_FILE_PATH).metadata,
                    metadata)
                
    def test_traverse_files(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        self.assertEqual(
            ''.join(map(lambda file: file.get_path(), dir.traverse_files())),
            ''.join(filter(lambda path: not path.endswith('/'), get_abs_contents(recursively=False))))

    def test_traverse_files_on_recursively(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        self.assertEqual(
            ''.join(map(lambda file: file.get_path(), dir.traverse_files(recursively=True))),
            ''.join(get_abs_contents(recursively=True)))
        
    def test_get_files(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        files = dir.get_files()
        self.assertEqual(
            ''.join(map(lambda path: files[path].get_path(), files)),
            ''.join(filter(lambda path: not path.endswith('/'), get_abs_contents(recursively=False))))

    def test_get_files_on_recursively(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        files = dir.get_files(recursively=True)
        self.assertEqual(
            ''.join(map(lambda path: files[path].get_path(), files)),
            ''.join(get_abs_contents(recursively=True)))
        
    def test_get_files_on_show_abs_path(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        files = dir.get_files(show_abs_path=True)
        self.assertEqual(
            ''.join(files),
            ''.join(filter(lambda path: not path.endswith('/'), get_abs_contents(recursively=False))))
        
    def test_get_files_on_recursively_and_show_abs_path(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        files = dir.get_files(recursively=True, show_abs_path=True)
        self.assertEqual(
            ''.join(files),
            ''.join(get_abs_contents(recursively=True)))
        
    def test_get_file(self):
        dir = self.build_dir(path=ABS_DIR_PATH)
        file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
        file = dir.get_file(file_path)
        self.assertEqual(file.get_path(), ABS_DIR_FILE_PATH)

    def test_file_shared_metadata_on_modify_from_dir(self):
        # Create dir and get file.
        dir = self.build_dir(path=ABS_DIR_PATH)
        file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
        file = dir.get_file(file_path)
        # Change metadata via "Dir" API.
        dir.set_metadata(file_path, METADATA)
        # Assert file metadata have been changed.
        self.assertEqual(file.get_metadata(), METADATA)

    def test_file_shared_metadata_on_modify_from_file(self):
        # Create dir and get file.
        dir = self.build_dir(path=ABS_DIR_PATH)
        file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
        file = dir.get_file(file_path)
        # Change metadata via "File" API.
        file.set_metadata(METADATA)
        # Assert file metadata have been changed.
        self.assertEqual(dir.get_metadata(file_path), METADATA)
        


class TestRemoteDir(unittest.TestCase):

    def setUp(self):
        for k, v in MockSFTPClient.get_mock_methods().items():
            patch(k, v).start()

    def tearDown(self):
        patch.stopall()
    
    @staticmethod
    def build_dir(
        path: str = ABS_DIR_PATH,
        cache: bool = False,
        create_if_missing: bool = False
    ) -> RemoteDir:
        return RemoteDir(**{
            'auth': get_remote_auth_instance(hostname=HOST),
            'path': path,
            'cache': cache,
            'create_if_missing': create_if_missing
        })
    
    def test_constructor(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_path(), ABS_DIR_PATH)

    def test_constructor_on_create_if_missing(self):
        dir_path = to_abs(f"{TEST_FILES_DIR}{SEPARATOR}NON_EXISTING_DIR/")
        with self.build_dir(path=dir_path, create_if_missing=True) as _:
            self.assertTrue(os.path.isdir(dir_path))
        os.rmdir(dir_path)

    def test_constructor_on_empty_path_error(self):
        self.assertRaises(InvalidPathError, self.build_dir, path="")

    def test_constructor_on_invalid_path_error(self):
        self.assertRaises(InvalidPathError, self.build_dir, path="NON_EXISTING_PATH")

    def test_get_hostname(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_hostname(), HOST)

    def test_get_name(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_name(), DIR_NAME)    

    def test_get_uri(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_uri(), f"sftp://{HOST}/{ABS_DIR_PATH.lstrip(os.sep)}")
            
    def test_set_and_get_metadata(self):
        with self.build_dir() as dir:
            file_path = ABS_DIR_FILE_PATH
            metadata = {'a': '1'}
            dir.set_metadata(file_path=file_path, metadata=metadata)
            self.assertEqual(dir.get_metadata(file_path=file_path), metadata)

    def test_get_metadata(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_metadata(file_path=ABS_DIR_FILE_PATH), {})

    def test_get_metadata_on_invalid_file_error(self):
        with self.build_dir() as dir:
            self.assertRaises(InvalidFileError, dir.get_metadata, file_path="NON_EXISTING_PATH")

    def test_set_metadata_on_non_string_metadata_key_error(self):
        with self.build_dir() as dir:
            args = {
                'file_path': ABS_DIR_FILE_PATH,
                'metadata': {1: '1'}
            }
            self.assertRaises(NonStringMetadataKeyError, dir.set_metadata, **args)

    def test_set_metadata_on_non_string_metadata_value_error(self):
        with self.build_dir() as dir:
            args = {
                'file_path': ABS_DIR_FILE_PATH,
                'metadata': {'1': 1}
            }
            self.assertRaises(NonStringMetadataValueError, dir.set_metadata, **args)

    def test_set_metadata_on_invalid_file_error(self):
        with self.build_dir() as dir:
            args = {
                'file_path': 'NON_EXISTING_FILE',
                'metadata': {'1': '1'}
            }
            self.assertRaises(InvalidFileError, dir.set_metadata, **args)

    def test_path_exists(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.path_exists(ABS_DIR_FILE_PATH), True)

    def test_path_not_exists(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.path_exists("NON_EXISTING_FILE"), False)

    def test_get_contents(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_contents(), CONTENTS)

    def test_get_contents_on_show_abs_path(self):
        with self.build_dir() as dir:
            self.assertEqual(
                dir.get_contents(show_abs_path=True),
                get_abs_contents(recursively=False))
        
    def test_get_contents_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                dir.get_contents(recursively=True),
                RECURSIVE_CONTENTS)
        
    def test_get_contents_on_show_abs_path_and_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                dir.get_contents(show_abs_path=True, recursively=True),
                get_abs_contents(recursively=True))
            
    def test_traverse(self):
        with self.build_dir() as dir:
            self.assertEqual(list(dir.traverse()), CONTENTS)

    def test_traverse_on_show_abs_path(self):
        with self.build_dir() as dir:
            self.assertEqual(
                list(dir.traverse(show_abs_path=True)),
                get_abs_contents(recursively=False))
        
    def test_traverse_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                list(dir.traverse(recursively=True)),
                RECURSIVE_CONTENTS)
        
    def test_traverse_on_show_abs_path_and_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                list(dir.traverse(show_abs_path=True, recursively=True)),
                get_abs_contents(recursively=True))
            
    def test_ls(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls()

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(CONTENTS) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)


    def test_ls_on_show_abs_path(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls(show_abs_path=True)

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(get_abs_contents(recursively=False)) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_ls_on_recursively(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls(recursively=True)

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(RECURSIVE_CONTENTS) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_ls_on_show_abs_path_and_recursively(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls(show_abs_path=True, recursively=True)
            
            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(get_abs_contents(recursively=True)) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_count(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.count(), 2)

    def test_count_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.count(recursively=True), 3)

    def test_get_size(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_size(), 4)

    def test_get_size_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_size(recursively=True), 16)

    def test_transfer_to(self):
        # Create a temporary dictionary.
        tmp_dir_path = ABS_DIR_PATH.replace(DIR_NAME, 'TMP_DIR')
        os.mkdir(tmp_dir_path)
        # Copy the directory's contents into this tmp directory.
        with self.build_dir() as dir:
            dir.transfer_to(dst=LocalDir(path=tmp_dir_path))
        # Assert that the two directories contain the same contents.
        self.assertEqual(
            ''.join(s for s in sorted(os.listdir(REL_DIR_PATH)) if s.endswith('.txt')),
            ''.join(s for s in sorted(os.listdir(tmp_dir_path))))
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_recursively(self):
        # Create a temporary dictionary.
        tmp_dir_path = ABS_DIR_PATH.replace(DIR_NAME, 'TMP_DIR')
        os.mkdir(tmp_dir_path)
        # Copy the directory's contents into this tmp directory.
        with self.build_dir() as dir:
            dir.transfer_to(
                dst=LocalDir(path=tmp_dir_path),
                recursively=True)
        # Assert that the two directories contain the same contents.
        self.assertEqual(
            ''.join(join_paths(dp, f).removeprefix(REL_DIR_PATH)
                for dp, _, fn in os.walk(REL_DIR_PATH) for f in fn),
            ''.join(join_paths(dp, f).removeprefix(tmp_dir_path)
                for dp, _, fn in os.walk(tmp_dir_path) for f in fn))
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_overwrite_set_to_false(self):
        # Create a copy of the directory.
        tmp_dir_name = 'TMP_DIR'
        tmp_dir_path = ABS_DIR_PATH.replace(DIR_NAME, tmp_dir_name)
        shutil.copytree(src=ABS_DIR_PATH, dst=tmp_dir_path)
        # While capturing stdout...
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo
            # Copy directory with "overwrite" set to "False".
            dir.transfer_to(
                dst=LocalDir(path=tmp_dir_path),
                overwrite=False)

            sys.stdout = sys.__stdout__

            self.assertTrue("Operation unsuccessful" in stdo.getvalue())
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_overwrite_set_to_true(self):
        # Create a copy of the directory.
        tmp_dir_name = 'TMP_DIR'
        tmp_dir_path = ABS_DIR_PATH.replace(DIR_NAME, tmp_dir_name)
        shutil.copytree(src=ABS_DIR_PATH, dst=tmp_dir_path)
        # While capturing stdout...
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo
            # Copy directory with "overwrite" set to "True".
            dir.transfer_to(
                dst=LocalDir(path=tmp_dir_path),
                overwrite=True)

            sys.stdout = sys.__stdout__

            self.assertTrue("Operation successful" in stdo.getvalue())
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_set_to_false(self):
        # Set metadata for a directory's file.
        filename, metadata = ABS_DIR_FILE_PATH, {'1': '1'}
        with self.build_dir() as dir:
            dir.set_metadata(file_path=filename, metadata=metadata)
            # Create a temporary dictionary.
            tmp_dir_path = ABS_DIR_PATH.replace(DIR_NAME, 'TMP_DIR')
            os.makedirs(tmp_dir_path)
            tmp_dir = LocalDir(path=tmp_dir_path)
            # Copy the directory's contents into this
            # tmp directory without including metadata.
            dir.transfer_to(dst=tmp_dir, include_metadata=False)
        # Assert that no metadata have been transfered.
        self.assertEqual(tmp_dir.get_metadata(
            file_path=filename.replace(ABS_DIR_PATH, '')), {})
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_set_to_true(self):
        # Set metadata for a directory's file.
        filename, metadata = ABS_DIR_FILE_PATH, {'1': '1'}
        with self.build_dir() as dir:
            dir.set_metadata(file_path=filename, metadata=metadata)
            # Create a temporary dictionary.
            tmp_dir_path = ABS_DIR_PATH.replace(DIR_NAME, 'TMP_DIR')
            os.mkdir(tmp_dir_path)
            tmp_dir = LocalDir(path=tmp_dir_path)
            # Copy the directory's contents into this
            # tmp directory without including metadata.
            dir.transfer_to(dst=tmp_dir, include_metadata=True)
        # Assert the file's metadata are the same.
        self.assertEqual(
            tmp_dir.get_metadata(
                file_path=filename.replace(ABS_DIR_PATH, '')),
            metadata)
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_cloud_dir_include_metadata_set_to_true(self):
        # Set metadata for a directory's file.
        with mock_s3() as mocks3:
            # Create AWSS3 bucket.
            create_aws_s3_bucket(mocks3, BUCKET, METADATA)
            # Copy directory contents including metadata
            filename, metadata = ABS_DIR_FILE_PATH, {'2': '2'}
            with (
                self.build_dir() as dir,
                TestAWSS3Dir().build_dir() as aws_dir
            ):
                dir.set_metadata(file_path=filename, metadata=metadata)
                dir.transfer_to(dst=aws_dir, overwrite=True, include_metadata=True)
                # Assert that the object's metadata have been modified.
                self.assertEqual(
                    get_aws_s3_object(
                        aws_dir.get_bucket_name(),
                        REL_DIR_FILE_PATH).metadata,
                    metadata)
                
    def test_transfer_to_as_destination(self):
        # Create a "remote" temporary dictionary.
        tmp_dir_path = REL_DIR_PATH.replace(DIR_NAME, 'TMP_DIR')
        os.mkdir(tmp_dir_path)
        # Copy the local dir's contents into this
        # "remote" tmp directory.
        with self.build_dir(path=tmp_dir_path) as dir:
            TestLocalDir.build_dir(ABS_DIR_PATH).transfer_to(dir)
        # Assert that the two directories contain the same contents.
        self.assertEqual(
            ''.join(s for s in sorted(os.listdir(ABS_DIR_PATH)) if s.endswith('.txt')),
            ''.join(s for s in sorted(os.listdir(tmp_dir_path))))
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_traverse_files(self):
        with self.build_dir() as dir:
            self.assertEqual(
                ''.join(map(lambda file: file.get_path(), dir.traverse_files())),
                ''.join(filter(lambda path: not path.endswith('/'), get_abs_contents(recursively=False))))

    def test_traverse_files_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                ''.join(map(lambda file: file.get_path(), dir.traverse_files(recursively=True))),
                ''.join(get_abs_contents(recursively=True)))
        
    def test_get_files(self):
        with self.build_dir() as dir:
            files = dir.get_files()
            self.assertEqual(
                ''.join(map(lambda path: files[path].get_path(), files)),
                ''.join(filter(lambda path: not path.endswith('/'), get_abs_contents(recursively=False))))

    def test_get_files_on_recursively(self):
        with self.build_dir() as dir:
            files = dir.get_files(recursively=True)
            self.assertEqual(
                ''.join(map(lambda path: files[path].get_path(), files)),
                ''.join(get_abs_contents(recursively=True)))
        
    def test_get_files_on_show_abs_path(self):
        with self.build_dir() as dir:
            files = dir.get_files(show_abs_path=True)
            self.assertEqual(
                ''.join(files),
                ''.join(filter(lambda path: not path.endswith('/'), get_abs_contents(recursively=False))))
        
    def test_get_files_on_recursively_and_show_abs_path(self):
        with self.build_dir() as dir:
            files = dir.get_files(recursively=True, show_abs_path=True)
            self.assertEqual(
                ''.join(files),
                ''.join(get_abs_contents(recursively=True)))
        
    def test_get_file(self):
        with self.build_dir() as dir:
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            file = dir.get_file(file_path)
            self.assertEqual(file.get_path(), ABS_DIR_FILE_PATH)

    def test_file_shared_metadata_on_modify_from_dir(self):
        with self.build_dir() as dir:
            # Access file via dir.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            file = dir.get_file(file_path)
            # Change metadata via "Dir" API.
            dir.set_metadata(file_path, METADATA)
            # Assert file metadata have been changed.
            self.assertEqual(file.get_metadata(), METADATA)

    def test_file_shared_metadata_on_modify_from_file(self):
        with self.build_dir() as dir:
            # Access file via dir.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            file = dir.get_file(file_path)
            # Change metadata via "File" API.
            file.set_metadata(METADATA)
            # Assert file metadata have been changed.
            self.assertEqual(dir.get_metadata(file_path), METADATA)
        
    '''
    Test connection methods.
    '''
    def test_open(self):
        dir = self.build_dir()
        dir.close()
        dir.open()
        self.assertTrue(dir._get_handler().is_open())
        dir.close()

    def test_close(self):
        dir = self.build_dir()
        dir.close()
        self.assertFalse(dir._get_handler().is_open())

    '''
    Test cache methods.
    '''
    def test_is_cachable_on_false(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.is_cacheable(), False)

    def test_is_cachable_on_true(self):
        with self.build_dir(cache=True) as dir:
            self.assertEqual(dir.is_cacheable(), True)

    def test_purge(self):
        with self.build_dir(cache=True) as dir:
            # Fetch size via HTTP.
            dir.get_size()
            # Fetch size from cache and time it.
            t = time.time()
            dir.get_size()
            cache_time = time.time() - t
            # Purge cache.
            dir.purge()
            # Re-fetch size via HTTP and time it.
            t = time.time()
            dir.get_size()
            normal_time = time.time() - t
            self.assertGreater(normal_time, cache_time)

    def test_traverse_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = (_ for _ in dir.traverse())
            self.assertEqual(
                ''.join([p for p in dir.traverse()]),
                ''.join(CONTENTS))
            
    def test_traverse_recursively_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = (_ for _ in dir.traverse(recursively=True))
            expected_results = []
            for dp, dn, fn in os.walk(REL_DIR_PATH):
                dn.sort()
                for f in sorted(fn):
                    expected_results.append(
                        join_paths(dp, f).removeprefix(REL_DIR_PATH))
            self.assertEqual(
                ''.join([p for p in dir.traverse(recursively=True)]),
                ''.join(expected_results))

    def test_traverse_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch contents via HTTP.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            normal_time = time.time() - t
            # Fetch contents from cache.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_traverse_recursively_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch contents via HTTP.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            normal_time = time.time() - t
            # Fetch contents from cache.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_get_size_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = dir.get_size()
            self.assertEqual(dir.get_size(), 4)

    def test_get_size_recursively_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = dir.get_size(recursively=True)
            self.assertEqual(
                dir.get_size(recursively=True), 16)

    def test_get_size_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch object size via HTTP.
            t = time.time()
            _ = dir.get_size()
            normal_time = time.time() - t
            # Fetch object size from cache.
            t = time.time()
            _ = dir.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_get_size_recursively_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch object size via HTTP.
            t = time.time()
            _ = dir.get_size(recursively=True)
            normal_time = time.time() - t
            # Fetch object size from cache.
            t = time.time()
            _ = dir.get_size(recursively=True)
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_file_shared_cache_on_cache_via_dir(self):
        with (
            self.build_dir(cache=False) as no_cache_dir,
            self.build_dir(cache=True) as cache_dir
        ):
            # Access file via both dirs.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            no_cache_file = no_cache_dir.get_file(file_path)
            cache_file = cache_dir.get_file(file_path)
            # Count total size for both dirs.
            _ = no_cache_dir.get_size()
            _ = cache_dir.get_size()
            # Time no-cache-file's "get_size"
            t = time.time()
            _ = no_cache_file.get_size()
            normal_time = time.time() - t
            # Time cache-file's "get_size"
            t = time.time()
            _ = cache_file.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_file_shared_cache_on_cache_via_file(self):
        with (
            self.build_dir(cache=False) as no_cache_dir,
            self.build_dir(cache=True) as cache_dir
        ):
            # Count size of files via both dirs using the "File" API.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            for file in no_cache_dir.get_files().values():
                _ = file.get_size()
            for file in cache_dir.get_files().values():
                _ = file.get_size()
            # Time no-cache-dir's "get_size"
            t = time.time()
            _ = no_cache_dir.get_size()
            normal_time = time.time() - t
            # Time cache-dir's "get_size"
            t = time.time()
            _ = cache_dir.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)


class TestAWSS3Dir(unittest.TestCase):

    MOCK_S3 = mock_s3()

    def get_abs_contents(self, recursively: bool):
        return [join_paths(REL_DIR_PATH, p) for p in (
            RECURSIVE_CONTENTS if recursively else CONTENTS
        )]
    
    def iterate_aws_s3_dir_objects(
        self,
        path: str = REL_DIR_PATH,
        recursively: bool = False,
        show_abs_path: bool = False
    ):
        delimiter = '' if recursively else SEPARATOR
        for obj in boto3.resource('s3').Bucket(BUCKET).objects.filter(
            Prefix=path,
            Delimiter=delimiter
        ):
            yield obj.key if show_abs_path else obj.key.removeprefix(path)

    def setUp(self):
        create_aws_s3_bucket(self.MOCK_S3, BUCKET, METADATA)

        from fluke._handlers import AWSClientHandler

        m1 = patch.object(AWSClientHandler, '_get_file_size_impl', autospec=True)
        m2 = patch.object(AWSClientHandler, '_get_file_metadata_impl', autospec=True)
        m3 = patch.object(AWSClientHandler, '_traverse_dir_impl', autospec=True)

        def simulate_latency_1(*args, **kwargs):
            time.sleep(0.2)
            return m1.temp_original(*args, **kwargs)
        
        def simulate_latency_2(*args, **kwargs):
            time.sleep(0.2)
            return m2.temp_original(*args, **kwargs)
        
        def simulate_latency_3(*args, **kwargs):
            time.sleep(0.2)
            return m3.temp_original(*args, **kwargs)

        m1.start().side_effect = simulate_latency_1
        m2.start().side_effect = simulate_latency_2
        m3.start().side_effect = simulate_latency_3

    def tearDown(self):
        self.MOCK_S3.stop()
        patch.stopall()
    
    @staticmethod
    def build_dir(
        path: str = REL_DIR_PATH,
        cache: bool = False,
        create_if_missing: bool = False
    ) -> AWSS3Dir:
        return AWSS3Dir(**{
            'auth': get_aws_auth_instance(),
            'bucket': BUCKET,
            'path': path,
            'cache': cache,
            'create_if_missing': create_if_missing
        })
    
    def test_constructor(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_path(), REL_DIR_PATH)

    def test_constructor_on_create_if_missing(self):
        dir_path = "NON_EXISTING_DIR/"
        with self.build_dir(path=dir_path, create_if_missing=True) as _:
            obj = boto3.resource("s3").Bucket(BUCKET).Object(dir_path)
            try:
                obj.load()
                obj.delete()
            except:
                self.fail(f"Directory {dir_path} was not created!")

    def test_constructor_on_invalid_path_error(self):
        self.assertRaises(InvalidPathError, self.build_dir, path="NON_EXISTING_PATH")

    def test_get_path_on_none_path(self):
        with self.build_dir(path=None) as dir:
            self.assertEqual(dir.get_path(), '')  

    def test_get_name(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_name(), DIR_NAME)

    def test_get_name_on_none_path(self):
        with self.build_dir(path=None) as dir:
            self.assertIsNone(dir.get_name())      

    def test_get_uri(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_uri(), f"s3://{BUCKET}/{REL_DIR_PATH}")

    def test_get_bucket_name(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_bucket_name(), BUCKET)

    def test_get_metadata(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_metadata(file_path='file2.txt'), {})

    def test_get_metadata_on_invalid_file_error(self):
        with self.build_dir() as dir:
            self.assertRaises(InvalidFileError, dir.get_metadata, file_path="NON_EXISTING_PATH")

    def test_set_and_get_metadata_on_relative_path(self):
        with self.build_dir() as dir:
            file_path, metadata = 'file2.txt', {'a': '1'}
            dir.set_metadata(file_path=file_path, metadata=metadata)
            self.assertEqual(dir.get_metadata(file_path=file_path), metadata)

    def test_set_and_get_metadata_on_absolute_path(self):
        with self.build_dir() as dir:
            file_path = join_paths(dir.get_path(), 'file2.txt')
            metadata = {'a': '1'}
            dir.set_metadata(file_path=file_path, metadata=metadata)
            self.assertEqual(dir.get_metadata(file_path=file_path), metadata)

    def test_set_metadata_on_non_string_metadata_key_error(self):
        with self.build_dir() as dir:
            args = {
                'file_path': 'file2.txt',
                'metadata': {1: '1'}
            }
            self.assertRaises(NonStringMetadataKeyError, dir.set_metadata, **args)

    def test_set_metadata_on_non_string_metadata_value_error(self):
        with self.build_dir() as dir:
            args = {
                'file_path': 'file2.txt',
                'metadata': {'1': 1}
            }
            self.assertRaises(NonStringMetadataValueError, dir.set_metadata, **args)

    def test_set_metadata_on_invalid_file_error(self):
        with self.build_dir() as dir:
            args = {
                'file_path': 'NON_EXISTING_FILE',
                'metadata': {'1': '1'}
            }
            self.assertRaises(InvalidFileError, dir.set_metadata, **args)

    def test_set_metadata_after_load_metadata(self):
        with self.build_dir() as dir:
            dir.load_metadata()
            new_metadata = {'a': 'a'}
            dir.set_metadata(REL_DIR_FILE_PATH, new_metadata)
            self.assertEqual(dir.get_metadata(REL_DIR_FILE_PATH), new_metadata)

    def test_load_metadata(self):
        with self.build_dir() as dir:
            dir.load_metadata()
            self.assertEqual(dir.get_metadata(REL_DIR_FILE_PATH), METADATA)

    def test_load_metadata_after_set_metadata(self):
        with self.build_dir() as dir:
            dir.set_metadata(REL_DIR_FILE_PATH, {'a': 'a'})
            dir.load_metadata()
            self.assertEqual(dir.get_metadata(REL_DIR_FILE_PATH), METADATA)

    def test_path_exists_on_abs_path(self):
        with self.build_dir() as dir:
            file_path = join_paths(REL_DIR_PATH, 'file2.txt')
            self.assertEqual(dir.path_exists(file_path), True)

    def test_path_exists_on_relative_path(self):
        with self.build_dir() as dir:
            file_path = 'file2.txt'
            self.assertEqual(dir.path_exists(file_path), True)

    def test_path_not_exists_on_abs_path(self):
        with self.build_dir() as dir:
            file_path = join_paths(REL_DIR_PATH, 'NON_EXISTING_FILE')
            self.assertEqual(dir.path_exists(file_path), False)

    def test_path_not_exists_on_relative_path(self):
        with self.build_dir() as dir:
            file_path = 'NON_EXISTING_FILE'
            self.assertEqual(dir.path_exists(file_path), False)

    def test_get_contents(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_contents(), CONTENTS)

    def test_get_contents_on_show_abs_path(self):
        with self.build_dir() as dir:
            self.assertEqual(
                dir.get_contents(show_abs_path=True),
                self.get_abs_contents(recursively=False))
        
    def test_get_contents_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                dir.get_contents(recursively=True),
                RECURSIVE_CONTENTS)
        
    def test_get_contents_on_show_abs_path_and_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                dir.get_contents(show_abs_path=True, recursively=True),
                self.get_abs_contents(recursively=True))
            
    def test_traverse(self):
        with self.build_dir() as dir:
            self.assertEqual(list(dir.traverse()), CONTENTS)

    def test_traverse_on_show_abs_path(self):
        with self.build_dir() as dir:
            self.assertEqual(
                list(dir.traverse(show_abs_path=True)),
                self.get_abs_contents(recursively=False))
        
    def test_traverse_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                list(dir.traverse(recursively=True)),
                RECURSIVE_CONTENTS)
        
    def test_traverse_on_show_abs_path_and_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                list(dir.traverse(show_abs_path=True, recursively=True)),
                self.get_abs_contents(recursively=True))
            
    def test_ls(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls()

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(CONTENTS) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)


    def test_ls_on_show_abs_path(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls(show_abs_path=True)

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(
                self.get_abs_contents(recursively=False)) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_ls_on_recursively(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls(recursively=True)

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(RECURSIVE_CONTENTS) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_ls_on_show_abs_path_and_recursively(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls(show_abs_path=True, recursively=True)
            
            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(
                self.get_abs_contents(recursively=True)) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_count(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.count(), 2)

    def test_count_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.count(recursively=True), 3)

    def test_get_size(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_size(), 4)

    def test_get_size_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_size(recursively=True), 16)

    def test_transfer_to(self):
        # Create a temporary dictionary.
        tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, 'TMP_DIR'))
        os.mkdir(tmp_dir_path)
        # Copy the directory's contents into this tmp directory.
        with self.build_dir() as dir:
            dir.transfer_to(dst=TestLocalDir.build_dir(path=tmp_dir_path))
        # Assert that the two directories contain the same contents.
        self.assertEqual(
            ''.join(s for s in sorted(self.iterate_aws_s3_dir_objects())),
            ''.join(s for s in sorted(os.listdir(tmp_dir_path))))
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_recursively(self):
        # Create a temporary dictionary.
        tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, 'TMP_DIR'))
        os.mkdir(tmp_dir_path)
        # Copy the directory's contents into this tmp directory.
        with self.build_dir() as dir:
            dir.transfer_to(
                dst=TestLocalDir.build_dir(path=tmp_dir_path),
                recursively=True)
        # Assert that the two directories contain the same contents.
            expected_results = []
            for dp, dn, fn in os.walk(tmp_dir_path):
                dn.sort()
                for f in sorted(fn):
                    expected_results.append(
                        join_paths(dp, f).removeprefix(tmp_dir_path))
        self.assertEqual(
            ''.join(s for s in self.iterate_aws_s3_dir_objects(recursively=True)),
            ''.join(expected_results))
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_overwrite_set_to_false(self):
        # Create a copy of the directory.
        tmp_dir_name = 'TMP_DIR'
        tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, tmp_dir_name))
        shutil.copytree(src=ABS_DIR_PATH, dst=tmp_dir_path)
        # While capturing stdout...
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo
            # Copy directory with "overwrite" set to "False".
            dir.transfer_to(
                dst=TestLocalDir.build_dir(path=tmp_dir_path),
                overwrite=False)

            sys.stdout = sys.__stdout__

            self.assertTrue("Operation unsuccessful" in stdo.getvalue())
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_overwrite_set_to_true(self):
        # Create a copy of the directory.
        tmp_dir_name = 'TMP_DIR'
        tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, tmp_dir_name))
        shutil.copytree(src=ABS_DIR_PATH, dst=tmp_dir_path)
        # While capturing stdout...
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo
            # Copy directory with "overwrite" set to "True".
            dir.transfer_to(
                dst=TestLocalDir.build_dir(path=tmp_dir_path),
                overwrite=True)

            sys.stdout = sys.__stdout__

            self.assertTrue("Operation successful" in stdo.getvalue())
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_set_to_false(self):
        # Set metadata for a directory's file.
        filename, metadata = DIR_FILE_NAME, {'1': '1'}
        with self.build_dir() as dir:
            dir.set_metadata(file_path=filename, metadata=metadata)
            # Create a temporary dictionary.
            tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, "TMP_DIR"))
            os.mkdir(tmp_dir_path)
            tmp_dir = TestLocalDir.build_dir(path=tmp_dir_path)
            # Copy the directory's contents into this
            # tmp directory without including metadata.
            dir.transfer_to(dst=tmp_dir, include_metadata=False)
        # Assert that no metadata have been transfered.
        self.assertEqual(tmp_dir.get_metadata(file_path=filename), {})
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_with_set_metadata(self):
        # Set metadata for a directory's file.
        filename, metadata = DIR_FILE_NAME, {'1': '1'}
        with self.build_dir() as dir:
            dir.set_metadata(file_path=filename, metadata=metadata)
            # Create a temporary dictionary.
            tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, "TMP_DIR"))
            os.mkdir(tmp_dir_path)
            tmp_dir = TestLocalDir.build_dir(path=tmp_dir_path)
            # Copy the directory's contents into this
            # tmp directory without including metadata.
            dir.transfer_to(dst=tmp_dir, include_metadata=True)
        # Assert the file's metadata are the same.
        self.assertEqual(
            tmp_dir.get_metadata(file_path=filename),
            metadata)
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_without_set_metadata(self):
        # Set metadata for a directory's file.
        filename = DIR_FILE_NAME
        with self.build_dir() as dir:
            # Create a temporary dictionary.
            tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, "TMP_DIR"))
            os.mkdir(tmp_dir_path)
            tmp_dir = TestLocalDir.build_dir(path=tmp_dir_path)
            # Copy the directory's contents into this
            # tmp directory without including metadata.
            dir.transfer_to(dst=tmp_dir, include_metadata=True)
        # Assert the file's metadata are the same.
        self.assertEqual(
            tmp_dir.get_metadata(file_path=filename),
            METADATA)
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_to_cloud_dir(self):
        # Set metadata for a directory's file.
        filename, metadata = DIR_FILE_NAME, {'2': '2'}
        with self.build_dir() as dir:
            dir.set_metadata(file_path=filename, metadata=metadata)
            # Copy directory contents into itself.
            dir.transfer_to(dst=dir, overwrite=True, include_metadata=True)
        # Assert that the object's metadata have been modified.
        self.assertEqual(
            get_aws_s3_object(BUCKET, REL_DIR_FILE_PATH).metadata,
            metadata)
        
    def test_traverse_files(self):
        with self.build_dir() as dir:
            self.assertEqual(
                ''.join(map(lambda file: file.get_path(), dir.traverse_files())),
                ''.join(filter(lambda path: not path.endswith('/'), self.get_abs_contents(recursively=False))))

    def test_traverse_files_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                ''.join(map(lambda file: file.get_path(), dir.traverse_files(recursively=True))),
                ''.join(self.get_abs_contents(recursively=True)))
        
    def test_get_files(self):
        with self.build_dir() as dir:
            files = dir.get_files()
            self.assertEqual(
                ''.join(map(lambda path: files[path].get_path(), files)),
                ''.join(filter(lambda path: not path.endswith('/'),
                    self.get_abs_contents(recursively=False))))

    def test_get_files_on_recursively(self):
        with self.build_dir() as dir:
            files = dir.get_files(recursively=True)
            self.assertEqual(
                ''.join(map(lambda path: files[path].get_path(), files)),
                ''.join(self.get_abs_contents(recursively=True)))
        
    def test_get_files_on_show_abs_path(self):
        with self.build_dir() as dir:
            files = dir.get_files(show_abs_path=True)
            self.assertEqual(
                ''.join(files),
                ''.join(filter(lambda path: not path.endswith('/'),
                    self.get_abs_contents(recursively=False))))
        
    def test_get_files_on_recursively_and_show_abs_path(self):
        with self.build_dir() as dir:
            files = dir.get_files(recursively=True, show_abs_path=True)
            self.assertEqual(
                ''.join(files),
                ''.join(self.get_abs_contents(recursively=True)))
        
    def test_get_file(self):
        with self.build_dir() as dir:
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            file = dir.get_file(file_path)
            self.assertEqual(file.get_path(), REL_DIR_FILE_PATH)

    def test_file_shared_metadata_on_modify_from_dir(self):
        with self.build_dir() as dir:
            # Access file via dir.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            file = dir.get_file(file_path)
            # Change metadata via "Dir" API.
            dir.set_metadata(file_path, METADATA)
            # Assert file metadata have been changed.
            self.assertEqual(file.get_metadata(), METADATA)

    def test_file_shared_metadata_on_modify_from_file(self):
        with self.build_dir() as dir:
            # Access file via dir.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            file = dir.get_file(file_path)
            # Change metadata via "File" API.
            file.set_metadata(METADATA)
            # Assert file metadata have been changed.
            self.assertEqual(dir.get_metadata(file_path), METADATA)
        
    '''
    Test connection methods.
    '''
    def test_open(self):
        dir = self.build_dir()
        dir.close()
        dir.open()
        self.assertTrue(dir._get_handler().is_open())
        dir.close()

    def test_close(self):
        dir = self.build_dir()
        dir.close()
        self.assertFalse(dir._get_handler().is_open())

    '''
    Test cache methods.
    '''
    def test_is_cachable_on_false(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.is_cacheable(), False)

    def test_is_cachable_on_true(self):
        with self.build_dir(cache=True) as dir:
            self.assertEqual(dir.is_cacheable(), True)

    def test_purge(self):
        with self.build_dir(cache=True) as dir:
            # Fetch size via HTTP.
            dir.load_metadata()
            # Fetch size from cache and time it.
            t = time.time()
            dir.load_metadata()
            cache_time = time.time() - t
            # Purge cache.
            dir.purge()
            # Re-fetch size via HTTP and time it.
            t = time.time()
            dir.load_metadata()
            normal_time = time.time() - t
            self.assertGreater(normal_time, cache_time)

    def test_traverse_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = (_ for _ in dir.traverse())
            self.assertEqual(list(dir.traverse()), CONTENTS)
            
    def test_traverse_recursively_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = (_ for _ in dir.traverse(recursively=True))
            expected_results = []
            for dp, dn, fn in os.walk(REL_DIR_PATH):
                dn.sort()
                for f in sorted(fn):
                    expected_results.append(
                        join_paths(dp, f).removeprefix(REL_DIR_PATH))
            self.assertEqual(
                ''.join([p for p in dir.traverse(recursively=True)]),
                ''.join(expected_results))
            
    def test_traverse_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch contents via HTTP.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            normal_time = time.time() - t
            # Fetch contents from cache.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_traverse_recursively_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch contents via HTTP.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            normal_time = time.time() - t
            # Fetch contents from cache.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)


    def test_load_metadata_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            dir.load_metadata()
            self.assertEqual(
                dir.get_metadata(REL_DIR_FILE_PATH),
                METADATA)

    def test_get_size_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = dir.get_size()
            self.assertEqual(dir.get_size(), 4)
            
    def test_load_metadata_recursively_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            dir.load_metadata(recursively=True)
            self.assertEqual(
                dir.get_metadata(f"{REL_DIR_PATH}subdir/file4.txt"),
                METADATA)

    def test_get_size_recursively_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = dir.get_size(recursively=True)
            self.assertEqual(dir.get_size(recursively=True), 16)

    def test_load_metadata_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch object metadata via HTTP.
            t = time.time()
            _ = dir.load_metadata()
            normal_time = time.time() - t
            # Fetch object metadata from cache.
            t = time.time()
            _ = dir.load_metadata()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_get_size_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch object size via HTTP.
            t = time.time()
            _ = dir.get_size()
            normal_time = time.time() - t
            # Fetch object size from cache.
            t = time.time()
            _ = dir.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_load_metadata_recursively_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch object metadata via HTTP.
            t = time.time()
            _ = dir.load_metadata(recursively=True)
            normal_time = time.time() - t
            # Fetch object metadata from cache.
            t = time.time()
            _ = dir.load_metadata(recursively=True)
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_get_size_recursively_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch object size via HTTP.
            t = time.time()
            _ = dir.get_size(recursively=True)
            normal_time = time.time() - t
            # Fetch object size from cache.
            t = time.time()
            _ = dir.get_size(recursively=True)
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_file_shared_cache_on_cache_via_dir(self):
        with (
            self.build_dir(cache=False) as no_cache_dir,
            self.build_dir(cache=True) as cache_dir
        ):
            # Access file via both dirs.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            no_cache_file = no_cache_dir.get_file(file_path)
            cache_file = cache_dir.get_file(file_path)
            # Count total size for both dirs.
            _ = no_cache_dir.get_size()
            _ = cache_dir.get_size()
            # Time no-cache-file's "get_size"
            t = time.time()
            _ = no_cache_file.get_size()
            normal_time = time.time() - t
            # Time cache-file's "get_size"
            t = time.time()
            _ = cache_file.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_file_shared_cache_on_cache_via_file(self):
        with (
            self.build_dir(cache=False) as no_cache_dir,
            self.build_dir(cache=True) as cache_dir
        ):
            # Count size of files via both dirs using the "File" API.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            for file in no_cache_dir.get_files().values():
                _ = file.get_size()
            for file in cache_dir.get_files().values():
                _ = file.get_size()
            # Time no-cache-dir's "get_size"
            t = time.time()
            _ = no_cache_dir.get_size()
            normal_time = time.time() - t
            # Time cache-dir's "get_size"
            t = time.time()
            _ = cache_dir.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)


class TestAzureBlobDir(unittest.TestCase):

    def get_abs_contents(self, recursively: bool):
        return [join_paths(REL_DIR_PATH, p) for p in (
            RECURSIVE_CONTENTS if recursively else CONTENTS
        )]

    def setUp(self):
        for k, v in MockContainerClient.get_mock_methods().items():
            patch(k, v).start()

    def tearDown(self):
        patch.stopall()
    
    @staticmethod
    def build_dir(
        path: str = REL_DIR_PATH,
        cache: bool = False,
        create_if_missing: bool = False,
        from_conn_string: bool = False
    ) -> AzureBlobDir:
        return AzureBlobDir(**{
            'auth': get_azure_auth_instance(from_conn_string),
            'container': CONTAINER,
            'path': path,
            'cache': cache,
            'create_if_missing': create_if_missing
        })
    
    def test_constructor(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_path(), REL_DIR_PATH)

    def test_constructor_from_conn_string(self):
        with self.build_dir(from_conn_string=True) as dir:
            self.assertEqual(dir.get_path(), REL_DIR_PATH)

    def test_constructor_on_create_if_missing(self):
        dir_path = f"{TEST_FILES_DIR}{SEPARATOR}NON_EXISTING_DIR{SEPARATOR}"
        with self.build_dir(path=dir_path, create_if_missing=True) as _:
            self.assertTrue(os.path.exists(dir_path))
            os.rmdir(dir_path)

    def test_constructor_on_invalid_path_error(self):
        self.assertRaises(InvalidPathError, self.build_dir, path="NON_EXISTING_PATH")

    def test_get_path_on_none_path(self):
        with self.build_dir(path=None) as dir:
            self.assertEqual(dir.get_path(), SEPARATOR)  

    def test_get_name(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_name(), DIR_NAME)

    def test_get_name_on_none_path(self):
        with self.build_dir(path=None) as dir:
            self.assertIsNone(dir.get_name())

    def test_get_container_name(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_container_name(), CONTAINER)   

    def test_get_uri(self):
        with self.build_dir() as dir:
            uri = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}"
            uri += f".dfs.core.windows.net/{REL_DIR_PATH}"
            self.assertEqual(dir.get_uri(), uri)

    def test_get_metadata(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_metadata(file_path='file2.txt'), {})

    def test_get_metadata_on_invalid_file_error(self):
        with self.build_dir() as dir:
            self.assertRaises(InvalidFileError, dir.get_metadata, file_path="NON_EXISTING_PATH")

    def test_set_and_get_metadata_on_relative_path(self):
        with self.build_dir() as dir:
            file_path, metadata = DIR_FILE_NAME, {'a': '1'}
            dir.set_metadata(file_path=file_path, metadata=metadata)
            self.assertEqual(dir.get_metadata(file_path=file_path), metadata)

    def test_set_and_get_metadata_on_absolute_path(self):
        with self.build_dir() as dir:
            file_path = join_paths(dir.get_path(), DIR_FILE_NAME)
            metadata = {'a': '1'}
            dir.set_metadata(file_path=file_path, metadata=metadata)
            self.assertEqual(dir.get_metadata(file_path=file_path), metadata)

    def test_set_metadata_on_non_string_metadata_key_error(self):
        with self.build_dir() as dir:
            args = {
                'file_path': DIR_FILE_NAME,
                'metadata': {1: '1'}
            }
            self.assertRaises(NonStringMetadataKeyError, dir.set_metadata, **args)

    def test_set_metadata_on_non_string_metadata_value_error(self):
        with self.build_dir() as dir:
            args = {
                'file_path': DIR_FILE_NAME,
                'metadata': {'1': 1}
            }
            self.assertRaises(NonStringMetadataValueError, dir.set_metadata, **args)

    def test_set_metadata_on_invalid_file_error(self):
        with self.build_dir() as dir:
            args = {
                'file_path': 'NON_EXISTING_FILE',
                'metadata': {'1': '1'}
            }
            self.assertRaises(InvalidFileError, dir.set_metadata, **args)

    def test_set_metadata_after_load_metadata(self):
        with self.build_dir() as dir:
            dir.load_metadata()
            new_metadata = {'a': 'a'}
            dir.set_metadata(REL_DIR_FILE_PATH, new_metadata)
            self.assertEqual(dir.get_metadata(REL_DIR_FILE_PATH), new_metadata)

    def test_load_metadata(self):
        with self.build_dir() as dir:
            dir.load_metadata()
            self.assertEqual(dir.get_metadata(REL_DIR_FILE_PATH), METADATA)

    def test_load_metadata_after_set_metadata(self):
        with self.build_dir() as dir:
            dir.set_metadata(REL_DIR_FILE_PATH, {'a': 'a'})
            dir.load_metadata()
            self.assertEqual(dir.get_metadata(REL_DIR_FILE_PATH), METADATA)

    def test_path_exists_on_abs_path(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.path_exists(REL_DIR_FILE_PATH), True)

    def test_path_exists_on_relative_path(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.path_exists(DIR_FILE_NAME), True)

    def test_path_not_exists_on_abs_path(self):
        with self.build_dir() as dir:
            file_path = join_paths(REL_DIR_PATH, 'NON_EXISTING_FILE')
            self.assertEqual(dir.path_exists(file_path), False)

    def test_path_not_exists_on_relative_path(self):
        with self.build_dir() as dir:
            file_path = 'NON_EXISTING_FILE'
            self.assertEqual(dir.path_exists(file_path), False)

    def test_get_contents(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_contents(), CONTENTS)

    def test_get_contents_on_show_abs_path(self):
        with self.build_dir() as dir:
            self.assertEqual(
                dir.get_contents(show_abs_path=True),
                self.get_abs_contents(recursively=False))
        
    def test_get_contents_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                dir.get_contents(recursively=True),
                RECURSIVE_CONTENTS)
        
    def test_get_contents_on_show_abs_path_and_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                dir.get_contents(show_abs_path=True, recursively=True),
                self.get_abs_contents(recursively=True))
            
    def test_traverse(self):
        with self.build_dir() as dir:
            self.assertEqual(list(dir.traverse()), CONTENTS)

    def test_traverse_on_show_abs_path(self):
        with self.build_dir() as dir:
            self.assertEqual(
                list(dir.traverse(show_abs_path=True)),
                self.get_abs_contents(recursively=False))
        
    def test_traverse_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                list(dir.traverse(recursively=True)),
                RECURSIVE_CONTENTS)
        
    def test_traverse_on_show_abs_path_and_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                list(dir.traverse(show_abs_path=True, recursively=True)),
                self.get_abs_contents(recursively=True))
            
    def test_ls(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls()

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(CONTENTS) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_ls_on_show_abs_path(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls(show_abs_path=True)

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(
                self.get_abs_contents(recursively=False)) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_ls_on_recursively(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls(recursively=True)

            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(RECURSIVE_CONTENTS) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_ls_on_show_abs_path_and_recursively(self):
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo

            dir.ls(show_abs_path=True, recursively=True)
            
            sys.stdout = sys.__stdout__

            ls_expected_output = '\n'.join(
                self.get_abs_contents(recursively=True)) + '\n'
            self.assertEqual(stdo.getvalue(), ls_expected_output)

    def test_count(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.count(), 2)

    def test_count_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.count(recursively=True), 3)

    def test_get_size(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_size(), 4)

    def test_get_size_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.get_size(recursively=True), 16)

    def test_transfer_to(self):
        # Create a temporary dictionary.
        tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, 'TMP_DIR'))
        os.mkdir(tmp_dir_path)
        # Copy the directory's contents into this tmp directory.
        with self.build_dir() as dir:
            dir.transfer_to(dst=LocalDir(path=tmp_dir_path))
        # Assert that the two directories contain the same contents.
        self.assertEqual(
            ''.join(s for s in sorted(os.listdir(REL_DIR_PATH)) if s.endswith('.txt')),
            ''.join(s for s in sorted(os.listdir(tmp_dir_path))))
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_recursively(self):
        # Create a temporary dictionary.
        tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, 'TMP_DIR'))
        os.mkdir(tmp_dir_path)
        # Copy the directory's contents into this tmp directory.
        with self.build_dir() as dir:
            dir.transfer_to(
                dst=LocalDir(path=tmp_dir_path),
                recursively=True)
        # Assert that the two directories contain the same contents.
        self.assertEqual(
            ''.join(join_paths(dp, f).removeprefix(REL_DIR_PATH)
                for dp, _, fn in os.walk(REL_DIR_PATH) for f in fn),
            ''.join(join_paths(dp, f).removeprefix(tmp_dir_path)
                for dp, _, fn in os.walk(tmp_dir_path) for f in fn))
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_overwrite_set_to_false(self):
        # Create a copy of the directory.
        tmp_dir_name = 'TMP_DIR'
        tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, tmp_dir_name))
        shutil.copytree(src=ABS_DIR_PATH, dst=tmp_dir_path)
        # While capturing stdout...
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo
            # Copy directory with "overwrite" set to "False".
            dir.transfer_to(
                dst=TestLocalDir.build_dir(path=tmp_dir_path),
                overwrite=False)

            sys.stdout = sys.__stdout__

            self.assertTrue("Operation unsuccessful" in stdo.getvalue())
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_overwrite_set_to_true(self):
        # Create a copy of the directory.
        tmp_dir_name = 'TMP_DIR'
        tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, tmp_dir_name))
        shutil.copytree(src=ABS_DIR_PATH, dst=tmp_dir_path)
        # While capturing stdout...
        with (
            io.StringIO() as stdo,
            self.build_dir() as dir
        ):
            sys.stdout = stdo
            # Copy directory with "overwrite" set to "True".
            dir.transfer_to(
                dst=TestLocalDir.build_dir(path=tmp_dir_path),
                overwrite=True)

            sys.stdout = sys.__stdout__

            self.assertTrue("Operation successful" in stdo.getvalue())
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_set_to_false(self):
        # Set metadata for a directory's file.
        filename, metadata = 'file2.txt', {'1': '1'}
        with self.build_dir() as dir:
            dir.set_metadata(file_path=filename, metadata=metadata)
            # Create a temporary dictionary.
            tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, 'TMP_DIR'))
            os.mkdir(tmp_dir_path)
            tmp_dir = TestLocalDir.build_dir(path=tmp_dir_path)
            # Copy the directory's contents into this
            # tmp directory without including metadata.
            dir.transfer_to(dst=tmp_dir, include_metadata=False)
        # Assert that no metadata have been transfered.
        self.assertEqual(tmp_dir.get_metadata(file_path=filename), {})
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_with_set_metadata(self):
        # Set metadata for a directory's file.
        filename, metadata = DIR_FILE_NAME, {'1': '1'}
        with self.build_dir() as dir:
            dir.set_metadata(file_path=filename, metadata=metadata)
            # Create a temporary dictionary.
            tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, 'TMP_DIR'))
            os.mkdir(tmp_dir_path)
            tmp_dir = TestLocalDir.build_dir(path=tmp_dir_path)
            # Copy the directory's contents into this
            # tmp directory without including metadata.
            dir.transfer_to(dst=tmp_dir, include_metadata=True)
        # Assert the file's metadata are the same.
        self.assertEqual(
            tmp_dir.get_metadata(file_path=filename),
            metadata)
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_without_set_metadata(self):
        # Set metadata for a directory's file.
        filename = DIR_FILE_NAME
        with self.build_dir() as dir:
            # Create a temporary dictionary.
            tmp_dir_path = to_abs(REL_DIR_PATH.replace(DIR_NAME, 'TMP_DIR'))
            os.mkdir(tmp_dir_path)
            tmp_dir = TestLocalDir.build_dir(path=tmp_dir_path)
            # Copy the directory's contents into this
            # tmp directory without including metadata.
            dir.transfer_to(dst=tmp_dir, include_metadata=True)
        # Assert the file's metadata are the same.
        self.assertEqual(
            tmp_dir.get_metadata(file_path=filename),
            METADATA)
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_transfer_to_on_include_metadata_to_cloud_dir(self):
        with mock_s3() as mocks3:
            # Create AWSS3 bucket.
            create_aws_s3_bucket(mocks3, BUCKET, METADATA)
            # Copy directory contents including metadata
            filename, metadata = DIR_FILE_NAME, {'2': '2'}
            with (
                self.build_dir() as dir,
                TestAWSS3Dir.build_dir() as aws_dir
            ):
                dir.set_metadata(file_path=filename, metadata=metadata)
                dir.transfer_to(dst=aws_dir, overwrite=True, include_metadata=True)
                # Assert that the object's metadata have been modified.
                self.assertEqual(
                    get_aws_s3_object(aws_dir.get_bucket_name(), REL_DIR_FILE_PATH).metadata,
                    metadata)
                
    def test_transfer_to_as_destination(self):
        # Create a "remote" temporary dictionary.
        tmp_dir_path = REL_DIR_PATH.replace(DIR_NAME, 'TMP_DIR')
        os.mkdir(tmp_dir_path)
        # Copy the local dir's contents into this
        # "remote" tmp directory.
        with self.build_dir(path=tmp_dir_path) as dir:
            TestLocalDir.build_dir(REL_DIR_PATH).transfer_to(dir)
        # Assert that the two directories contain the same contents.
        self.assertEqual(
            ''.join(s for s in sorted(os.listdir(REL_DIR_PATH)) if s.endswith('.txt')),
            ''.join(s for s in sorted(os.listdir(tmp_dir_path))))
        # Remove temporary directory.
        shutil.rmtree(tmp_dir_path)

    def test_traverse_files(self):
        with self.build_dir() as dir:
            self.assertEqual(
                ''.join(map(lambda file: file.get_path(), dir.traverse_files())),
                ''.join(filter(lambda path: not path.endswith('/'), self.get_abs_contents(recursively=False))))

    def test_traverse_files_on_recursively(self):
        with self.build_dir() as dir:
            self.assertEqual(
                ''.join(map(lambda file: file.get_path(), dir.traverse_files(recursively=True))),
                ''.join(self.get_abs_contents(recursively=True)))
        
    def test_get_files(self):
        with self.build_dir() as dir:
            files = dir.get_files()
            self.assertEqual(
                ''.join(map(lambda path: files[path].get_path(), files)),
                ''.join(filter(lambda path: not path.endswith('/'),
                    self.get_abs_contents(recursively=False))))

    def test_get_files_on_recursively(self):
        with self.build_dir() as dir:
            files = dir.get_files(recursively=True)
            self.assertEqual(
                ''.join(map(lambda path: files[path].get_path(), files)),
                ''.join(self.get_abs_contents(recursively=True)))
        
    def test_get_files_on_show_abs_path(self):
        with self.build_dir() as dir:
            files = dir.get_files(show_abs_path=True)
            self.assertEqual(
                ''.join(files),
                ''.join(filter(lambda path: not path.endswith('/'),
                    self.get_abs_contents(recursively=False))))
        
    def test_get_files_on_recursively_and_show_abs_path(self):
        with self.build_dir() as dir:
            files = dir.get_files(recursively=True, show_abs_path=True)
            self.assertEqual(
                ''.join(files),
                ''.join(self.get_abs_contents(recursively=True)))
        
    def test_get_file(self):
        with self.build_dir() as dir:
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            file = dir.get_file(file_path)
            self.assertEqual(file.get_path(), REL_DIR_FILE_PATH)
            
    def test_file_shared_metadata_on_modify_from_dir(self):
        with self.build_dir() as dir:
            # Access file via dir.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            file = dir.get_file(file_path)
            # Change metadata via "Dir" API.
            dir.set_metadata(file_path, METADATA)
            # Assert file metadata have been changed.
            self.assertEqual(file.get_metadata(), METADATA)

    def test_file_shared_metadata_on_modify_from_file(self):
        with self.build_dir() as dir:
            # Access file via dir.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            file = dir.get_file(file_path)
            # Change metadata via "File" API.
            file.set_metadata(METADATA)
            # Assert file metadata have been changed.
            self.assertEqual(dir.get_metadata(file_path), METADATA)
        
    '''
    Test connection methods.
    '''
    def test_open(self):
        dir = self.build_dir()
        dir.close()
        dir.open()
        self.assertTrue(dir._get_handler().is_open())
        dir.close()

    def test_close(self):
        dir = self.build_dir()
        dir.close()
        self.assertFalse(dir._get_handler().is_open())

    '''
    Test cache methods.
    '''
    def test_is_cachable_on_false(self):
        with self.build_dir() as dir:
            self.assertEqual(dir.is_cacheable(), False)

    def test_is_cachable_on_true(self):
        with self.build_dir(cache=True) as dir:
            self.assertEqual(dir.is_cacheable(), True)

    def test_purge(self):
        with self.build_dir(cache=True) as dir:
            # Fetch size via HTTP.
            dir.load_metadata()
            # Fetch size from cache and time it.
            t = time.time()
            dir.load_metadata()
            cache_time = time.time() - t
            # Purge cache.
            dir.purge()
            # Re-fetch size via HTTP and time it.
            t = time.time()
            dir.load_metadata()
            normal_time = time.time() - t
            self.assertGreater(normal_time, cache_time)

    def test_traverse_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = (_ for _ in dir.traverse())
            self.assertEqual(
                ''.join([p for p in dir.traverse()]),
                ''.join(CONTENTS))
            
    def test_traverse_recursively_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = (_ for _ in dir.traverse(recursively=True))
            expected_results = []
            for dp, dn, fn in os.walk(REL_DIR_PATH):
                dn.sort()
                for f in sorted(fn):
                    expected_results.append(
                        join_paths(dp, f).removeprefix(REL_DIR_PATH))
            self.assertEqual(
                ''.join([p for p in dir.traverse(recursively=True)]),
                ''.join(expected_results))
            
    def test_traverse_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch contents via HTTP.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            normal_time = time.time() - t
            # Fetch contents from cache.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_traverse_recursively_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch contents via HTTP.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            normal_time = time.time() - t
            # Fetch contents from cache.
            t = time.time()
            _ = (_ for _ in dir.traverse())
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)


    def test_load_metadata_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            dir.load_metadata()
            self.assertEqual(
                dir.get_metadata(REL_DIR_FILE_PATH),
                METADATA)

    def test_get_size_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = dir.get_size()
            self.assertEqual(dir.get_size(), 4)
            
    def test_load_metadata_recursively_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            dir.load_metadata(recursively=True)
            self.assertEqual(
                dir.get_metadata(f"{REL_DIR_PATH}subdir/file4.txt"),
                METADATA)

    def test_get_size_recursively_from_cache_on_value(self):
        with self.build_dir(cache=True) as dir:
            _ = dir.get_size(recursively=True)
            self.assertEqual(dir.get_size(recursively=True), 16)

    def test_load_metadata_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch object metadata via HTTP.
            t = time.time()
            _ = dir.load_metadata()
            normal_time = time.time() - t
            # Fetch object metadata from cache.
            t = time.time()
            _ = dir.load_metadata()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_get_size_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch object size via HTTP.
            t = time.time()
            _ = dir.get_size()
            normal_time = time.time() - t
            # Fetch object size from cache.
            t = time.time()
            _ = dir.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_load_metadata_recursively_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch object metadata via HTTP.
            t = time.time()
            _ = dir.load_metadata(recursively=True)
            normal_time = time.time() - t
            # Fetch object metadata from cache.
            t = time.time()
            _ = dir.load_metadata(recursively=True)
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_get_size_recursively_from_cache_on_time(self):
        with self.build_dir(cache=True) as dir:
            # Fetch object size via HTTP.
            t = time.time()
            _ = dir.get_size(recursively=True)
            normal_time = time.time() - t
            # Fetch object size from cache.
            t = time.time()
            _ = dir.get_size(recursively=True)
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_file_shared_cache_on_cache_via_dir(self):
        with (
            self.build_dir(cache=False) as no_cache_dir,
            self.build_dir(cache=True) as cache_dir
        ):
            # Access file via both dirs.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            no_cache_file = no_cache_dir.get_file(file_path)
            cache_file = cache_dir.get_file(file_path)
            # Count total size for both dirs.
            _ = no_cache_dir.get_size()
            _ = cache_dir.get_size()
            # Time no-cache-file's "get_size"
            t = time.time()
            _ = no_cache_file.get_size()
            normal_time = time.time() - t
            # Time cache-file's "get_size"
            t = time.time()
            _ = cache_file.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)

    def test_file_shared_cache_on_cache_via_file(self):
        with (
            self.build_dir(cache=False) as no_cache_dir,
            self.build_dir(cache=True) as cache_dir
        ):
            # Count size of files via both dirs using the "File" API.
            file_path = REL_DIR_FILE_PATH.removeprefix(REL_DIR_PATH)
            for file in no_cache_dir.get_files().values():
                _ = file.get_size()
            for file in cache_dir.get_files().values():
                _ = file.get_size()
            # Time no-cache-dir's "get_size"
            t = time.time()
            _ = no_cache_dir.get_size()
            normal_time = time.time() - t
            # Time cache-dir's "get_size"
            t = time.time()
            _ = cache_dir.get_size()
            cache_time = time.time() - t
            # Compare fetch times.
            self.assertGreater(normal_time, cache_time)


if __name__=="__main__":
    unittest.main()
