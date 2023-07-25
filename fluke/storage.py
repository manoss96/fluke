

__all__ = [
    'AmazonS3Dir',
    'AmazonS3File',
    'AzureBlobDir',
    'AzureBlobFile',
    'LocalFile',
    'LocalDir',
    'RemoteFile',
    'RemoteDir'
]


import os as _os
import typing as _typ
import warnings as _warn
from abc import ABC as _ABC
from abc import abstractmethod as _absmethod


from tqdm import tqdm as _tqdm


from ._helper import join_paths as _join_paths
from ._helper import infer_separator as _infer_sep
from .auth import AWSAuth as _AWSAuth
from .auth import AzureAuth as _AzureAuth
from .auth import RemoteAuth as _RemoteAuth
from ._handlers import ClientHandler as _ClientHandler
from ._handlers import FileSystemHandler as _FileSystemHandler
from ._handlers import SSHClientHandler as _SSHClientHandler
from ._handlers import AWSClientHandler as _AWSClientHandler
from ._handlers import AzureClientHandler as _AzureClientHandler
from ._exceptions import InvalidPathError as _IPE
from ._exceptions import InvalidFileError as _IFE
from ._exceptions import InvalidDirectoryError as _IDE
from ._exceptions import OverwriteError as _OverwriteError
from ._exceptions import NonStringMetadataKeyError as _NSMKE
from ._exceptions import NonStringMetadataValueError as _NSMVE
from ._exceptions import ContainerNotFoundError as _CNFE 


class _File(_ABC):
    '''
    An abstract class which serves as the \
    base class for all file-like classes.

    :param str path: A path pointing to the file.
    :param dict[str, str] metadata: A dictionary \
        containing any metadata associated with the file.
    :param ClientHandler handler: A ``ClientHandler`` class \
        instance used for interacting with the underlying handler.
    :param bool close_after_use: This value indicates whether \
        all open connections should close before the instance \
        destructor is called.
    '''

    def __init__(
        self,
        path: str,
        metadata: dict[str, str],
        handler: _ClientHandler,
        close_after_use: bool = True
    ):
        '''
        An abstract class which serves as the \
        base class for all file-like classes.

        :param str path: A path pointing to the file.
        :param dict[str, str] metadata: A dictionary \
            containing any metadata associated with the file.
        :param ClientHandler handler: A ``ClientHandler`` class \
            instance used for interacting with the underlying handler.
        :param bool close_after_use: This value indicates whether \
            all open connections should close before the instance \
            destructor is called.
        '''
        self.__path = path
        self.__metadata = metadata
        self.__separator = _infer_sep(path=path)
        self.__name = path.split(self.__separator)[-1]
        self.__handler = handler
        self.__close_after_use = close_after_use


    def get_path(self) -> str:
        '''
        Returns the file's absolute path.
        '''
        return self.__path
    

    def get_name(self) -> str:
        '''
        Returns the file's name.
        '''
        return self.__name


    def get_metadata(self) -> dict[str, str]:
        '''
        Returns a dictionary containing any \
        metadata associated with the file.
        '''
        return dict(self.__metadata)


    def set_metadata(self, metadata: dict[str, str]) -> None:
        '''
        Associates the provided metadata with \
        this file.

        :param dict[str, str] metadata: A dictionary \
            containing the metadata that are to be \
            associated with the file.

        :raises NonStringMetadataKeyError: The provided \
            metadata dictionary contains at least one \
            non-string key.
        :raises NonStringMetadataValueError: The provided \
            metadata dictionary contains at least one \
            non-string value.
        '''
        for (key, val) in metadata.items():
            if not isinstance(key, str):
                raise _NSMKE(key=key)
            if not isinstance(val, str):
                raise _NSMVE(val=val)
            
        # NOTE: Update the metadata dictionary without
        #       creating a new reference.
        self.__metadata.clear()
        for (key, val) in metadata.items():
            self.__metadata.update({ key: val })


    def get_size(self) -> int:
        '''
        Returns the file's size in bytes.
        '''
        return self._get_handler().get_file_size(
            file_path=self.get_path())
    

    def read(self) -> bytes:
        '''
        Reads and returns the file's contents in bytes.
        '''
        with self.__handler.get_reader(self.get_path()) as reader:
            return reader.read()
        

    def read_range(
        self,
        start: _typ.Optional[int],
        end: _typ.Optional[int]
    ) -> bytes:
        '''
        Returns the file contents that correspond to the \
        provided byte range.

        :param int | None start: The point in file from which \
            to begin reading bytes. If ``None``, then begin \
            reading from the start of the file.
        :param int | None end: The point in file at which \
            to stop reading bytes. If ``None``, then stop \
            reading at the end of the file.
        '''
        with self.__handler.get_reader(file_path=self.get_path()) as reader:
            return reader.read_range(start, end)
        

    def read_chunks(self, chunk_size: int = 8192) -> _typ.Iterator[bytes]:
        '''
        Returns an iterator capable of going through the file's \
        contents as distinct chunks of bytes.

        :param int chunk_size: The file chunk size in bytes. \
            Defaults to ``8192``.
        '''
        with self.__handler.get_reader(file_path=self.get_path()) as reader:
            yield from reader.read_chunks(chunk_size=chunk_size)


    def read_text(
        self,
        encoding: str = 'utf-8'
    ) -> str:
        '''
        Reads and returns the file's contents as text.

        :param str encoding: The encoding with which to decode \
            the bytes. Defaults to ``utf-8``.

        :note: See
            `Standard Encodings <https://docs.python.org/3/library/codecs.html#standard-encodings>`_ \
            for all available encodings.
        '''
        return self.read().decode(encoding)
        

    def read_lines(
        self,
        chunk_size: _typ.Optional[int] = None,
        encoding: str = 'utf-8'
    ) -> _typ.Iterator[str]:
        '''
        Returns an iterator capable of going through \
        the file line-by-line.

        :param int | None chunk_size: If not ``None``, then the file \
            is read in distinct chunks, whose size are equal to this \
            parameter value. Defaults to ``None``.
        :param str encoding: The encoding with which to decode \
            the bytes. Defaults to ``utf-8``.

        :note: See
            `Standard Encodings <https://docs.python.org/3/library/codecs.html#standard-encodings>`_ \
            for all available encodings.
        '''
        if chunk_size is None:
            for line in self.read_text(encoding).split('\n'):
                yield line
        else:
            text_temp = ''
            for chunk in self.read_chunks(chunk_size):
                text_chunks = chunk.decode(encoding).split('\n')
                if len(text_chunks) > 1:
                    yield text_temp + text_chunks[0]
                    for line in text_chunks[1:-1]:
                        yield line
                    text_temp = ''
                text_temp += text_chunks[-1]
            yield text_temp


    def transfer_to(
        self,
        dst: '_Directory',
        overwrite: bool = False,
        include_metadata: bool = False,
        chunk_size: _typ.Optional[int] = None,
        suppress_output: bool = False
    ) -> bool:
        '''
        Copies the file into the provided directory. \
        Returns ``True`` if everything went as expected, \
        else returns ``False``.

        :param _Directory dst: A ``_Directory`` class instance, \
            which represents the transfer operation's destination.
        :param bool overwrite: Indicates whether to overwrite \
            the file if it already exists. Defaults to ``False``.
        :param bool include_metadata: Indicates whether any \
            existing metadata are to be assigned to the resulting \
            file. Defaults to ``False``.
        :param int | None chunk_size: If not ``None``, then files are \
            transferred in chunks, whose size are equal to this parameter \
            value. Defaults to ``None``.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        source = self.get_uri() \
            if isinstance(self, _NonLocalFile) \
            else self.get_path()
        
        destination = dst.get_uri() \
            if isinstance(dst, _NonLocalDir) \
            else dst.get_path()
        
        if not suppress_output:
            print(f'\nTransferring file "{source}" into "{destination}".')

        dst_fp = dst._to_absolute(self.get_name(), replace_sep=True)

        try:
            # Raise an "OverwriteError" if file exists
            # in destination and "overwrite" is "False".
            if not overwrite and dst.path_exists(dst_fp):
                raise _OverwriteError(file_path=dst_fp)
            # Define metadata dictionary.
            if include_metadata:
                if custom_metadata := self.get_metadata():
                    metadata = custom_metadata
                else:
                    metadata = self.__handler.get_file_metadata(
                        file_path=self.get_path())
            else:
                metadata = None
            # Perform the file transfer.
            with (
                _tqdm(
                    disable=(
                        suppress_output or
                        chunk_size is None
                    ),
                    desc="Progress",
                    unit='bytes',
                    total=self.get_size()
                ) as progress,
                self.__handler.get_reader(
                    file_path=self.get_path()
                ) as reader,
                dst._get_handler().get_writer(
                    file_path=dst_fp,
                    metadata=metadata,
                    in_chunks=chunk_size is not None
                ) as writer
            ):
                if chunk_size is None:
                    writer.write(reader.read())
                else:
                    for chunk in reader.read_chunks(chunk_size):
                        progress.update(n=writer.write(chunk))
            # Upsert metadata to destination if not "None".
            if metadata is not None:
                dst._upsert_metadata(
                    file_path=dst_fp,
                    metadata=metadata)
            if not suppress_output:
                print("Operation successful!")
        except Exception as e:
            if not suppress_output:
                print(f"Operation unsuccessful: {e}")
            return False
          
        return True
    

    def _get_close_after_use(self) -> bool:
        '''
        Returns a value indicating whether all open connections \
        should close before the instance destructor is called.
        '''
        try:
            return self.__close_after_use
        except AttributeError:
            return True


    def _get_separator(self) -> str:
        '''
        Returns the file's path separator.
        '''
        return self.__separator
    

    def _get_handler(self) -> _ClientHandler:
        '''
        Returns this instance's ``ClientHandler``.
        '''
        return self.__handler
    

    def __new__(cls, *args, **kwargs):
        '''
        Creates an instance of this class.

        :note: This method defines field ``__handler`` \
            so that throwing an exception before invoking \
            the parent constructor does not result in a \
            second exception being thrown due to ``__del__``.
        '''
        instance = object.__new__(cls)
        instance.__handler = None
        return instance
    

    @_absmethod
    def get_uri(self) -> str:
        '''
        Returns the file's URI.
        '''
        pass


class LocalFile(_File):
    '''
    This class represents a file which resides \
    within the local file system.

    :param str path: A path pointing to the file.

    :raises InvalidPathError: The provided path \
        does not exist.
    :raises InvalidFileError: The provided path \
        points to a directory.
    '''

    def __init__(self, path: str):
        '''
        This class represents a file which resides \
        within the local file system.

        :param str path: A path pointing to the file.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path \
            points to a directory.
        '''
        if not _os.path.exists(path):
            raise _IPE(path)
        if not _os.path.isfile(path):
            raise _IFE(path)
        
        sep = _infer_sep(path=path)
        super().__init__(
            path=_os.path.abspath(path).replace(_os.sep, sep),
            metadata=dict(),
            handler=_FileSystemHandler())
        

    def get_uri(self) -> str:
        '''
        Returns the file's URI.
        '''
        return f"file:///{self.get_path().removeprefix(self._get_separator())}"


    @classmethod
    def _create_file(
        cls,
        path: str,
        handler: _FileSystemHandler,
        metadata: dict[str, str]
    ) -> 'LocalFile':
        '''
        Creates and returns a ``LocalFile`` instance.

        :param str path: The path pointing to the file.
        :param FileSystemHandler handler: A ``FileSystemHandler`` \
            class instance.
        :param dict[str, str] metadata: A dictionary containing \
            any metadata associated with the file.
        '''
        instance = cls.__new__(cls)
        _File.__init__(
            instance,
            path=path,
            metadata=metadata,
            handler=handler,
            close_after_use=False)
        return instance
    

class _NonLocalFile(_File, _ABC):
    '''
    An abstract class which serves as the base class for \
    all file-like classes that represent either remote files \
    or files in the cloud.

    :param str path: A path pointing to the file.
    :param ClientHandler handler: A ``ClientHandler`` class \
        instance used for interacting with the underlying handler.
    '''

    def __init__(
        self,
        path: str,
        handler: _ClientHandler
    ):
        '''
        An abstract class which serves as the base class for \
        all file-like classes that represent either remote files \
        or files in the cloud.

        :param str path: A path pointing to the file.
        :param ClientHandler handler: A ``ClientHandler`` class \
            instance used for interacting with the underlying handler.
        '''
        super().__init__(
            path=path,
            metadata=dict(),
            handler=handler)
        self.open()


    def is_cacheable(self) -> bool:
        '''
        Returns ``True`` if file has been defined \
        as cacheable, else returns ``False``.
        '''
        return self._get_handler().is_cacheable()


    def purge(self) -> None:
        '''
        If cacheable, then purges the file's cache, \
        else does nothing.
        '''
        self._get_handler().purge()


    def open(self) -> None:
        '''
        Opens all necessary connections.
        '''
        self._get_handler().open_connections()


    def close(self) -> None:
        '''
        Closes all open connections.
        '''
        self._get_handler().close_connections()
    

    def __enter__(self) -> '_NonLocalFile':
        '''
        Enter the runtime context related to this instance.
        '''
        return self


    def __exit__(self, exc_type, exc_value, traceback) -> None:
        '''
        Exit the runtime context related to this object. 
        '''
        self.close()


    def __del__(self) -> None:
        '''
        The class destructor method.
        '''
        if self._get_close_after_use():
            if (handler := self._get_handler()) is not None:
                # Purge cache.
                self.purge()
                # Warn if connections are open.
                if handler.is_open():
                    # Display warning.
                    msg = f'You might want to consider instantiating class "{self.__class__.__name__}"'
                    msg += " through the use of a context manager by utilizing Python's"
                    msg += ' "with" statement, or by simply invoking an instance\'s'
                    msg += ' "close" method after being done using it.'
                    _warn.warn(msg, ResourceWarning)
                    # Close connections.
                    self.close()


class RemoteFile(_NonLocalFile):
    '''
    This class represents a file which resides \
    within a remote machine's file system.

    :param RemoteAuth auth: A ``RemoteAuth`` \
        instance used for authenticating with a remote \
        machine via the SSH protocol.
    :param str path: A path pointing to the file.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access. Defaults to ``True``.

    :raises InvalidPathError: The provided path \
        does not exist.
    :raises InvalidFileError: The provided path \
        points to a directory.
    '''

    def __init__(
        self,
        auth: _RemoteAuth,
        path: str,
        cache: bool = False
    ):
        '''
        This class represents a file which resides \
        within a remote machine's file system.

        :param RemoteAuth auth: A ``RemoteAuth`` \
            instance used for authenticating with a remote \
            machine via the SSH protocol.
        :param str path: A path pointing to the file.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access. Defaults to ``False``.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path \
            points to a directory.
        '''
        # Instantiate a connection handler.
        ssh_handler = _SSHClientHandler(auth=auth, cache=cache)

        # Set up hostname.
        self.__host = auth.get_credentials()['hostname']

        super().__init__(
            path=path,
            handler=ssh_handler)

        if not ssh_handler.path_exists(path=path):
            self.close()
            raise _IPE(path)
        
        if not ssh_handler.is_file(file_path=path):
            self.close()
            raise _IFE(path)


    def get_hostname(self) -> str:
        '''
        Returns the name of the host in which \
        this fileresides.
        '''
        return self.__host


    def get_uri(self) -> str:
        '''
        Returns the file's URI.
        '''
        return f"sftp://{self.__host}/{self.get_path().removeprefix(self._get_separator())}"
    

    @classmethod
    def _create_file(
        cls,
        path: str,
        host: str,
        handler: _SSHClientHandler,
        metadata: dict[str, str]
    ) -> 'RemoteFile':
        '''
        Creates and returns a ``RemoteFile`` instance.

        :param str path: The path pointing to the file.
        :param str host: The name of the host in which \
            the file resides.
        :param SSHClientHandler handler: An ``SSHClientHandler`` \
            class instance.
        :param dict[str, str] metadata: A dictionary containing \
            any metadata associated with the file.
        '''
        instance = cls.__new__(cls)
        instance.__host = host
        _File.__init__(
            instance,
            path=path,
            metadata=metadata,
            handler=handler,
            close_after_use=False)
        return instance


    def __enter__(self) -> 'RemoteFile':
        '''
        Enter the runtime context related to this instance.
        '''
        return self


class _CloudFile(_NonLocalFile, _ABC):
    '''
    An abstract class which serves as the base class for \
    all file-like classes that represent files in the cloud.
    '''

    def load_metadata(self) -> None:
        '''
        Loads any metadata associated with the file, \
        which can then be accessed via the instance's \
        ``get_metadata`` method.

        :note: Any metadata set via the ``set_metadata`` \
            method will be overriden after invoking this \
            method.
        '''
        metadata = self._get_handler().get_file_metadata(
            self.get_path())
        self.set_metadata(metadata=metadata)


class AmazonS3File(_CloudFile):
    '''
    This class represents an object which resides \
    within an Amazon S3 bucket.

    :param AWSAuth auth: An ``AWSAuth`` instance used \
        for authenticating with AWS.
    :param str bucket: The name of the bucket in which \
        the file resides.
    :param str path: The path pointing to the file.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access.

    :raises BucketNotFoundError: The specified \
        bucket does not exist.
    :raises InvalidPathError: The provided path \
        does not exist.
    :raises InvalidFileError: The provided path \
        points to a directory.

    :note: The provided path must not begin with \
        a separator.

            * Wrong: ``/path/to/file.txt``
            * Right: ``path/to/file.txt``
    '''

    def __init__(
        self,
        auth: _AWSAuth,
        bucket: str,
        path: str,
        cache: bool = False
    ):
        '''
        This class represents an object which resides \
        within an Amazon S3 bucket.

        :param AWSAuth auth: An ``AWSAuth`` instance used \
            for authenticating with AWS.
        :param str bucket: The name of the bucket in which \
            the file resides.
        :param str path: The path pointing to the file.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access. Defaults to ``False``.

        :raises BucketNotFoundError: The specified \
            bucket does not exist.
        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path \
            points to a directory.

        :note: The provided path must not begin with \
            a separator.
                
                * Wrong: ``/path/to/file.txt``
                * Right: ``path/to/file.txt``
        '''
        # Validate path.
        sep = _infer_sep(path=path)
        if path.startswith(sep):
            raise _IPE(path=path)
        
        # Instantiate a connection handler.
        aws_handler = _AWSClientHandler(
            auth=auth,
            bucket=bucket,
            cache=cache)

        super().__init__(
            path=path,
            handler=aws_handler)

        if not aws_handler.path_exists(path=path):
            if aws_handler.dir_exists(path=path):
                self.close()
                raise _IFE(path)
            self.close()
            raise _IPE(path)     
        if not aws_handler.is_file(file_path=path):
            self.close()
            raise _IFE(path)
        

    def get_bucket_name(self) -> str:
        '''
        Returns the name of the bucket in which \
        the directory resides.
        '''
        return self._get_handler().get_bucket_name()


    def get_uri(self) -> str:
        '''
        Returns the object's URI.
        '''
        return f"s3://{self.get_bucket_name()}{self._get_separator()}{self.get_path()}"
    

    @classmethod
    def _create_file(
        cls,
        path: str,
        handler: _AWSClientHandler,
        metadata: dict[str, str]
    ) -> 'AmazonS3File':
        '''
        Creates and returns an ``AmazonS3File`` instance.

        :param str path: The path pointing to the file.
        :param AWSClientHandler handler: An ``AWSClientHandler`` \
            class instance.
        :param dict[str, str] metadata: A dictionary containing \
            any metadata associated with the file.
        '''
        instance = cls.__new__(cls)
        _File.__init__(
            instance,
            path=path,
            metadata=metadata,
            handler=handler,
            close_after_use=False)
        return instance
    

    def __enter__(self) -> 'AmazonS3File':
        '''
        Enter the runtime context related to this instance.
        '''
        return self


class AzureBlobFile(_CloudFile):
    '''
    This class represents a blob which resides \
    within an Azure blob container.

    :param AzureAuth auth: An ``AzureAuth`` instance \
        used for authenticating with Microsoft Azure.
    :param str container: The name of the container in \
        which the blob resides.
    :param str path: The path pointing to the file.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access.

    :raises ContainerNotFoundError: The \
        specified container does not exist.
    :raises InvalidPathError: The provided path \
        does not exist.
    :raises InvalidFileError: The provided path \
        points to a directory.

    :note: The provided path must not begin with \
        a separator.
            
            * Wrong: ``/path/to/file.txt``
            * Right: ``path/to/file.txt``
    '''

    def __init__(
        self,
        auth: _AzureAuth,
        container: str,
        path: str,
        cache: bool = False,
    ):
        '''
        This class represents a blob which resides \
        within an Azure blob container.

        :param AzureAuth auth: An ``AzureAuth`` instance \
            used for authenticating with Microsoft Azure.
        :param str container: The name of the container in \
            which the blob resides.
        :param str path: The path pointing to the file.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access. Defaults to ``False``.

        :raises ContainerNotFoundError: The \
            specified container does not exist.
        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path \
            points to a directory.

        :note: The provided path must not begin with \
            a separator.
                
                * Wrong: ``/path/to/file.txt``
                * Right: ``path/to/file.txt``
        '''
        # Validate path.
        sep = _infer_sep(path=path)
        if path.startswith(sep):
            raise _IPE(path=path)

        # Instantiate a connection handler.
        azr_handler = _AzureClientHandler(
            auth=auth,
            container=container,
            cache=cache)
        
        # Infer storage account.
        self.__storage_account = auth._get_storage_account()
        
        super().__init__(
            path=path,
            handler=azr_handler)
        
        # Throw an exception if container does not exist.
        if not azr_handler.container_exists():
            self.close()
            raise _CNFE(container)
        
        if not azr_handler.path_exists(path=path):
            self.close()
            raise _IPE(path)
        if not azr_handler.is_file(file_path=path):
            self.close()
            raise _IFE(path)
            

    def get_container_name(self) -> str:
        '''
        Returns the name of the bucket in which \
        the directory resides.
        '''
        return self._get_handler().get_container_name()


    def get_uri(self) -> str:
        '''
        Returns the object's URI.
        '''
        uri = f"abfss://{self.get_container_name()}@{self.__storage_account}"
        uri += f".dfs.core.windows.net/{self.get_path()}"
        return uri
    

    @classmethod
    def _create_file(
        cls,
        path: str,
        storage_account: str,
        handler: _AzureClientHandler,
        metadata: dict[str, str]
    ) -> 'AzureBlobFile':
        '''
        Creates and returns an ``AzureBlobFile`` instance.

        :param str path: The path pointing to the file.
        :param str storage_account: The name of the storage \
            account to which the blob's container belongs.
        :param AzureClientHandler handler: An ``AzureClientHandler`` \
            class instance.
        :param dict[str, str] metadata: A dictionary containing \
            any metadata associated with the file.
        '''
        instance = cls.__new__(cls)
        instance.__storage_account = storage_account
        _File.__init__(
            instance,
            path=path,
            metadata=metadata,
            handler=handler,
            close_after_use=False)
        return instance
    

    def __enter__(self) -> 'AzureBlobFile':
        '''
        Enter the runtime context related to this instance.
        '''
        return self


class _Directory(_ABC):
    '''
    An abstract class which serves as the base class \
    for all directory-like classes.

    :param str path: The path pointing to the directory.
    :param dict[str, dict[str, str]] metadata: A dictionary \
        containing file metadata.
    :param ClientHandler handler: A ``ClientHandler`` class \
        instance used for interacting with the underlying handler.
    :param bool close_after_use: This value indicates whether \
        all open connections should close before the instance \
        destructor is called.
    '''

    def __init__(
        self,
        path: str,
        metadata: dict[str, dict[str, str]],
        handler: _ClientHandler,
        close_after_use: bool = True
    ):
        '''
        An abstract class which serves as the base class \
        for all directory-like classes.

        :param str path: The path pointing to the directory.
        :param dict[str, dict[str, str]] metadata: A dictionary \
            containing file metadata.
        :param ClientHandler handler: A ``ClientHandler`` class \
            instance used for interacting with the underlying handler.
        :param bool close_after_use: This value indicates whether \
            all open connections should close before the instance \
            destructor is called.
        '''
        sep = _infer_sep(path)
        self.__path = (
            f"{path.removesuffix(sep)}{sep}"
            if path != '' else path)
        self.__name = name if (
                name := self.__path.removesuffix(sep).split(sep)[-1]
            ) != '' else None
        self.__separator = sep
        self.__handler = handler
        self.__metadata = metadata
        self.__close_after_use = close_after_use


    def get_path(self) -> str:
        '''
        Returns the path to the directory.
        '''
        return self.__path
    

    def get_name(self) -> _typ.Optional[str]:
        '''
        Returns the name of the directory, \
        or ``None`` if it's the root directory.
        '''
        return self.__name
    

    def get_metadata(self, file_path: str) -> dict[str, str]:
        '''
        Returns a dictionary containing any metadata \
            associated with the file in question.

        :param str file_path: Either the absolute path \
            or the path relative to the directory of the \
            file in question.

        :raises InvalidFileError: The provided path does \
            not point to a file within the directory.
        '''
        return dict(self._get_file_metadata_ref(file_path=file_path))


    def set_metadata(self, file_path: str, metadata: dict[str, str]) -> None:
        '''
        Associates the provided metadata with \
        the file located in the given path.

        :param str file_path: Either the absolute path \
            or the path relative to the directory of the \
            file in question.
        :param dict[str, str] metadata: A dictionary \
            containing the metadata that are to be \
            associated with the file.

        :raises NonStringMetadataKeyError: The provided \
            metadata dictionary contains at least one \
            non-string key.
        :raises NonStringMetadataValueError: The provided \
            metadata dictionary contains at least one \
            non-string value.
        :raises InvalidFileError: The provided path does \
            not point to a file within the directory.
        '''
        for (key, val) in metadata.items():
            if not isinstance(key, str):
                raise _NSMKE(key=key)
            if not isinstance(val, str):
                raise _NSMVE(val=val)

        if not (self.path_exists(file_path) and self.is_file(file_path)):
            raise _IFE(path=file_path)

        # Upsert metadata into the dictionary.
        self._upsert_metadata(file_path, metadata)


    def path_exists(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path exists \
        within the directory, else returns ``False``.

        :param str path: Either an absolute path or a \
            path relative to the directory.
        '''
        path = self._to_absolute(path, replace_sep=False)
        return self._get_handler().path_exists(path)
    

    def is_file(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path points \
        to a file, else returns ``False``.

        :param str path: Either an absolute path or a \
            path relative to the directory.
        '''
        path = self._to_absolute(path, replace_sep=False)
        return self._get_handler().is_file(path)


    def traverse(
        self,
        recursively: bool = False,
        show_abs_path: bool = False
    ) -> _typ.Iterator[str]:
        '''
        Returns an iterator capable of traversing the directory \
        by going through the paths of its contents.

        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.
        :param bool show_abs_path: Indicates whether it \
            should be displayed the absolute or the relative \
            path of the contents. Defaults to ``False``.

        :note: The resulting iterator may vary depending on the \
            value of parameter ``recursively``.
        '''
        return self._get_handler().traverse_dir(
            dir_path=self.get_path(),
            recursively=recursively,
            include_dirs=True,
            show_abs_path=show_abs_path)


    def get_contents(
        self,
        recursively: bool = False,
        show_abs_path: bool = False
    ) -> list[str]:
        '''
        Returns a list containing the paths that \
        correspond to the directory's contents.

        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.
        :param bool show_abs_path: Determines whether to include the \
            contents' absolute path or their path relative to this directory. \
            Defaults to ``False``.

        :note: The resulting list may vary depending on the value \
            of parameter ``recursively``.
        '''
        return list(self.traverse(
            recursively=recursively,
            show_abs_path=show_abs_path))
    

    def ls(self, recursively: bool = False, show_abs_path: bool = False) -> None:
        '''
        Lists the contents of the directory.

        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.
        :param bool show_abs_path: Determines whether to include the \
            contents' absolute path or their path relative to this directory. \
            Defaults to ``False``.

        :note: The resulting output may vary depending on the value \
            of parameter ``recursively``.
        '''
        for entity in self.traverse(
            recursively=recursively,
            show_abs_path=show_abs_path
        ):
            print(entity)


    def count(self, recursively: bool = False) -> int:
        '''
        Returns the total number of files within \
        within the directory.

        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.

        :note: The resulting number may vary depending on the value \
            of parameter ``recursively``.
        '''
        count = 0

        for _ in self.traverse(
            recursively=recursively,
            show_abs_path=True
        ):
            count += 1

        return count
    

    def get_size(self, recursively: bool = False) -> int:
        '''
        Returns the total sum of the sizes of all files \
        within the directory, in bytes.

        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.

        :note: The resulting size may vary depending on the value \
            of parameter ``recursively``.
        '''
        handler = self._get_handler()
        size = 0

        for file_path in handler.traverse_dir(
            dir_path=self.get_path(),
            recursively=recursively,
            include_dirs=False,
            show_abs_path=True
        ):
            size += handler.get_file_size(file_path)

        return size
    

    def transfer_to(
        self,
        dst: '_Directory',
        recursively: bool = False,
        overwrite: bool = False,
        include_metadata: bool = False,
        chunk_size: _typ.Optional[int] = None,
        filter: _typ.Optional[_typ.Callable[[str], bool]] = None,
        suppress_output: bool = False,
    ) -> bool:
        '''
        Copies all files within this directory into \
        the destination directory. Returns ``True`` if \
        all files were successfully transferred, else returns \
        ``False``.

        :param _Directory dst: A ``_Directory`` class instance, \
            which represents the transfer operation's destination.
        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.
        :param bool overwrite: Indicates whether to overwrite \
            any files if they already exist. Defaults to ``False``.
        :param bool include_metadata: Indicates whether any \
            existing metadata are to be assigned to the resulting \
            files. Defaults to ``False``.
        :param int | None chunk_size: If not ``None``, then files are \
            transferred in chunks, whose size are equal to this parameter \
            value. Defaults to ``None``.
        :param Callable[[str], bool] | None filter: A filter function \
            to be used in order to exclude certain files from being \
            transferred. This function is considered to receive a single \
            parameter, namely the file's absolute path within the source \
            directory, and to return a boolean value, based on which it is \
            determined whether the file is to be filtered out during the \
            transfer (``True``) or not (``False``). Defaults to ``None``.
        :param bool suppress_output: If set to ``True``, then \
            suppresses all output. Defaults to ``False``.
        '''
        if filter is None:
            filter = lambda _: True

        # Store the directory's files in a list.
        if not suppress_output:
            print("\nListing files to be transferred...")
        file_paths = [
            fp for fp in self.__handler.traverse_dir(
                dir_path = self.get_path(),
                recursively=recursively,
                include_dirs=False,
                show_abs_path=True)
            if filter(fp)]
        if not suppress_output:
            print("Listing operation completed.")

        total_num_files = len(file_paths)
        failures = 0
        dst_dirs = dict()

        # Iterate through all files that are to be transferred.
        for i, fp in enumerate(file_paths):

            # Define src and dst paths.
            rel_fp = self._to_relative(path=fp, replace_sep=False)
            dst_sep = dst._get_separator()
            dst_fp = (dst_sep
                .join(dst._to_absolute(path=rel_fp, replace_sep=True)
                .split(dst_sep)[:-1])
                + dst_sep)
            
            # Fetch src file and dst directory.
            src_file = self.get_file(path=fp)

            if dst_fp in dst_dirs:
                dst_dir = dst_dirs[dst_fp]
            else:
                dst_dir = dst._get_subdir_impl(dst_fp)
                dst_dirs.update({dst_fp: dst_dir})

            # Perform the transfer.
            if not src_file.transfer_to(
                dst=dst_dir,
                overwrite=overwrite,
                include_metadata=include_metadata,
                chunk_size=chunk_size,
                suppress_output=suppress_output
            ):
                failures += 1

            if not suppress_output:
                print(f"Total progress: {i+1}/{total_num_files} files.")

        if failures == 0:
            if not suppress_output:
                print(f'\nOperation successful: All {total_num_files} files were transferred!')
            return True
        else:
            if not suppress_output:
                msg = f"\nOperation unsuccessful: {failures} out of "
                msg += f"{total_num_files} files failed to be transferred."
                print(msg)
            return False
    
    
    def _get_close_after_use(self) -> bool:
        '''
        Returns a value indicating whether all open connections \
        should close before the instance destructor is called.
        '''
        try:
            return self.__close_after_use
        except AttributeError:
            return True
    

    def _get_separator(self) -> str:
        '''
        Returns the directory path's separator.
        '''
        return self.__separator
    

    def _get_handler(self) -> _ClientHandler:
        '''
        Returns this instance's ``ClientHandler``.
        '''
        return self.__handler
    

    def _get_metadata_ref(self) -> dict[str, dict[str, str]]:
        '''
        Returns a reference to the metadata dictionary.
        '''
        return self.__metadata
    

    def _get_file_metadata_ref(self, file_path: str) -> dict[str, str]:
        '''
        Returns a reference to the metadata dictionary \
        that corresponds to the specified file. If said \
        dictionary doesn't exist, then this method creates it \
        and returns it.

        :param str file_path: Either the absolute path \
            or the path relative to the directory of the \
            file in question.

        :raises InvalidFileError: The provided path does \
            not point to a file within the directory.
        '''
        if not (self.path_exists(file_path) and self.is_file(file_path)):
            raise _IFE(path=file_path)
        
        abs_path = self._to_absolute(path=file_path, replace_sep=False)
        
        if abs_path not in self.__metadata:
            self.__metadata.update({abs_path: dict()})

        return self.__metadata.get(abs_path)
    

    def _to_relative(self, path: str, replace_sep: bool) -> str:
        '''
        Transforms the provided path so that it is \
        relative to the directory.

        :param str path: The provided path.
        :param bool replace_sep: Indicates whether \
            to replace the provided path's separator \
            with the separator used by this directory.
        '''
        if replace_sep:
            sep = _infer_sep(path)
            path = path.replace(sep, self._get_separator())
        return path.removeprefix(self.__path)
    

    def _to_absolute(self, path: str, replace_sep: bool) -> str:
        '''
        Transforms the provided path so that it is \
        absolute.

        :param str path: The provided path.
        :param bool replace_sep: Indicates whether \
            to replace the provided path's separator \
            with the separator used by this directory.
        '''
        path = self._to_relative(path, replace_sep=False)
        path = _join_paths(self._get_separator(), self.__path, path)
        if replace_sep:
            sep = _infer_sep(path)
            path = path.replace(sep, self._get_separator())
        return path
            

    def _upsert_metadata(self, file_path: str, metadata: dict[str, str]) -> None:
        '''
        Updates the metadata dictionary by \
        upserting the provided metadata.

        :param str file_path: Either the absolute path \
            or the path relative to the directory of the \
            file in question.
        :param dict[str, str] metadata: A dictionary \
            containing the metadata that are to be \
            associated with the file.
        '''
        abs_path = self._to_absolute(path=file_path, replace_sep=False)

        if abs_path not in self.__metadata:
            self.__metadata.update({abs_path: dict()})

        # NOTE: Update the metadata dictionary without
        #       creating a new reference.
        self.__metadata[abs_path].clear()

        for (key, val) in metadata.items():
            self.__metadata[abs_path].update({ key: val })
    

    def __new__(cls, *args, **kwargs):
        '''
        Creates an instance of this class.

        :note: This method defines field ``__handler`` \
            so that throwing an exception before invoking \
            the parent constructor does not result in a \
            second exception being thrown due to ``__del__``.
        '''
        instance = object.__new__(cls)
        instance.__handler = None
        return instance
    

    @_absmethod
    def get_uri(self) -> str:
        '''
        Returns the directory's URI.
        '''
        pass


    @_absmethod
    def get_file(self, path: str) -> '_File':
        '''
        Returns the file residing in the specified \
        path as a ``_File`` instance.

        :param str path: Either the absolute path or the \
            path relative to the directory of the file in \
            question.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path does \
            not point to a file within the directory.
        '''
        pass

    
    @_absmethod
    def get_subdir(self, path: str) -> '_Directory':
        '''
        Returns the directory residing in the specified \
        path as a ``_Directory`` instance.

        :param str path: Either the absolute path or the \
            path relative to the directory of the subdirectory \
            in question.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidDirectoryError: The provided path \
            does not point to a subdirectory within the \
            directory.
        '''
        pass


    @_absmethod
    def _get_subdir_impl(self, dir_path: str) -> '_Directory':
        '''
        Returns the directory residing in the specified \
        path as a ``_Directory`` instance.

        :param str dir_path: Either the absolute path \
            or the path relative to the directory of the \
            subdirectory in question.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidDirectoryError: The provided path \
            does not point to a subdirectory within the \
            directory.
        '''
        pass


class LocalDir(_Directory):
    '''
    This class represents a directory which resides \
    within the local file system.

    :param str path: The path pointing to the directory.
    :param bool create_if_missing: If set to ``True``, then the directory \
        to which the provided path points will be automatically created \
        in case it does not already exist, instead of an exception being \
        thrown. Defaults to ``False``.

    :raises InvalidPathError: The provided path \
        does not exist.
    :raises InvalidDirectoryError: The provided path \
        does not point to a directory.
    '''

    def __init__(
        self,
        path: str,
        create_if_missing: bool = False
    ):
        '''
        This class represents a directory which resides \
        within the local file system.

        :param str path: The path pointing to the directory.
        :param bool create_if_missing: If set to ``True``, then the directory \
            to which the provided path points will be automatically created \
            in case it does not already exist, instead of an exception being \
            thrown. Defaults to ``False``.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidDirectoryError: The provided path \
            does not point to a directory.
        '''
        if not _os.path.exists(path):
            if create_if_missing:
                _os.makedirs(path)
            else:
                raise _IPE(path)
        elif not _os.path.isdir(path):
            raise _IDE(path)

        sep = _infer_sep(path=path)

        super().__init__(
            path=f"{_os.path.abspath(path).replace(_os.sep, sep).removesuffix(sep)}{sep}",
            metadata=dict(),
            handler=_FileSystemHandler())


    def get_uri(self) -> str:
        '''
        Returns the directory's URI.
        '''
        sep = self._get_separator()
        return f"file:///{self.get_path().removeprefix(sep)}"


    def get_file(self, path: str) -> LocalFile:
        '''
        Returns the file residing in the specified \
        path as a ``LocalFile`` instance.

        :param str path: Either the absolute path or the \
            path relative to the directory of the file in \
            question.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path does \
            not point to a file within the directory.
        '''
        if not self.path_exists(path):
            raise _IPE(path=path)
        if not self.is_file(path):
            raise _IFE(path=path)
        
        path = self._to_absolute(path=path, replace_sep=False)
        return LocalFile._create_file(
            path=path,
            handler=self._get_handler(),
            metadata=self._get_file_metadata_ref(path))
    

    def get_subdir(self, path: str) -> 'LocalDir':
        '''
        Returns the directory residing in the specified \
        path as a ``LocalDir`` instance.

        :param str path: Either the absolute path or the \
            path relative to the directory of the subdirectory \
            in question.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidDirectoryError: The provided path \
            does not point to a subdirectory within the \
            directory.
        '''

        if not self.path_exists(path):
            raise _IPE(path=path)
        if self.is_file(path):
            raise _IDE(path=path)
        
        return self._get_subdir_impl(path)
    

    @classmethod
    def _create_dir(
        cls,
        path: str,
        handler: _FileSystemHandler,
        metadata: dict[str, str]
    ) -> 'LocalDir':
        '''
        Creates and returns a ``LocalDir`` instance.

        :param str path: The path pointing to the directory.
        :param FileSystemHandler handler: A ``FileSystemHandler`` \
            class instance.
        :param dict[str, str] metadata: A dictionary containing \
            file metadata.
        '''
        instance = cls.__new__(cls)
        _Directory.__init__(
            instance,
            path=path,
            metadata=metadata,
            handler=handler,
            close_after_use=False)
        path = instance.get_path()
        return instance
    

    def _get_subdir_impl(self, dir_path: str) -> 'LocalDir':
        '''
        Returns the directory residing in the specified \
        path as a ``LocalDir`` instance.

        :param str dir_path: Either the absolute path \
            or the path relative to the directory of the \
            subdirectory in question.
        '''        
        sep = self._get_separator()
        dir_path = f"{dir_path.removesuffix(sep)}{sep}"
        dir_path = self._to_absolute(
            path=dir_path, replace_sep=False)
        
        return __class__._create_dir(
            path=dir_path,
            handler=self._get_handler(),
            metadata=self._get_metadata_ref())


class _NonLocalDir(_Directory, _ABC):
    '''
    An abstract class which serves as the base class for \
    all directory-like classes that represent either remote \
    directories or directories in the cloud.

    :param str path: The path pointing to the directory.
    :param ClientHandler handler: A ``ClientHandler`` class \
        instance used for interacting with the underlying handler.
    '''
    def __init__(
        self,
        path: str,
        handler: _ClientHandler
    ):
        '''
        An abstract class which serves as the base class for \
        all directory-like classes that represent either remote \
        directories or directories in the cloud.

        :param str path: The path pointing to the directory.
        :param ClientHandler handler: A ``ClientHandler`` class \
            instance used for interacting with the underlying handler.
        '''
        super().__init__(path=path, metadata=dict(), handler=handler)
        self.open()


    def is_cacheable(self) -> bool:
        '''
        Returns ``True`` if directory has been \
        defined as cacheable, else returns ``False``.
        '''
        return self._get_handler().is_cacheable()


    def purge(self) -> None:
        '''
        If cacheable, then purges the directory's cache, \
        else does nothing.
        '''
        self._get_handler().purge()


    def open(self) -> None:
        '''
        Opens all necessary connections.
        '''
        self._get_handler().open_connections()


    def close(self) -> None:
        '''
        Closes all open connections.
        '''
        self._get_handler().close_connections()


    def __enter__(self) -> '_NonLocalDir':
        '''
        Enter the runtime context related to this instance.
        '''
        return self


    def __exit__(self, exc_type, exc_value, traceback) -> None:
        '''
        Exit the runtime context related to this object. 
        '''
        self.close()


    def __del__(self) -> None:
        '''
        The class destructor method.
        '''
        if self._get_close_after_use():
            if (handler := self._get_handler()) is not None:
                # Purge cache.
                self.purge()
                # Warn if connections are open.
                if handler.is_open():
                    # Display warning.
                    msg = f'You might want to consider instantiating class "{self.__class__.__name__}"'
                    msg += " through the use of a context manager by utilizing Python's"
                    msg += ' "with" statement, or by simply invoking an instance\'s'
                    msg += ' "close" method after being done using it.'
                    _warn.warn(msg, ResourceWarning)
                    # Close connections.
                    self.close()


class RemoteDir(_NonLocalDir):
    '''
    This class represents a directory which resides \
    within a remote file system.

    :param RemoteAuth auth: A ``RemoteAuth`` \
        instance used for authenticating with a remote \
        machine via the SSH protocol.
    :param str path: The path pointing to the directory.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access. Defaults to ``False``.
    :param bool create_if_missing: If set to ``True``, then the directory \
        to which the provided path points will be automatically created \
        in case it does not already exist, instead of an exception being \
        thrown. Defaults to ``False``.

    :raises InvalidPathError: The provided path \
        does not exist.
    :raises InvalidDirectoryError: The provided path \
        does not point to a directory.
    '''
    def __init__(
        self,
        auth: _RemoteAuth,
        path: str,
        cache: bool = False,
        create_if_missing: bool = False
    ):
        '''
        This class represents a directory which resides \
        within a remote file system.

        :param RemoteAuth auth: A ``RemoteAuth`` \
            instance used for authenticating with a remote \
            machine via the SSH protocol.
        :param str path: The path pointing to the directory.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access. Defaults to ``True``.
        :param bool create_if_missing: If set to ``True``, then the directory \
            to which the provided path points will be automatically created \
            in case it does not already exist, instead of an exception being \
            thrown. Defaults to ``False``.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidDirectoryError: The provided path \
            does not point to a directory.
        '''        
        ssh_handler = _SSHClientHandler(auth=auth, cache=cache)
        self.__host = auth.get_credentials()['hostname']

        super().__init__(
            path=path,
            handler=ssh_handler)
        
        # Validate path.
        if path == '':
            self.close()
            raise _IPE(path=path)

        if not ssh_handler.path_exists(path=path):
            if create_if_missing:
                ssh_handler.mkdir(path=path)
            else:
                self.close()
                raise _IPE(path)
        if ssh_handler.is_file(file_path=path):
            self.close()
            raise _IDE(path)


    def get_hostname(self) -> str:
        '''
        Returns the name of the host in which \
        this directory resides.
        '''
        return self.__host


    def get_uri(self) -> str:
        '''
        Returns the directory's URI.
        '''
        return f"sftp://{self.__host}/{self.get_path().removeprefix(self._get_separator())}"
    

    def get_file(self, path: str) -> RemoteFile:
        '''
        Returns the file residing in the specified \
        path as a ``RemoteFile`` instance.

        :param str path: Either the absolute path or the \
            path relative to the directory of the file in \
            question.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path does \
            not point to a file within the directory.
        '''
        if not self.path_exists(path):
            raise _IPE(path=path)
        if not self.is_file(path):
            raise _IFE(path=path)
        
        path = self._to_absolute(path, replace_sep=False)
        return RemoteFile._create_file(
            path=path,
            host=self.get_hostname(),
            handler=self._get_handler(),
            metadata=self._get_file_metadata_ref(path))
    

    def get_subdir(self, path: str) -> 'RemoteDir':
        '''
        Returns the subdirectory residing in the specified \
        path as a ``RemoteDir`` instance.

        :param str path: Either the absolute path or the \
            path relative to the directory of the subdirectory \
            in question.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidDirectoryError: The provided path \
            does not point to a subdirectory within the \
            directory.
        '''
        if not self.path_exists(path):
            raise _IPE(path=path)
        if self.is_file(path):
            raise _IDE(path=path)
        
        return self._get_subdir_impl(path)
    

    @classmethod
    def _create_dir(
        cls,
        path: str,
        host: str,
        handler: _SSHClientHandler,
        metadata: dict[str, str]
    ) -> 'RemoteDir':
        '''
        Creates and returns a ``LocalDir`` instance.

        :param str path: The path pointing to the directory.
        :param str host: The name of the host in which \
            the directory resides.
        :param SSHClientHandler handler: An ``SSHClientHandler`` \
            class instance.
        :param dict[str, str] metadata: A dictionary containing \
            file metadata.
        '''
        instance = cls.__new__(cls)
        instance.__host = host
        _Directory.__init__(
            instance,
            path=path,
            metadata=metadata,
            handler=handler,
            close_after_use=False)
        return instance
    

    def _get_subdir_impl(self, dir_path: str) -> 'RemoteDir':
        '''
        Returns the directory residing in the specified \
        path as a ``RemoteDir`` instance.

        :param str dir_path: Either the absolute path \
            or the path relative to the directory of the \
            subdirectory in question.
        '''        
        sep = self._get_separator()
        dir_path = f"{dir_path.removesuffix(sep)}{sep}"
        dir_path = self._to_absolute(
            path=dir_path, replace_sep=False)
        
        return __class__._create_dir(
            path=dir_path,
            host=self.get_hostname(),
            handler=self._get_handler(),
            metadata=self._get_metadata_ref())
    

    def __enter__(self) -> 'RemoteDir':
        '''
        Enter the runtime context related to this instance.
        '''
        return super().__enter__()


class _CloudDir(_NonLocalDir, _ABC):
    '''
    An abstract class which serves as the base class for \
    all directory-like classes that represent directories \
    in the cloud.
    '''

    def load_metadata(self, recursively: bool = False) -> None:
        '''
        Loads any metadata associated with the files \
        within the directory, which can then be accessed \
        via the instance's ``get_metadata`` method.

        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.

        :note: 
            - The number of the loaded metadata may vary depending \
              on the value of parameter ``recursively``.
            - Any metadata set via the ``set_metadata`` \
              method will be overriden after invoking this \
              method.
        '''
        handler = self._get_handler()

        for file_path in handler.traverse_dir(
            dir_path=self.get_path(),
            recursively=recursively,
            include_dirs=False,
            show_abs_path=True
        ):
            metadata = handler.get_file_metadata(file_path)
            self.set_metadata(file_path, metadata)


class AmazonS3Dir(_CloudDir):
    '''
    This class represents a virtual directory which resides \
    within an Amazon S3 bucket.

    :param AWSAuth auth: An ``AWSAuth`` instance used \
        for authenticating with AWS.
    :param str bucket: The name of the bucket in which \
        the directory resides.
    :param str | None path: The path pointing to the directory. \
        If ``None``, then the whole bucket is considered. \
        Defaults to ``None``.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access. Defaults to ``False``.
    :param bool create_if_missing: If set to ``True``, then the directory \
        to which the provided path points will be automatically created \
        in case it does not already exist, instead of an exception being \
        thrown. Defaults to ``False``.

    :raises BucketNotFoundError: The specified \
        bucket does not exist.
    :raises InvalidPathError: The provided path \
        does not exist.

    :note: The provided path must not begin with \
        a separator.
            
            * Wrong: ``/path/to/dir``
            * Right: ``path/to/dir``
    '''
    def __init__(
        self,
        auth: _AWSAuth,
        bucket: str,
        path: _typ.Optional[str] = None,
        cache: bool = False,
        create_if_missing: bool = False
    ):
        '''
        This class represents a virtual directory which resides \
        within an Amazon S3 bucket.

        :param AWSAuth auth: An ``AWSAuth`` instance used \
            for authenticating with AWS.
        :param str bucket: The name of the bucket in which \
            the directory resides.
        :param str | None path: The path pointing to the directory. \
            If ``None``, then the whole bucket is considered. \
            Defaults to ``None``.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access. Defaults to ``True``.
        :param bool create_if_missing: If set to ``True``, then the directory \
            to which the provided path points will be automatically created \
            in case it does not already exist, instead of an exception being \
            thrown. Defaults to ``False``.

        :raises BucketNotFoundError: The specified \
            bucket does not exist.
        :raises InvalidPathError: The provided path \
            does not exist.

        :note: The provided path must not begin with \
            a separator.
                
                * Wrong: ``/path/to/dir``
                * Right: ``path/to/dir``
        '''
        # Validate path.
        if path is None:
            path = ''
        else:
            sep = _infer_sep(path=path)
            if path.startswith(sep):
                raise _IPE(path=path)
            
        # Instantiate a connection handler.
        aws_handler = _AWSClientHandler(
            auth=auth,
            bucket=bucket,
            cache=cache)

        super().__init__(
            path=path,
            handler=aws_handler)

        # Create directory or throw an exception
        # depending on the value of "create_if_missing".
        if path != '':
            if not aws_handler.dir_exists(path=path):
                if create_if_missing:
                    aws_handler.mkdir(path=path)
                else:
                    self.close()
                    raise _IPE(path)


    def get_bucket_name(self) -> str:
        '''
        Returns the name of the bucket in which \
        the directory resides.
        '''
        return self._get_handler().get_bucket_name()


    def get_uri(self) -> str:
        '''
        Returns the directory's URI.
        '''
        return f"s3://{self.get_bucket_name()}/{self.get_path()}"
    

    def get_file(self, path: str) -> AmazonS3File:
        '''
        Returns the file residing in the specified \
        path as a ``AmazonS3File`` instance.

        :param str path: Either the absolute path or the \
            path relative to the directory of the file in \
            question.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path does \
            not point to a file within the directory.

        :note: The provided path, if absolute, must not \
            begin with a separator.

                * Wrong: ``/path/to/file.txt``
                * Right: ``path/to/file.txt``
        '''
        if not self.path_exists(path):
            if self._get_handler().dir_exists(
                self._to_absolute(path, False)
            ):
                raise _IFE(path=path)
            raise _IPE(path=path)
        
        path = self._to_absolute(path, replace_sep=False)
        return AmazonS3File._create_file(
            path=path,
            handler=self._get_handler(),
            metadata=self._get_file_metadata_ref(path))
    

    def get_subdir(self, path: str) -> 'AmazonS3Dir':
        '''
        Returns the directory residing in the specified \
        path as an ``AmazonS3Dir`` instance.

        :param str path: Either the absolute path or the \
            path relative to the directory of the subdirectory \
            in question.

        :raises InvalidPathError: The provided path \
            does not exist.

        :note: The provided path, if absolute, must not \
            begin with a separator.

                * Wrong: ``/path/to/dir``
                * Right: ``path/to/dir``
        '''
        sep = self._get_separator()
        path = f"{path.removesuffix(sep)}{sep}"
        abs_path = self._to_absolute(
            path=path, replace_sep=False)

        if not self._get_handler().dir_exists(abs_path):
            raise _IPE(path=path)
        
        return self._get_subdir_impl(path)


    @classmethod
    def _create_dir(
        cls,
        path: str,
        handler: _AWSClientHandler,
        metadata: dict[str, str]
    ) -> 'AmazonS3Dir':
        '''
        Creates and returns an ``AmazonS3Dir`` instance.

        :param str path: The path pointing to the directory.
        :param AWSClientHandler handler: An ``AWSClientHandler`` \
            class instance.
        :param dict[str, str] metadata: A dictionary containing \
            file metadata.
        '''
        instance = cls.__new__(cls)
        _Directory.__init__(
            instance,
            path=path,
            metadata=metadata,
            handler=handler,
            close_after_use=False)
        return instance
    

    def _get_subdir_impl(self, dir_path: str) -> 'AmazonS3Dir':
        '''
        Returns the directory residing in the specified \
        path as an ``AmazonS3Dir`` instance.

        :param str dir_path: Either the absolute path \
            or the path relative to the directory of the \
            subdirectory in question.
        '''
        sep = self._get_separator()
        dir_path = f"{dir_path.removesuffix(sep)}{sep}"
        abs_dir_path = self._to_absolute(
            path=dir_path, replace_sep=False)
        
        return __class__._create_dir(
            path=abs_dir_path,
            handler=self._get_handler(),
            metadata=self._get_metadata_ref())
    

    def __enter__(self) -> 'AmazonS3Dir':
        '''
        Enter the runtime context related to this instance.
        '''
        return super().__enter__()


class AzureBlobDir(_CloudDir):
    '''
    This class represents a virtual directory which resides \
    within an Azure blob container.

    :param AzureAuth auth: An ``AzureAuth`` instance used \
        for authenticating with Microsoft Azure.
    :param str container: The name of the container in which \
        the directory resides.
    :param str | None path: The path pointing to the directory. \
        If ``None``, then the whole container is considered. \
        Defaults to ``None``.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access. Defaults to ``False``.
    :param bool create_if_missing: If set to ``True``, then the directory \
        to which the provided path points will be automatically created \
        in case it does not already exist, instead of an exception being \
        thrown. Defaults to ``False``.

    :raises ContainerNotFoundError: The \
        specified container does not exist.
    :raises InvalidPathError: The provided path \
        does not exist.

    :note: The provided path must not begin with \
        a separator.
            
            * Wrong: ``/path/to/dir``
            * Right: ``path/to/dir``
    '''
    def __init__(
        self,
        auth: _AzureAuth,
        container: str,
        path: _typ.Optional[str] = None,
        cache: bool = False,
        create_if_missing: bool = False
    ):
        '''
        This class represents a virtual directory which resides \
        within an Azure blob container.

        :param AzureAuth auth: An ``AzureAuth`` instance used \
            for authenticating with Microsoft Azure.
        :param str container: The name of the container in which \
            the directory resides.
        :param str | None path: The path pointing to the directory. \
            If ``None``, then the whole container is considered. \
            Defaults to ``None``.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access. Defaults to ``True``.
        :param bool create_if_missing: If set to ``True``, then the directory \
            to which the provided path points will be automatically created \
            in case it does not already exist, instead of an exception being \
            thrown. Defaults to ``False``.

        :raises ContainerNotFoundError: The \
            specified container does not exist.
        :raises InvalidPathError: The provided path \
            does not exist.

        :note: The provided path must not begin with \
            a separator.
                
                * Wrong: ``/path/to/dir``
                * Right: ``path/to/dir``
        '''        
        # Validate path.
        if path is None:
            path = ''
        else:
            sep = _infer_sep(path=path)
            if path.startswith(sep):
                raise _IPE(path=path)
            
        # Instantiate a connection handler.
        azr_handler = _AzureClientHandler(
            auth=auth,
            cache=cache,
            container=container)
        
        # Infer storage account.
        self.__storage_account = auth._get_storage_account()

        super().__init__(
            path=path,
            handler=azr_handler)

        # Throw an exception if container does not exist.
        if not azr_handler.container_exists():
            self.close()
            raise _CNFE(container)

        # Create directory or throw an exception
        # depending on the value of "create_if_missing".
        if path != '':
            if not azr_handler.path_exists(path=path):
                if create_if_missing:
                    azr_handler.mkdir(path=path)
                else:
                    self.close()
                    raise _IPE(path)


    def get_container_name(self) -> str:
        '''
        Returns the name of the bucket in which \
        the directory resides.
        '''
        return self._get_handler().get_container_name()


    def get_uri(self) -> str:
        '''
        Returns the directory's URI.
        '''
        uri = f"abfss://{self.get_container_name()}@{self.__storage_account}"
        uri += f".dfs.core.windows.net/{self.get_path()}"
        return uri
    

    def get_file(self, path: str) -> AzureBlobFile:
        '''
        Returns the file residing in the specified \
        path as a ``AzureBlobFile`` instance.

        :param str path: Either the absolute path or the \
            path relative to the directory of the file in \
            question.

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path does \
            not point to a file within the directory.

        :note: The provided path, if absolute, must not \
            begin with a separator.

                * Wrong: ``/path/to/file.txt``
                * Right: ``path/to/file.txt``
        '''
        if not self.path_exists(path):
            raise _IPE(path=path)
        if not self.is_file(path):
            raise _IFE(path=path)
        
        path = self._to_absolute(path, replace_sep=False)
        return AzureBlobFile._create_file(
            path=path,
            storage_account=self.__storage_account,
            handler=self._get_handler(),
            metadata=self._get_file_metadata_ref(path))
    

    def get_subdir(self, path: str) -> 'AzureBlobDir':
        '''
        Returns the directory residing in the specified \
        path as an ``AzureBlobDir`` instance.

        :param str path: Either the absolute path or the \
            path relative to the directory of the subdirectory \
            in question.

        :raises InvalidPathError: The provided path \
            does not exist.

        :note: The provided path, if absolute, must not \
            begin with a separator.

                * Wrong: ``/path/to/dir``
                * Right: ``path/to/dir``
        '''
        sep = self._get_separator()
        path = f"{path.removesuffix(sep)}{sep}"

        if not self.path_exists(path):
            raise _IPE(path=path)
        
        return self._get_subdir_impl(path)
    

    @classmethod
    def _create_dir(
        cls,
        path: str,
        storage_account: str,
        handler: _AzureClientHandler,
        metadata: dict[str, str]
    ) -> 'AzureBlobDir':
        '''
        Creates and returns an ``AzureBlobDir`` instance.

        :param str path: The path pointing to the directory.
        :param str storage_account: The name of the storage \
            account to which the blob directory's container \
            belongs.
        :param AzureClientHandler handler: An ``AzureClientHandler`` \
            class instance.
        :param dict[str, str] metadata: A dictionary containing \
            file metadata.
        '''
        instance = cls.__new__(cls)
        instance.__storage_account = storage_account
        _Directory.__init__(
            instance,
            path=path,
            metadata=metadata,
            handler=handler,
            close_after_use=False)
        return instance
    

    def _get_subdir_impl(self, dir_path: str) -> 'AzureBlobDir':
        '''
        Returns the directory residing in the specified \
        path as an ``AzureBlobDir`` instance.

        :param str dir_path: Either the absolute path \
            or the path relative to the directory of the \
            subdirectory in question.
        '''
        sep = self._get_separator()
        dir_path = f"{dir_path.removesuffix(sep)}{sep}"
        
        dir_path = self._to_absolute(
            path=dir_path, replace_sep=False)
        
        return __class__._create_dir(
            path=dir_path,
            storage_account=self.__storage_account,
            handler=self._get_handler(),
            metadata=self._get_metadata_ref())

    
    def __enter__(self) -> 'AzureBlobDir':
        '''
        Enter the runtime context related to this instance.
        '''
        return super().__enter__()
