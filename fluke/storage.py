

__all__ = [
    'AWSS3Dir',
    'AWSS3File',
    'AzureBlobDir',
    'AzureBlobFile',
    'LocalFile',
    'LocalDir',
    'RemoteFile',
    'RemoteDir'
]


import os as _os
import io as _io
import typing as _typ
import warnings as _warn
from abc import ABC as _ABC
from abc import abstractmethod as _absmethod


import paramiko as _prmk
from tqdm import tqdm as _tqdm
from azure.storage.blob import ContainerClient as _ContainerClient


from ._helper import join_paths as _join_paths
from ._helper import infer_separator as _infer_sep
from ._errors import Error as _Error
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
from ._exceptions import AzureBlobContainerNotFoundError as _ABCNFE 
from ._ingesters import Ingester as _Ingester
from ._ingesters import LocalIngester as _LocalIngester
from ._ingesters import RemoteIngester as _RemoteIngester
from ._ingesters import AWSS3Ingester as _AWSS3Ingester
from ._ingesters import AzureIngester as _AzureIngester


class _File(_ABC):
    '''
    An abstract class which serves as the \
    base class for all file-like classes.

    :param str path: A path pointing to the file.
    :param Ingester ingester: An ``Ingester`` class instance.
    '''

    def __init__(self, path: str, handler: _ClientHandler, ingester: _Ingester):
        '''
        An abstract class which serves as the \
        base class for all file-like classes.

        :param str path: A path pointing to the file.
        :param Ingester ingester: An ``Ingester`` class instance.
        '''
        self.__path = path
        self.__separator = _infer_sep(path=path)
        self.__handler = handler
        self.__ingester = ingester
        self.__name = path.split(self.__separator)[-1]
        self.__metadata = dict()


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
        self.__metadata = dict(metadata)


    def get_size(self) -> int:
        '''
        Returns the file's size in bytes.
        '''
        self._get_handler().get_file_size(self.get_path())
    

    def _get_handler(self) -> _ClientHandler:
        '''
        Returns this instance's ``ClientHandler``.
        '''
        return self.__handler


    def _get_ingester(self) -> _Ingester:
        '''
        Returns the instance's ingester.
        '''
        return self.__ingester
    

    def _get_separator(self) -> str:
        '''
        Returns the file's path separator.
        '''
        return self.__separator
    

    def __new__(
        cls,
        handler: _typ.Optional[_ClientHandler] = None,
        *args,
        **kwargs
    ) -> 'LocalFile':
        '''
        Creates an instance of this class.

        :param Any | None handler: A ``ClientHandler`` instance. \
            Defaults to ``None``.

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


    @_absmethod
    def read(self) -> bytes:
        '''
        Reads and returns the file's bytes.
        Returns ``None`` if something goes wrong.
        '''
        pass


    @_absmethod
    def transfer_to(
        self,
        dst: '_Directory',
        overwrite: bool = False,
        include_metadata: bool = False
    ) -> None:
        '''
        Copies the file into the provided directory.

        :param _Directory dst: A ``_Directory`` class instance, \
            which represents the transfer operation's destination.
        :param bool overwrite: Indicates whether to overwrite \
            the file if it already exists. Defaults to ``False``.
        :param bool include_metadata: Indicates whether any \
            existing metadata are to be assigned to the resulting \
            file. Defaults to ``False``.

        :raises OverwriteError: File already exists while parameter \
            ``overwrite`` has been set to ``False``.
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
            handler=_FileSystemHandler(),
            ingester=_LocalIngester())
        

    def get_uri(self) -> str:
        '''
        Returns the file's URI.
        '''
        return f"file:///{self.get_path().lstrip(self._get_separator())}"
    

    def read(self) -> bytes:
        '''
        Reads and returns the file's bytes.
        Returns ``None`` if something goes wrong.
        '''
        with open(file=self.get_path(), mode='rb') as file:
            return file.read()
    

    def transfer_to(
        self,
        dst: '_Directory',
        overwrite: bool = False,
        include_metadata: bool = False
    ) -> None:
        '''
        Copies the file into the provided directory.

        :param _Directory dst: A ``_Directory`` class instance, \
            which represents the transfer operation's destination.
        :param bool overwrite: Indicates whether to overwrite \
            the file if it already exists. Defaults to ``False``.
        :param bool include_metadata: Indicates whether any \
            existing metadata are to be assigned to the resulting \
            file. Defaults to ``False``.

        :raises OverwriteError: File already exists while parameter \
            ``overwrite`` has been set to ``False``.
        '''
        destination = dst.get_uri() \
            if isinstance(dst, _NonLocalDir) \
            else dst.get_path()
        print(f'\nCopying file "{self.get_path()}" into "{destination}".')

        file_name = self.get_name()

        if not overwrite and dst.path_exists(file_name):
            raise _OverwriteError(
                file_path=_join_paths(self._get_separator(), destination, file_name))
        else:
            with open(self.get_path(), "rb") as file:
                error = dst._load_from_source(
                    file_name=file_name,
                    src=file,
                    metadata=self.get_metadata()
                        if include_metadata else None)

            if error is None:
                print("Operation successful!")
            else:
                print(f"Operation unsuccessful: {error.get_message()}")


class _NonLocalFile(_File, _ABC):
    '''
    An abstract class which serves as the base class for \
    all file-like classes that represent either remote files \
    or files in the cloud.

    :param str path: A path pointing to the file.
    :param ClientHandler handler: A ``ClientHandler`` class \
        instance used for interacting with the underlying handler.
    :param Ingester ingester: An ``Ingester`` class instance used \
        for ingesting data.
    '''

    def __init__(
        self,
        path: str,
        handler: _ClientHandler,
        ingester: _Ingester
    ):
        '''
        An abstract class which serves as the base class for \
        all file-like classes that represent either remote files \
        or files in the cloud.

        :param str path: A path pointing to the file.
        :param ClientHandler handler: A ``ClientHandler`` class \
            instance used for interacting with the underlying handler.
        :param Ingester ingester: An ``Ingester`` class instance used \
            for ingesting data.
        '''
        super().__init__(path=path, handler=handler, ingester=ingester)
        self.open()


    def is_cacheable(self) -> bool:
        '''
        Returns ``True`` if file has been defined \
        as cacheable, else returns ``False``.
        '''
        return self.__handler.is_cacheable()


    def purge(self) -> None:
        '''
        If cacheable, then purges the file's cache, \
        else does nothing.
        '''
        self.__handler.purge()


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
    

    def read(self) -> bytes:
        '''
        Reads and returns the file's bytes.
        '''
        ingester = self._get_ingester()
        ingester.set_source(self.get_path())
        with _io.BytesIO() as buffer:
            ingester.extract(snk=buffer, include_metadata=False)
            return buffer.getvalue()


    def transfer_to(
        self,
        dst: '_Directory',
        overwrite: bool = False,
        include_metadata: bool = False
    ) -> None:
        '''
        Copies the file into the provided directory.

        :param _Directory dst: A ``_Directory`` class instance, \
            which represents the transfer operation's destination.
        :param bool overwrite: Indicates whether to overwrite \
            the file if it already exists. Defaults to ``False``.
        :param bool include_metadata: Indicates whether any \
            existing metadata are to be assigned to the resulting \
            file. Defaults to ``False``.

        :raises OverwriteError: File already exists while parameter \
            ``overwrite`` has been set to ``False``.
        '''
        destination = dst.get_uri() \
            if isinstance(dst, _NonLocalDir) \
            else dst.get_path()
        print(f'\nCopying file "{self.get_uri()}" into "{destination}".')

        file_name = self.get_name()

        if not overwrite and dst.path_exists(file_name):
            raise _OverwriteError(
                file_path=_join_paths(self._get_separator(), destination, file_name))
        else:
            ingester = self._get_ingester()
            ingester.set_source(src=self.get_path())

            fetch_metadata = include_metadata
            if include_metadata and (
                (custom_metadata := self.get_metadata())
                is not None
            ):
                ingester.set_metadata(metadata=custom_metadata)
                fetch_metadata = False

            error = dst._load_from_ingester(
                file_name=file_name,
                ingester=ingester,
                fetch_metadata=fetch_metadata)

            if error is None:
                print("Operation successful!")
            else:
                print(f"Operation unsuccessful: {error.get_message()}")


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
        # Instantiate a connection handler,
        # if none has been set.
        if (ssh_handler := self._get_handler()) is None:
            ssh_handler = _SSHClientHandler(auth=auth, cache=cache)
            self.__host = auth.get_credentials()['hostname']

        print(ssh_handler)
        super().__init__(
            path=path,
            handler=ssh_handler,
            ingester=_RemoteIngester(ssh_handler))

        if not ssh_handler.path_exists(path=path):
            self.close()
            raise _IPE(path)
        
        if not ssh_handler.is_file(file_path=path):
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
        return f"sftp://{self.__host}/{self.get_path().lstrip(self._get_separator())}"
    

    @classmethod
    def _create_file(
        cls,
        path: str,
        host: str,
        handler: _typ.Any,
        metadata: _typ.Optional[dict[str, str]]
    ) -> 'RemoteFile':
        instance = cls.__new__(cls, handler=handler)
        _File.__init__(instance, path=path, ingester=_RemoteIngester(handler=handler))
        instance.set_metadata(metadata=metadata)
        instance.__host = host
        return instance


    def __del__(self) -> None:
        if self._get_handler() is not None:
            msg = "You might want to consider instantiating class ``RemoteFile``"
            msg += " through the use of a context manager by utilizing Python's"
            msg += " ``with`` statement, or by simply invoking an instance's"
            msg += " ``close`` method after being done using it."
            _warn.warn(msg, ResourceWarning)


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
        metadata = self._get_handler().fetch_file_metadata(self.get_path())
        self.set_metadata(metadata=metadata)


class AWSS3File(_CloudFile):
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

    :raises InvalidPathError: The provided path \
        does not exist.
    :raises InvalidFileError: The provided path \
        points to a directory.

    :note: The provided path must not begin with a separator.
        - Wrong: ``/path/to/file.txt``
        - Right: ``path/to/file.txt``
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

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path \
            points to a directory.

        :note: The provided path must not begin with a separator.
            - Wrong: ``/path/to/file.txt``
            - Right: ``path/to/file.txt``
        '''
        # Validate path.
        sep = _infer_sep(path=path)
        if path.startswith(sep):
            raise _IPE(path=path)
        
        # Instantiate a connection handler,
        # if none has been set.
        if (aws_handler := self._get_handler()) is None:
            aws_handler = _AWSClientHandler(
                auth=auth,
                bucket=bucket)
        
        super().__init__(
            path=path,
            cache=cache,
            handler=aws_handler,
            ingester=_AWSS3Ingester(aws_handler))
        
        # Check whether this is a path to
        # a directory instead of a file.
        if path.endswith(self._get_separator()):
            self.close()
            raise _IFE(path)

        # Check whether the file exists.
        from botocore.exceptions import ClientError as _CE

        try:
            self._get_client().Object(key=path).load()
        except _CE:
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
        Returns the object's URI.
        '''
        return f"s3://{self.get_bucket_name()}{self._get_separator()}{self.get_path()}"


    def __del__(self) -> None:
        if self._get_client() is not None:
            msg = "You might want to consider instantiating class `AWSS3File``"
            msg += " through the use of a context manager by utilizing Python's"
            msg += " ``with`` statement, or by simply invoking an instance's"
            msg += " ``close`` method after being done using it."
            _warn.warn(msg, ResourceWarning)


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

    :raises InvalidPathError: The provided path \
        does not exist.
    :raises InvalidFileError: The provided path \
        points to a directory.

    :note: The provided path must not begin with a separator.
        - Wrong: ``/path/to/file.txt``
        - Right: ``path/to/file.txt``
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

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidFileError: The provided path \
            points to a directory.

        :note: The provided path must not begin with a separator.
            - Wrong: ``/path/to/file.txt``
            - Right: ``path/to/file.txt``
        '''
        # Validate path.
        sep = _infer_sep(path=path)
        if path.startswith(sep):
            raise _IPE(path=path)

        # Instantiate a connection handler,
        # if none has been set.
        if (azr_handler := self._get_handler()) is None:
            azr_handler = _AzureClientHandler(
                auth=auth,
                container=container)
        
        # Infer storage account.
        self.__storage_account = auth._get_storage_account()
        
        super().__init__(
            path=path,
            cache=cache,
            handler=azr_handler,
            ingester=_AzureIngester(handler=azr_handler))
        
        # Check whether this is a path to
        # a directory instead of a file.
        if path.endswith(self._get_separator()):
            self.close()
            raise _IFE(path)

        # Fetch container client.
        azr_container: _ContainerClient = azr_handler.get_client()

        # Throw an exception if container does not exist.
        if not azr_container.exists():
            self.close()
            raise _ABCNFE(container)

        # Throw an exception if blob does not exist.  
        with azr_container.get_blob_client(blob=path) as blob:
            if not blob.exists():
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
        Returns the object's URI.
        '''
        uri = f"abfss://{self.get_container_name()}@{self.__storage_account}"
        uri += f".dfs.core.windows.net/{self.get_path()}"
        return uri


    def __del__(self) -> None:
        if self._get_client() is not None:
            msg = "You might want to consider instantiating class `AzureBlobFile``"
            msg += " through the use of a context manager by utilizing Python's"
            msg += " ``with`` statement, or by simply invoking an instance's"
            msg += " ``close`` method after being done using it."
            _warn.warn(msg, ResourceWarning)


class _Directory(_ABC):
    '''
    An abstract class which serves as the base class \
    for all directory-like classes.

    :param str path: The path pointing to the directory.
    :param Ingester ingester: An "Ingester" class instance used \
        for ingesting data.
    '''

    def __init__(
        self,
        path: str,
        handler: _ClientHandler,
        ingester: _Ingester
    ):
        '''
        An abstract class which serves as the base class \
        for all directory-like classes.

        :param str path: The path pointing to the directory.
        :param Ingester ingester: An "Ingester" class instance used \
            for ingesting data.
        '''
        sep = _infer_sep(path)
        self.__path = f"{path.rstrip(sep)}{sep}" if path != '' else path
        self.__separator = sep
        self.__handler = handler
        self.__ingester = ingester
        self.__metadata: dict[str, dict[str, str]] = dict()


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
        sep = self.__separator
        name = self.__path.rstrip(sep).split(sep)[-1]
        return name if name != "" else None
    

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
        if not self._is_file(path=file_path):
            raise _IFE(path=file_path)
        return dict(self.__metadata.get(self._relativize(path=file_path), dict()))


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

        if not self._is_file(path=file_path):
            raise _IFE(path=file_path)

        self.__metadata[self._relativize(path=file_path)] = dict(metadata)


    def path_exists(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path exists \
        within the directory, else returns ``False``.

        :param str path: Either an absolute path or a \
            path relative to the directory.
        '''
        return self.__handler.path_exists(_join_paths(
            self._get_separator(),
            self.get_path(),
            self._relativize(path=path)))


    def count(self, recursively: bool = False) -> int:
        '''
        Returns the total number of files within \
        within the directory.

        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.

        :note: The resulting number may vary depending on the value \
            of parameter ``recursively``.
        '''
        count = 0
        
        for _ in self._get_contents_iterable(
            recursively=recursively,
            include_dirs=True,
            show_abs_path=True
        ):
            count += 1

        return count


    def ls(self, recursively: bool = False, show_abs_path: bool = False) -> None:
        '''
        Lists the contents of the directory.

        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
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
        for file in self._get_contents_iterable(
            recursively=recursively,
            include_dirs=True,
            show_abs_path=show_abs_path
        ):
            print(file)


    def iterate_contents(
        self,
        recursively: bool = False,
        show_abs_path: bool = False
    ) -> _typ.Iterator[str]:
        '''
        Returns an iterator capable of going through the paths \
        ofthe dictionary's contents as strings.

        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
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
        return self._get_contents_iterable(
            recursively=recursively,
            include_dirs=True,
            show_abs_path=show_abs_path)


    def get_contents(
        self,
        recursively: bool = False,
        show_abs_path: bool = False
    ) -> list[str]:
        '''
        Returns a list containing the paths of the directory's contents.

        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
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
        return list(self._get_contents_iterable(
            recursively=recursively,
            include_dirs=True,
            show_abs_path=show_abs_path))
    

    def _get_separator(self) -> str:
        '''
        Returns the path's separator.
        '''
        return self.__separator
    

    def _get_contents_iterable(
        self,
        recursively: bool,
        include_dirs: bool,
        show_abs_path: bool
    ) -> _typ.Iterator[str]:
        '''
        Returns an iterator over the dictionary's contents.

        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories.
        :param bool include_dirs: Indicates whether to include any \
            directories in the results in case ``recursively`` is \
            set to ``False``.
        :param bool show_abs_path: Determines whether to include the \
            contents' absolute path or their path relative to this directory.
        '''
        if recursively:
            return self._iterate_contents_impl(
                recursively=True,
                show_abs_path=show_abs_path)

        iterable = self._iterate_contents_impl(
            recursively=False,
            show_abs_path=show_abs_path)
                 
        if not include_dirs:
            iterable = filter(
                lambda path: self._is_file(path=path),
                self._iterate_contents_impl(
                    recursively=False,
                    show_abs_path=show_abs_path))

        return iterable
    

    def _relativize(self, path: str) -> str:
        '''
        Transforms the provided path so that it is \
        relative to the directory.

        :param str path: The provided path.
        '''
        return path.removeprefix(self.__path).lstrip(self._get_separator())


    def _get_ingester(self) -> _Ingester:
        '''
        Returns the class's ``Ingester`` instance.
        '''
        return self.__ingester
    

    def _add_file_to_metadata_dict(
        self,
        file_path: str,
        metadata: _typ.Optional[dict[str, str]]
    ) -> None:
        '''
        Adds the provided path entry to the directory's \
        metadata store.

        :param str file_path: The relative path of the  \
            file in question.
        :param dict[str, str] | None metadata: A dictionary \
            containing the metadata that are to be \
            associated with the file.
        '''
        self.__metadata.update({ file_path: dict() })

        if metadata is not None:
            self.set_metadata(file_path=file_path, metadata=metadata)
    

    @_absmethod
    def get_uri(self) -> str:
        '''
        Returns the directory's URI.
        '''
        pass


    @_absmethod
    def get_size(self, recursively: bool = False) -> int:
        '''
        Returns the total sum of the sizes of all files \
        within the directory, in bytes.

        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.

        :note: The resulting size may vary depending on the value \
            of parameter ``recursively``.
        '''
        pass


    @_absmethod
    def transfer_to(
        self,
        dst: '_Directory',
        recursively: bool = False,
        overwrite: bool = False,
        include_metadata: bool = False,
        show_progress: bool = True
    ) -> None:
        '''
        Copies all files within this directory into \
        the destination directory.

        :param _Directory dst: A ``_Directory`` class instance, \
            which represents the transfer operation's destination.
        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
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
        :param bool show_progress: Indicates whether to display \
            a loading bar on the progress of the operations. \
            Defaults to ``True``.
        '''
        pass


    @_absmethod
    def _load_from_source(
        self,
        file_name: str,
        src: _typ.Union[_io.BufferedReader, _io.BytesIO],
        metadata: _typ.Optional[dict[str, str]]
    ) -> _typ.Optional[_Error]:
        '''
        Loads a file directly from the given source \
        and into this directory. Returns ``None`` if \
        the operation was successful, else returns an \
        ``Error`` instance.

        :param str file_name: The name of the file that is to be \
            loaded.
        :param Union[io.BufferedReader, io.BytesIO] src: A buffer \
            containing the file in bytes.
        :param dict[str, str] | None metadata: A dictionary \
            containing any metadata associated with the file.
        '''
        pass


    @_absmethod
    def _load_from_ingester(
        self,
        file_name: str,
        ingester: _Ingester,
        fetch_metadata: bool
    ) -> _typ.Optional[_Error]:
        '''
        Loads a file into this directory by using \
        the provided ingester. Returns ``None`` if \
        the operation was successful, else returns \
        an ``Error`` instance.

        :param str file_name: The name of the file that is to be \
            loaded.
        :param Ingester ingester: An ingester method used in \
            ingesting the file.
        :param bool overwrite: Indicates whether to overwrite \
            the file if it already exists. Defaults to ``False``.
        :param bool fetch_metadata: Indicates whether to \
            fetch any metadata associated with the file or not.
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
            path=f"{_os.path.abspath(path).replace(_os.sep, sep).rstrip(sep)}{sep}",
            ingester=_LocalIngester())


    def get_uri(self) -> str:
        '''
        Returns the directory's URI.
        '''
        sep = self._get_separator()
        return f"file:///{self.get_path().lstrip(sep)}"


    def get_size(self, recursively: bool = False) -> int:
        '''
        Returns the total sum of the sizes of all files \
        within the directory, in bytes.

        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.

        :note: The resulting size may vary depending on the value \
            of parameter ``recursively``.
        '''
        size = 0

        for file_path in self._get_contents_iterable(
            recursively=recursively,
            include_dirs=False,
            show_abs_path=True
        ):
            size += self._get_file_size(file_path)
        
        return size
    

    def get_file(self, path):
        return LocalFile.__new__(path)

    
    def transfer_to(
        self,
        dst: '_Directory',
        recursively: bool = False,
        overwrite: bool = False,
        include_metadata: bool = False,
        show_progress: bool = True
    ) -> None:
        '''
        Copies all files within this directory into \
        the destination directory.

        :param _Directory dst: A ``_Directory`` class instance, \
            which represents the transfer operation's destination.
        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
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
        :param bool show_progress: Indicates whether to display \
            a loading bar on the progress of the operations. \
            Defaults to ``True``.
        '''
        destination = dst.get_uri() \
            if isinstance(dst, _NonLocalDir) \
            else dst.get_path()

        print_msg = f'\nCopying files from "{self.get_path()}" '
        print_msg += f'into "{destination}".'
        print(print_msg)

        if recursively:
            total_num_files = self.count(recursively=True)
        else:
            total_num_files = 0
            for _ in self._get_contents_iterable(
                recursively=False,
                include_dirs=False,
                show_abs_path=True
            ):
                total_num_files += 1

        errors: list[_Error] = list()

        for file_path in _tqdm(
            iterable=self._get_contents_iterable(
                recursively=recursively,
                include_dirs=False,
                show_abs_path=True
            ),
            disable=not show_progress,
            desc="Progress",
            unit='files',
            total=total_num_files
        ):
            relative_path = self._relativize(file_path)
            if not overwrite and dst.path_exists(relative_path):
                error = _Error(
                    uri=_join_paths(dst._get_separator(), destination, relative_path),
                    is_src=False,
                    msg='File already exists. Try setting "overwrite" to "True".')
            else:
                with open(file_path, "rb") as file:
                    error = dst._load_from_source(
                        file_name=relative_path,
                        src=file,
                        metadata=self.get_metadata(
                            file_path=relative_path)
                            if include_metadata else None)

            if error is not None:
                errors.append(error)

        if len(errors) == 0:
            print(f'Operation successful: Copied all {total_num_files} files!')
        else:
            msg = "Operation unsuccessful: Failed to copy "
            msg += f"{len(errors)} out of {total_num_files} files."
            msg += f"\n\nDisplaying {len(errors)} errors:\n"
            for err in errors:
               msg += str(err)
            print(msg)


    def _iterate_contents_impl(
        self,
        recursively: bool = False,
        show_abs_path: bool = False
    ) -> _typ.Iterator[str]:
        '''
        Returns an iterator capable of going through the paths \
        ofthe dictionary's contents as strings.

        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
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

        sep = self._get_separator()

        if recursively:
            for dp, dn, fn in _os.walk(self.get_path()):
                dn.sort()
                for file in sorted(fn):
                    if not show_abs_path:
                        dp = self._relativize(dp.replace(_os.sep, sep))
                    yield _join_paths(sep, dp, file)
        else:
            for obj in sorted(_os.listdir(self.get_path())):
                if not self._is_file(obj):
                    obj += sep
                yield _join_paths(sep, self.get_path(), obj) \
                    if show_abs_path else obj


    def _load_from_source(
        self,
        file_name: str,
        src: _typ.Union[_io.BufferedReader, _io.BytesIO],
        metadata: _typ.Optional[dict[str, str]]
    ) -> _typ.Optional[_Error]:
        '''
        Loads a file directly from the given source \
        and into this directory. Returns ``None`` if \
        the operation was successful, else returns an \
        ``Error`` instance.

        :param str file_name: The name of the file that is to be \
            loaded.
        :param Union[io.BufferedReader, io.BytesIO] src: A buffer \
            containing the file in bytes.
        :param dict[str, str] | None metadata: A dictionary \
            containing any metadata associated with the file.
        '''

        file_path = _join_paths(self._get_separator(), self.get_path(), file_name)

        _os.makedirs(_os.path.dirname(file_path), exist_ok=True)

        ingester = self._get_ingester()

        with open(file_path, 'wb') as file:

            ingester.set_sink(snk=file)

            try:
                ingester.load(src=src, metadata=metadata)
                self._add_file_to_metadata_dict(
                    file_path=file_name,
                    metadata=metadata)
            except Exception as e:
                return _Error(
                    uri=file_path,
                    is_src=False,
                    msg=str(e))


    def _load_from_ingester(
        self,
        file_name: str,
        ingester: _Ingester,
        fetch_metadata: bool
    ) -> _typ.Optional[_Error]:
        '''
        Loads a file into this directory by using \
        the provided ingester. Returns ``None`` if \
        the operation was successful, else returns \
        an ``Error`` instance.

        :param str file_name: The name of the file that is to be \
            loaded.
        :param Ingester ingester: An ingester method used in \
            ingesting the file.
        :param bool fetch_metadata: Indicates whether to \
            fetch any metadata associated with the file or not.
        '''

        file_path = _join_paths(self._get_separator(), self.get_path(), file_name)

        _os.makedirs(_os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'wb') as file:
            try:
                ingester.extract(snk=file, include_metadata=fetch_metadata)
                self._add_file_to_metadata_dict(
                    file_path=file_name,
                    metadata=ingester.get_metadata())
            except Exception as e:
                return _Error(
                    uri=ingester.get_source(),
                    is_src=True,
                    msg=str(e))


    def _get_file_size(self, path: str) -> int:
        '''
        Returns the size of the file that corresponds \
        to the provided path in bytes.

        :param str path: The absolute path pointing to \
            the file in question.
        '''
        return _os.path.getsize(path) 


    def _is_file(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path \
        points to a file, else returns ``False``.

        :param str path: Either the absolute path or the \
            path relative to the directory of the file in \
            question.
        '''
        if not path.startswith(self.get_path()):
            path = _join_paths(self._get_separator(), self.get_path(), path)
        return _os.path.isfile(path)


class _NonLocalDir(_Directory, _ABC):
    '''
    An abstract class which serves as the base class for \
    all directory-like classes that represent either remote \
    directories or directories in the cloud.

    :param str path: The path pointing to the directory.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access.
    :param ClientHandler handler: A ``ClientHandler`` class \
        instance used for interacting with the underlying handler.
    :param Ingester ingester: An ``Ingester`` class instance used \
        for ingesting data.
    '''
    def __init__(
        self,
        path: str,
        cache: bool,
        handler: _ClientHandler,
        ingester: _Ingester
    ):
        '''
        An abstract class which serves as the base class for \
        all directory-like classes that represent either remote \
        directories or directories in the cloud.

        :param str path: The path pointing to the directory.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access.
        :param ClientHandler handler: A ``ClientHandler`` class \
            instance used for interacting with the underlying handler.
        :param Ingester ingester: An ``Ingester`` class instance used \
            for ingesting data.
        '''
        super().__init__(path=path, ingester=ingester)
        self.__handler = handler
        self.open()


    def is_cacheable(self) -> bool:
        '''
        Returns ``True`` if directory has been \
        defined as cacheable, else returns ``False``.
        '''
        return self.__handler.is_cacheable()


    def purge(self) -> None:
        '''
        If cacheable, then purges the directory's cache, \
        else does nothing.
        '''
        self.__handler.purge()


    def open(self) -> None:
        '''
        Opens all necessary connections.
        '''
        self.__handler.open_connections()


    def close(self) -> None:
        '''
        Closes all open connections.
        '''
        self.__handler.close_connections()


    def get_size(self, recursively: bool = False) -> int:
        '''
        Returns the total sum of the sizes of all files \
        within the directory, in bytes.

        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories. Defaults to ``False``.

        :note: The resulting size may vary depending on the value \
            of parameter ``recursively``.
        '''
        total_size = 0

        for file_path in self._get_contents_iterable(
            recursively=recursively,
            include_dirs=False,
            show_abs_path=True
        ):
            if self.is_cacheable():
                if (size := self.__cache_manager.get_size(file_path=file_path)) is not None:
                    total_size += size
                else:
                    size = self._get_file_size(file_path)
                    self.__cache_manager.cache_size(file_path, size)
                    total_size += size
            else:
                total_size += self._get_file_size(file_path)

        return total_size


    def transfer_to(
        self,
        dst: '_Directory',
        recursively: bool = False,
        overwrite: bool = False,
        include_metadata: bool = False,
        show_progress: bool = True
    ) -> None:
        '''
        Copies all files within this directory into \
        the destination directory.

        :param _Directory dst: A ``_Directory`` class instance, \
            which represents the transfer operation's destination.
        :param bool recursively: Indicates whether the directory \
            is to be scanned recursively or not. If set to  ``False``, \
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
        :param bool show_progress: Indicates whether to display \
            a loading bar on the progress of the operations. \
            Defaults to ``True``.
        '''
        destination = dst.get_uri() \
            if isinstance(dst, _NonLocalDir) \
            else dst.get_path()

        print_msg = f'\nCopying files from "{self.get_path()}" '
        print_msg += f'into "{destination}".'
        print(print_msg)

        if recursively:
            total_num_files = self.count(recursively=True)
        else:
            total_num_files = 0
            for _ in self._get_contents_iterable(
                recursively=recursively,
                include_dirs=False,
                show_abs_path=True
            ):
                total_num_files += 1

        errors: list[_Error] = list()

        ingester = self._get_ingester()

        for file_path in _tqdm(
            iterable=self._get_contents_iterable(
                recursively=recursively,
                include_dirs=False,
                show_abs_path=True
            ),
            disable=not show_progress,
            desc="Progress",
            unit='files',
            total=total_num_files
        ):
            relative_path = self._relativize(file_path)
            if not overwrite and dst.path_exists(relative_path):
                error = _Error(
                    uri=_join_paths(dst._get_separator(), destination, relative_path),
                    is_src=False,
                    msg='File already exists. Try setting "overwrite" to "True".')
            else:
                ingester.set_source(src=file_path)

                fetch_metadata = include_metadata

                if include_metadata and (
                    (custom_metadata := self.get_metadata(file_path))
                    is not None
                ):
                    ingester.set_metadata(metadata=custom_metadata)
                    fetch_metadata = False

                error = dst._load_from_ingester(
                    file_name = relative_path,
                    ingester=ingester,
                    fetch_metadata=fetch_metadata)

            if error is not None:
                errors.append(error)
        
        if len(errors) == 0:
            print(f'Operation successful: Copied all {total_num_files} files!')
        else:
            msg = "Operation unsuccessful: Failed to copy "
            msg += f"{len(errors)} out of {total_num_files} files."
            msg += f"\n\nDisplaying {len(errors)} errors:\n"
            for err in errors:
               msg += str(err)
            print(msg)


    def _load_from_source(
        self,
        file_name: str,
        src: _typ.Union[_io.BufferedReader, _io.BytesIO],
        metadata: _typ.Optional[dict[str, str]]
    ) -> _typ.Optional[_Error]:
        '''
        Loads a file directly from the given source \
        and into this directory. Returns ``None`` if \
        the operation was successful, else returns an \
        ``Error`` instance.

        :param str file_name: The name of the file that is to be \
            loaded.
        :param Union[io.BufferedReader, io.BytesIO] src: A buffer \
            containing the file in bytes.
        :param dict[str, str] | None metadata: A dictionary \
            containing any metadata associated with the file.
        '''

        ingester = self._get_ingester()
        ingester.set_sink(_join_paths(self._get_separator(), self.get_path(), file_name))

        try:
            ingester.load(src=src, metadata=metadata)
            self._add_file_to_metadata_dict(
                file_path=file_name,
                metadata=metadata)
        except Exception as e:
            return _Error(
                uri=_join_paths(self._get_separator(), self.get_uri(), file_name),
                is_src=False,
                msg=str(e))


    def _load_from_ingester(
        self,
        file_name: str,
        ingester: _Ingester,
        fetch_metadata: bool
    ) -> _typ.Optional[_Error]:
        '''
        Loads a file into this directory by using \
        the provided ingester. Returns ``None`` if \
        the operation was successful, else returns \
        an ``Error`` instance.

        :param str file_name: The name of the file that is to be \
            loaded.
        :param Ingester ingester: An ingester method used in \
            ingesting the file.
        :param bool fetch_metadata: Indicates whether to \
            fetch any metadata associated with the file or not.
        '''
        try:
            with _io.BytesIO() as buffer:
                # Load data into buffer via the ingester.
                ingester.extract(
                    snk=buffer,
                    include_metadata=fetch_metadata)
                # Then load data to sink directly from buffer.
                buffer.seek(0)
                error: _Error = self._load_from_source(
                    file_name=file_name,
                    src=buffer,
                    metadata=ingester.get_metadata())
                if error is None:
                    self._add_file_to_metadata_dict(
                        file_path=file_name,
                        metadata=ingester.get_metadata())
                else:
                    return error
        except Exception as e:
            return _Error(
                uri=ingester.get_source(),
                is_src=True,
                msg=str(e))


    def _get_handler(self) -> _typ.Any:
        '''
        Returns this instance's ``ClientHandler``.
        '''
        return self.__handler
    

    def __new__(cls, *args, **kwargs) -> '_NonLocalDir':
        '''
        Creates an instance of this class.

        :note: This method defines field ``__handler`` \
            so that throwing an exception before invoking \
            the parent constructor does not result in a \
            second exception being thrown due to ``__del__``.
        '''
        instance = super().__new__(cls)
        instance.__handler = None
        return instance
    

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
        ssh_handler = _SSHClientHandler(auth=auth)

        super().__init__(
            path=path,
            cache=cache,
            handler=ssh_handler,
            ingester=_RemoteIngester(handler=ssh_handler))
        
        # Validate path.
        if path == '':
            self.close()
            raise _IPE(path=path)

        self.__host = auth.get_credentials()['hostname']

        from stat import S_ISDIR as _is_dir

        sftp = ssh_handler.get_client()

        stats = None

        try:
            stats = sftp.stat(path=path)
        except FileNotFoundError:
            if create_if_missing:
                sftp.mkdir(path=path)
                stats = sftp.stat(path=path)
            else:
                self.close()
                raise _IPE(path)
        except:
            self.close()
            raise ConnectionError()

        if not _is_dir(stats.st_mode):
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
        return f"sftp://{self.__host}/{self.get_path().lstrip(self._get_separator())}"
    

    def get_file(self, file_path: str) -> RemoteFile:
        '''
        '''
        file_path = _join_paths(
            self._get_separator(),
            self.get_path(),
            self._relativize(file_path))
        return RemoteFile._create_file(
            path=file_path,
            host=self.get_hostname(),
            handler=self._get_handler(),
            cache=self._get_cache_manager().get_cache(file_path)
                if self.is_cacheable() else None,
            metadata=self.get_metadata(file_path))


    def _is_file(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path \
        points to a file, else returns ``False``.

        :param str path: Either the absolute path or the \
            path relative to the directory of the file in \
            question.
        '''
        from stat import S_ISDIR as _is_dir
        if not path.startswith(self.get_path()):
            path = _join_paths(self._get_separator(), self.get_path(), path)
        try:
            return not _is_dir(self._get_client().stat(path=path).st_mode)
        except FileNotFoundError:
            return False


    def __del__(self) -> None:
        if self._get_client() is not None:
            msg = "You might want to consider instantiating class ``RemoteDir``"
            msg += " through the use of a context manager by utilizing Python's"
            msg += " ``with`` statement, or by simply invoking an instance's"
            msg += " ``close`` method after being done using it."
            _warn.warn(msg, ResourceWarning)


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
            is to be scanned recursively or not. If set to  ``False``, \
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

        cache_manager = self._get_cache_manager()

        for file_path in self._get_contents_iterable(
            recursively=recursively,
            include_dirs=False,
            show_abs_path=True
        ):
            if self.is_cacheable():
                if (metadata := cache_manager.get_metadata(file_path=file_path)) is not None:
                    self.set_metadata(file_path, metadata)
                else:
                    metadata = self._load_metadata_impl(file_path)
                    cache_manager.cache_metadata(file_path, metadata)
                    self.set_metadata(file_path, metadata)
            else:
                self.set_metadata(file_path,
                    self._load_metadata_impl(file_path))
                

    def get_metadata(self, file_path: str) -> _typ.Optional[dict[str, str]]:
        '''
        Returns a dictionary containing any metadata \
            associated with the file in question.

        :param str file_path: Either the absolute path \
            or the path relative to the directory of the \
            file in question.

        :raises InvalidFileError: The provided path does \
            not point to a file within the directory.
        '''
        metadata = super().get_metadata(file_path=file_path)
   
        if metadata is not None:
            return metadata
        
        if self.is_cacheable():
            abs_path = _join_paths(
                self._get_separator(),
                self.get_path(),
                self._relativize(file_path))
            return self._get_cache_manager().get_metadata(file_path=abs_path)
        
        return None


class AWSS3Dir(_CloudDir):
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

    :raises InvalidPathError: The provided path \
        does not exist.

    :note: The provided path must not begin with a separator.
        - Wrong: ``/path/to/dir/``
        - Right: ``path/to/dir/``
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

        :raises InvalidPathError: The provided path \
            does not exist.

        :note: The provided path must not begin with a separator.
            - Wrong: ``/path/to/dir/``
            - Right: ``path/to/dir/``
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
            bucket=bucket)

        super().__init__(
            path=path,
            cache=cache,
            handler=aws_handler,
            ingester=_AWSS3Ingester(handler=aws_handler))

        s3_bucket = aws_handler.get_client()

        # Check if directory exists.
        dir_exists: bool = False

        for _ in s3_bucket.objects.filter(Prefix=path):
            dir_exists = True
            break

        # If it doesn't, either create it
        # or throw an exception.
        if not dir_exists:
            if create_if_missing:
                s3_bucket.put_object(
                    Key=path,
                    ContentType='application/x-directory; charset=UTF-8')
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


    def __del__(self) -> None:
        if self._get_client() is not None:
            msg = "You might want to consider instantiating class `AWSS3Dir``"
            msg += " through the use of a context manager by utilizing Python's"
            msg += " ``with`` statement, or by simply invoking an instance's"
            msg += " ``close`` method after being done using it."
            _warn.warn(msg, ResourceWarning)


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

    :raises InvalidPathError: The provided path \
        does not exist.
    :raises InvalidDirectoryError: The provided path \
        does not point to a directory.

    :note: The provided path must not begin with a separator.
        - Wrong: ``/path/to/dir/``
        - Right: ``path/to/dir/``
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

        :raises InvalidPathError: The provided path \
            does not exist.
        :raises InvalidDirectoryError: The provided path \
            does not point to a directory.

        :note: The provided path must not begin with a separator.
            - Wrong: ``/path/to/dir/``
            - Right: ``path/to/dir/``
        '''        
        # Validate path.
        if path is None:
            path = '/'
        else:
            sep = _infer_sep(path=path)
            if path.startswith(sep):
                raise _IPE(path=path)
            
        # Instantiate a connection handler.
        azr_handler = _AzureClientHandler(
            auth=auth,
            container=container)
        
        # Infer storage account.
        self.__storage_account = auth._get_storage_account()

        super().__init__(
            path=path,
            cache=cache,
            handler=azr_handler,
            ingester=_AzureIngester(handler=azr_handler))

        # Fetch container client.
        azr_container: _ContainerClient = azr_handler.get_client()

        # Throw an exception if container does not exist.
        if not azr_container.exists():
            self.close()
            raise _ABCNFE(container)

        # Create directory or throw an exception
        # depending on the value of "create_if_missing".
        with azr_container.get_blob_client(blob=path) as blob:
            if not blob.exists():
                if create_if_missing:
                    dummy_blob = azr_container.get_blob_client(blob=f"{path}DUMMY")
                    dummy_blob.create_append_blob()
                    dummy_blob.delete_blob()
                    dummy_blob.close()
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


    def __del__(self) -> None:
        if self._get_client() is not None:
            msg = "You might want to consider instantiating class ``AzureBlobDir``"
            msg += " through the use of a context manager by utilizing Python's"
            msg += " ``with`` statement, or by simply invoking an instance's"
            msg += " ``close`` method after being done using it."
            _warn.warn(msg, ResourceWarning)
