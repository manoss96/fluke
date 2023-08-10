import os as _os
from abc import ABC as _ABC
from abc import abstractmethod as _absmethod
from typing import Iterator as _Iterator
from typing import Optional as _Optional


import boto3 as _boto3
import paramiko as _prmk
from google.cloud.storage import Client as _GCSClient
from azure.identity import ClientSecretCredential as _CSC
from azure.storage.blob import ContainerClient as _ContainerClient
from botocore.exceptions import ClientError as _CE


from .auth import AWSAuth as _AWSAuth
from .auth import AzureAuth as _AzureAuth
from .auth import GCPAuth as _GCPAuth
from .auth import RemoteAuth as _RemoteAuth
from ._cache import CacheManager as _CacheManager
from ._iohandlers import _FileReader
from ._iohandlers import _FileWriter
from ._iohandlers import LocalFileReader as _LocalFileReader
from ._iohandlers import LocalFileWriter as _LocalFileWriter
from ._iohandlers import RemoteFileReader as _RemoteFileReader
from ._iohandlers import RemoteFileWriter as _RemoteFileWriter
from ._iohandlers import AmazonS3FileReader as _AmazonS3FileReader
from ._iohandlers import AmazonS3FileWriter as _AmazonS3FileWriter
from ._iohandlers import AzureBlobReader as _AzureBlobReader
from ._iohandlers import AzureBlobWriter as _AzureBlobWriter
from ._iohandlers import GCPFileReader as _GCPFileReader
from ._iohandlers import GCPFileWriter as _GCPFileWriter
from ._exceptions import UnknownKeyTypeError as _UKTE
from ._exceptions import BucketNotFoundError as _BNFE
from ._helper import join_paths as _join_paths
from ._helper import infer_separator as _infer_sep
from ._helper import relativize_path as _relativize


class ClientHandler(_ABC):
    '''
    An abstract class which serves as the \
    base class for all client-handler-like classes.

    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access.
    '''


    def __init__(self, cache: bool):
        '''
        An abstract class which serves as the \
        base class for all client-handler-like classes.

        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access.
        '''

        self.__cache_manager = _CacheManager() if cache else None


    def is_cacheable(self) -> bool:
        '''
        Returns a value indicating whether this \
        handler instance has been defined so that \
        it is able to cache data.
        '''
        return self.__cache_manager is not None
    

    def purge(self) -> None:
        '''
        If cacheable, then purges the handler's cache, \
        else does nothing.
        '''
        if self.is_cacheable():
            self.__cache_manager.purge()


    def get_file_size(self, file_path: str) -> int:
        '''
        Returns the size of a file in bytes.
        
        :param str file_path: The absolute path of the \
            file in question.

        :note: This method will go on to fetch the requested value \
            from a remote resource only when one of the following is \
            true:
            - Caching has not been enabled.
            - Caching has not been enabled, though \
              the requested value has not been cached.

            In the second case, the value will be cached \
            after it has been retrieved.
        '''
        if self.is_cacheable():
            if (size := self.__cache_manager.get_size(file_path=file_path)) is not None:
                return size
            else:
                size = self._get_file_size_impl(file_path)
                self.__cache_manager.cache_size(file_path, size)
                return size
        else:
            return self._get_file_size_impl(file_path)
        

    def get_file_metadata(self, file_path: str) -> dict[str, str]:
        '''
        Returns a dictionary containing the metadata of a file.
        
        :param str file_path: The absolute path of the file \
            in question.
        
        :note: This method will go on to fetch the requested value \
            from a remote resource only when one of the following is \
            true:
            - Caching has not been enabled.
            - Caching has not been enabled, though \
              the requested value has not been cached.

            In the second case, the value will be cached \
            after it has been retrieved.
        '''
        if self.is_cacheable():
            if (metadata := self.__cache_manager.get_metadata(
                file_path=file_path)
            ) is not None:
                return metadata
            else:
                metadata = self._get_file_metadata_impl(file_path)
                self.__cache_manager.cache_metadata(file_path, metadata)
                return metadata
        else:
            return self._get_file_metadata_impl(file_path)
        

    def traverse_dir(
        self,
        dir_path: str,
        recursively: bool,
        include_dirs: bool,
        show_abs_path: bool
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through \
        the directory's contents as their paths.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories.
        :param bool include_dirs: Indicates whether to include any \
            directories in the results in case ``recursively`` is \
            set to ``False``.
        :param bool show_abs_path: Indicates whether it \
            should be displayed the absolute or the relative \
            path of the contents.

        :note: The resulting iterator may vary depending on the \
            value of parameter ``recursively``.
        '''

        def relativize_iter(iterator: _Iterator[str]):
            return map(lambda p: _relativize(
                parent=dir_path,
                child=p,
                sep=_infer_sep(dir_path)
            ), iterator)

        if self.is_cacheable():
            # Grab content iterator from cache if it exists.
            if (iterator := self.__cache_manager.get_content_iterator(
                dir_path=dir_path,
                recursively=recursively,
                include_dirs=include_dirs)
            ) is not None:
                return iterator if show_abs_path \
                    else relativize_iter(iterator)
            # Else...
            else:
                # Fetch content iterator.
                # NOTE: Set "show_abs_path" to "True" so that all
                #       contents are cached via their absolute paths.
                iterator = self._traverse_dir_impl(
                    dir_path=dir_path,
                    recursively=recursively,
                    show_abs_path=True)
                # Cache all contents.
                self.__cache_manager.cache_contents(
                    dir_path=dir_path,
                    iterator=iterator,
                    recursively=recursively,
                    is_file=self.is_file)
                # Reset iterator by grabbing it from cache.
                iterator = self.__cache_manager.get_content_iterator(
                    dir_path=dir_path,
                    recursively=recursively,
                    include_dirs=include_dirs)
                if show_abs_path:
                    return iterator
                else:
                    return relativize_iter(iterator)
        else:
            if recursively or include_dirs:
                return self._traverse_dir_impl(
                    dir_path=dir_path,
                    recursively=recursively,
                    show_abs_path=show_abs_path)
            else:
                iterator = filter(
                    self.is_file,
                    self._traverse_dir_impl(
                        dir_path=dir_path,
                        recursively=recursively,
                        show_abs_path=True))
                return iterator if show_abs_path \
                    else relativize_iter(iterator)


    @_absmethod
    def is_open(self) -> bool:
        '''
        Returns a value indicating whether \
        this handler's underlying client connection \
        is open or not.
        '''
        pass


    @_absmethod
    def open_connections(self) -> None:
        '''
        Opens all necessary connections.
        '''
        pass


    @_absmethod
    def close_connections(self) -> None:
        '''
        Close all open connections.
        '''
        pass


    @_absmethod
    def path_exists(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path exists \
        within the directory, else returns ``False``.

        :param str path: An absolute path.
        '''
        pass


    @_absmethod
    def is_file(self, file_path: str) -> bool:
        '''
        Returns ``True`` if the provided path points \
        to a file, else returns ``False``.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        pass


    @_absmethod
    def mkdir(self, path: str) -> None:
        '''
        Creates a directory into the provided path.

        :param str path: The path of the directory \
            that is to be created.
        '''
        pass


    @_absmethod
    def get_reader(self, file_path: str) -> '_FileReader':
        '''
        Returns a ``_FileReader`` class instance \
        used for reading from a file.

        :param str file_path: The absolute path of \
            the file in question.
        '''
        pass


    @_absmethod
    def get_writer(
        self,
        file_path: str,
        metadata: _Optional[dict[str, str]],
        chunk_size: _Optional[int]
    ) -> '_FileWriter':
        '''
        Returns an ``_FileWriter`` class instance \
        used for writing to a file.

        :param str file_path: The absolute path of \
            the file in question.
        :param dict[str, str] | None metadata: A \
            dictionary containing the metadata that \
            are to be assigned to the file in question. \
            If ``None``, then no metadata are assigned.
        :param int | None chunk_size: Indicates whether \
            the size of distinct chunks in which the file \
            is written. If ``None``, then the file is to be \
            written as a single chunk of bytes.
        '''
        pass


    @_absmethod
    def _traverse_dir_impl(
        self,
        dir_path: str,
        recursively: bool,
        show_abs_path: bool
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through \
        the directory's contents as their paths.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories.
        :param bool show_abs_path: Indicates whether it \
            should be displayed the absolute or the relative \
            path of the contents.

        :note: The resulting iterator may vary depending on the \
            value of parameter ``recursively``.
        '''
        pass


    @_absmethod
    def _get_file_size_impl(self, file_path: str) -> int:
        '''
        Fetches and returns the size of a file in bytes.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        pass


    @_absmethod
    def _get_file_metadata_impl(self, file_path: str) -> dict[str, str]:
        '''
        Fetches and returns a dictionary containing the \
        metadata of a file.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        pass


class FileSystemHandler(ClientHandler):
    '''
    A class used in handling all local file \
    system operations.

    :param str file_path: The absolute path of \
        the file in question.
    '''

    def __init__(self):
        '''
        A class used in handling all local file \
        system operations.
        '''
        super().__init__(cache=False)


    def is_open(self) -> bool:
        '''
        Returns a value indicating whether \
        this handler's underlying client connection \
        is open or not.
        '''
        raise NotImplementedError()


    def open_connections(self):
        '''
        Throws ``NotImplementedError``.
        '''
        raise NotImplementedError()


    def close_connections(self):
        '''
        Throws ``NotImplementedError``.
        '''
        raise NotImplementedError()


    def path_exists(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path exists \
        within the directory, else returns ``False``.

        :param str path: An absolute path.
        '''
        return _os.path.exists(path=path)
    

    def is_file(self, file_path: str) -> bool:
        '''
        Returns ``True`` if the provided path points \
        to a file, else returns ``False``.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        return _os.path.isfile(file_path)
    

    def mkdir(self, path: str) -> None:
        '''
        Creates a directory into the provided path.

        :param str path: The path of the directory \
            that is to be created.
        '''
        _os.makedirs(path, exist_ok=True)


    def get_reader(self, file_path: str) -> _LocalFileReader:
        '''
        Returns a ``LocalFileReader`` class instance \
        used for reading from a file which resides \
        within the local file system.

        :param str file_path: The absolute path of \
            the file in question.
        '''
        return _LocalFileReader(
            file_path=file_path,
            file_size=self.get_file_size(file_path))


    def get_writer(
        self,
        file_path: str,
        metadata: _Optional[dict[str, str]],
        chunk_size: _Optional[int]
    ) -> _LocalFileWriter:
        '''
        Returns an ``LocalFileWriter`` class instance \
        used for writing to a file which resides within \
        the local file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param dict[str, str] | None metadata: A \
            dictionary containing the metadata that \
            are to be assigned to the file in question. \
            If ``None``, then no metadata are assigned.
        :param int | None chunk_size: Indicates whether \
            the size of distinct chunks in which the file \
            is written. If ``None``, then the file is to be \
            written as a single chunk of bytes.
        '''
        return _LocalFileWriter(file_path=file_path)
    

    def _get_file_size_impl(self, file_path) -> int:
        '''
        Fetches and returns the size of a file in bytes.

        :param str file_path: The path of the file in question.
        '''
        return _os.path.getsize(file_path)
    

    def _get_file_metadata_impl(self, file_path: str) -> dict[str, str]:
        '''
        Throws ``NotImplementedError``.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        raise NotImplementedError()


    def _traverse_dir_impl(
        self,
        dir_path: str,
        recursively: bool,
        show_abs_path: bool
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through \
        the directory's contents as their paths.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories.
        :param bool show_abs_path: Indicates whether it \
            should be displayed the absolute or the relative \
            path of the contents.

        :note: The resulting iterator may vary depending on the \
            value of parameter ``recursively``.
        '''
        sep = _infer_sep(path=dir_path)

        if recursively:
            for dp, dn, fn in _os.walk(dir_path):
                dn.sort()
                for file in sorted(fn):
                    dp = dp.replace(_os.sep, sep)
                    if not show_abs_path:
                        dp = _relativize(
                            parent=dir_path,
                            child=dp,
                            sep=sep)
                    yield _join_paths(sep, dp, file)
        else:
            for obj in sorted(_os.listdir(dir_path)):
                abs_path = _join_paths(sep, dir_path, obj)
                if not self.is_file(abs_path):
                    abs_path += sep
                    obj += sep
                yield abs_path if show_abs_path else obj


class SSHClientHandler(ClientHandler):
    '''
    A class used in handling the SSH and SFTP \
    connections to a remote server.

    :param RemoteAuth auth: A ``RemoteAuth`` instance used \
        for authenticating with a remote machine.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access.
    '''        

    def __init__(self, auth: _RemoteAuth, cache: bool):
        '''
        A class used in handling the SSH and SFTP \
        connections to a remote server.

        :param RemoteAuth auth: A ``RemoteAuth`` instance used \
            for authenticating with a remote machine.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access.
        '''
        super().__init__(cache=cache)
        self.__auth: _RemoteAuth = auth
        self.__ssh: _prmk.SSHClient = None
        self.__sftp: _prmk.SFTPClient = None


    def is_open(self) -> bool:
        '''
        Returns a value indicating whether \
        this handler's underlying client connection \
        is open or not.
        '''
        return self.__ssh is not None


    def open_connections(self):
        '''
        Opens an SSH/SFTP connection to \
        the remote server.
        '''

        if self.__ssh is not None:
            return

        ssh = _prmk.SSHClient()

        credentials = self.__auth.get_credentials()

        public_key = credentials.pop('public_key')
        key_type = credentials.pop('key_type')
        verify_host = credentials.pop('verify_host')

        print(f"\nEstablishing connection to '{credentials['hostname']}'...")

        # Load all known hosts.
        ssh.load_system_host_keys()

        # If 'verify_host' has been set to 'False',
        # then automatically add any host to the list of
        # known hosts.
        if not verify_host:
            ssh.set_missing_host_key_policy(_prmk.AutoAddPolicy)
        # Else if 'public_key' and 'key_type' have been set,
        # try verifying the host before adding them to the
        # list of known hosts.
        elif public_key is not None and key_type is not None:
            if key_type == 'ssh-rsa':
                key_builder = _prmk.RSAKey
            elif key_type == 'ssh-dss':
                key_builder = _prmk.DSSKey
            elif key_type == 'ssh-ed25519':
                key_builder = _prmk.Ed25519Key    
            elif 'ecdsa-sha2' in key_type:
                key_builder = _prmk.ECDSAKey
            else:
                raise _UKTE(key_type=key_type)
            
            from base64 import decodebytes as _decodebytes

            ssh.get_host_keys().add(
                hostname=credentials['hostname'],
                keytype=str(key_type),
                key=key_builder(data=_decodebytes(
                    public_key.encode())))
            
        # If key-based authentication has been chosen,
        # then create a ``PKey`` instance.
        if 'pkey' in credentials:
            credentials.update({'pkey': _prmk.PKey.from_private_key_file(
                filename=credentials['pkey'],
                password=credentials.pop('passphrase'))})

        # Try connecting to the remote machine.
        try:
            ssh.connect(**credentials)
        except _prmk.SSHException as e:
            raise e

        self.__ssh = ssh
        self.__sftp = ssh.open_sftp()
        print("Connection established!")


    def close_connections(self):
        '''
        Closes the SSH/SFTP connection to \
        the remote server.
        '''
        if self.__ssh is not None:
            self.__sftp.close()
            self.__sftp = None
            self.__ssh.close()
            self.__ssh = None


    def path_exists(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path exists \
        within the directory, else returns ``False``.

        :param str path: An absolute path.
        '''
        try:
            self.__sftp.stat(path=path)
        except FileNotFoundError:
            return False
        return True
    

    def is_file(self, file_path: str) -> bool:
        '''
        Returns ``True`` if the provided path points \
        to a file, else returns ``False``.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        from stat import S_ISDIR as _is_dir
        return not _is_dir(self.__sftp.stat(path=file_path).st_mode)
    
    
    def mkdir(self, path: str) -> None:
        '''
        Creates a directory into the provided path.

        :param str path: The path of the directory \
            that is to be created.
        '''
        self.__sftp.mkdir(path=path)


    def get_reader(self, file_path: str) -> _RemoteFileReader:
        '''
        Returns a ``RemoteFileReader`` class instance \
        used for reading from a file which resides \
        within a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        '''
        return _RemoteFileReader(
            file_path=file_path,
            file_size=self.get_file_size(file_path),
            sftp=self.__sftp)


    def get_writer(
        self,
        file_path: str,
        metadata: _Optional[dict[str, str]],
        chunk_size: _Optional[int]
    ) -> _RemoteFileWriter:
        '''
        Returns a ``RemoteFileWriter`` class instance \
        used for writing to a file which resides within \
        a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param dict[str, str] | None metadata: A \
            dictionary containing the metadata that \
            are to be assigned to the file in question. \
            If ``None``, then no metadata are assigned.
        :param int | None chunk_size: Indicates whether \
            the size of distinct chunks in which the file \
            is written. If ``None``, then the file is to be \
            written as a single chunk of bytes.
        '''
        return _RemoteFileWriter(
            file_path=file_path, sftp=self.__sftp)


    def _get_file_size_impl(self, file_path) -> int:
        '''
        Fetches and returns the size of a file in bytes.

        :param str file_path: The path of the file in question.
        '''
        return self.__sftp.stat(path=file_path).st_size
    

    def _get_file_metadata_impl(self, file_path: str) -> dict[str, str]:
        '''
        Fetches and returns a dictionary containing the \
        metadata of a file.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        raise NotImplementedError()


    def _traverse_dir_impl(
        self,
        dir_path: str,
        recursively: bool,
        show_abs_path: bool
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through \
        the directory's contents as their paths.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories.
        :param bool show_abs_path: Indicates whether it \
            should be displayed the absolute or the relative \
            path of the contents.

        :note: The resulting iterator may vary depending on the \
            value of parameter ``recursively``.
        '''
        from stat import S_ISDIR as _is_dir

        sep = _infer_sep(dir_path)

        if recursively:

            def filter_obj(
                sftp: _prmk.SFTPClient,
                attr: _prmk.SFTPAttributes,
                parent_dir: str
            ):
                abs_path = _join_paths(
                    sep,
                    parent_dir,
                    attr.filename)

                if _is_dir(attr.st_mode):
                    try:
                        for sub_attr in sftp.listdir_attr(path=abs_path):
                            yield from filter_obj(
                                sftp=sftp,
                                attr=sub_attr,
                                parent_dir=abs_path)
                    except:
                        pass
                else:
                    yield abs_path

            for attr in self.__sftp.listdir_attr(path=dir_path):
                for file_path in filter_obj(
                    sftp=self.__sftp,
                    attr=attr,
                    parent_dir=dir_path
                ):
                    yield (file_path if show_abs_path \
                        else _relativize(
                            parent=dir_path,
                            child=file_path,
                            sep=sep))
        else:
            for attr in self.__sftp.listdir_attr(path=dir_path):
                path = attr.filename
                if _is_dir(attr.st_mode):
                    path += sep
                yield _join_paths(sep, dir_path, path) \
                    if show_abs_path else path


class AWSClientHandler(ClientHandler):
    '''
    A class used in handling the HTTP \
    connection to an Amazon S3 bucket.

    :param AWSAuth auth: An ``AWSAuth`` instance \
        used in authenticating with AWS.
    :param str bucket: The name of the Amazon S3 bucket \
        to which a connection is to be established.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access.
    '''

    def __init__(
        self,
        auth: _AWSAuth,
        bucket: str,
        cache: bool
    ):
        '''
        A class used in handling the HTTP \
        connection to an Amazon S3 bucket.

        :param AWSAuth auth: An ``AWSAuth`` instance \
            used in authenticating with AWS.
        :param str bucket: The name of the Amazon S3 bucket \
            to which a connection is to be established.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access.
        '''
        super().__init__(cache=cache)
        self.__auth = auth
        self.__bucket_name = bucket
        self.__bucket = None


    def get_bucket_name(self) -> str:
        '''
        Returns the name of the bucket to which \
        a connection has been established.
        '''
        return self.__bucket_name
    

    def is_open(self) -> bool:
        '''
        Returns a value indicating whether \
        this handler's underlying client connection \
        is open or not.
        '''
        return self.__bucket is not None


    def open_connections(self) -> None:
        '''
        Opens an HTTP connection to the Amazon S3 bucket.
        '''

        if self.__bucket is not None:
            return

        print(f"\nEstablishing connection to '{self.__bucket_name}' Amazon S3 bucket...")
        self.__bucket = _boto3.resource(
            service_name='s3',
            **self.__auth.get_credentials()
        ).Bucket(self.__bucket_name)
        # Ensure that bucket exists.
        try:
            self.__bucket.meta.client.head_bucket(Bucket=self.__bucket_name)
        except _CE:
            self.__bucket = None
            raise _BNFE(bucket=self.__bucket_name)

        print("Connection established.")


    def close_connections(self):
        '''
        Closes the HTTP connection to the Amazon S3 bucket.
        '''
        if self.__bucket is not None:
            self.__bucket.meta.client.close()
            self.__bucket = None


    def path_exists(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path exists \
        within the directory, else returns ``False``.

        :param str path: An absolute path.
        '''
        try:
            self.__bucket.Object(path).load()
        except _CE:
            return False
        return True


    def is_file(self, file_path: str) -> bool:
        '''
        Returns ``True`` if the provided path points \
        to a file, else returns ``False``.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        return not file_path.endswith('/')
    

    def dir_exists(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path exists \
        and is a directory, else returns ``False``.

        :param str path: Either an absolute path or a \
            path relative to the parent directory.
        '''
        for _ in self.__bucket.objects.filter(Prefix=path):
            return True
        return False
        
    
    def mkdir(self, path: str) -> None:
        '''
        Creates a directory into the provided path.

        :param str path: The path of the directory \
            that is to be created.
        '''
        self.__bucket.put_object(
            Key=path,
            ContentType='application/x-directory; charset=UTF-8')
        

    def get_reader(self, file_path: str) -> _AmazonS3FileReader:
        '''
        Returns an ``AmazonS3FileReader`` class instance \
        used for reading from a file which resides within \
        an Amazon S3 bucket.

        :param str file_path: The absolute path of \
            the file in question.
        '''
        return _AmazonS3FileReader(
            file_path=file_path,
            file_size=self.get_file_size(file_path),
            bucket=self.__bucket)


    def get_writer(
        self,
        file_path: str,
        metadata: _Optional[dict[str, str]],
        chunk_size: _Optional[int]
    ) -> _AmazonS3FileWriter:
        '''
        Returns an ``AmazonS3FileWriter`` class instance \
        used for writing to a file which resides within \
        an Amazon S3 bucket.

        :param str file_path: The absolute path of \
            the file in question.
        :param dict[str, str] | None metadata: A \
            dictionary containing the metadata that \
            are to be assigned to the file in question. \
            If ``None``, then no metadata are assigned.
        :param int | None chunk_size: Indicates whether \
            the size of distinct chunks in which the file \
            is written. If ``None``, then the file is to be \
            written as a single chunk of bytes.
        '''
        return _AmazonS3FileWriter(
            file_path=file_path,
            metadata=metadata,
            chunk_size=chunk_size,
            bucket=self.__bucket)
    

    def _get_file_size_impl(self, file_path) -> int:
        '''
        Fetches and returns the size of a file in bytes.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        return self.__bucket.Object(key=file_path).content_length
    

    def _get_file_metadata_impl(self, file_path: str) -> dict[str, str]:
        '''
        Fetches and returns a dictionary containing the metadata of a file.

        :param str file_path: The path of the file in question.
        '''
        return self.__bucket.Object(key=file_path).metadata


    def _traverse_dir_impl(
        self,
        dir_path: str,
        recursively: bool,
        show_abs_path: bool
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through \
        the directory's contents as their paths.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories.
        :param bool show_abs_path: Indicates whether it \
            should be displayed the absolute or the relative \
            path of the contents.

        :note: The resulting iterator may vary depending on the \
            value of parameter ``recursively``.
        '''
        paginator = self.__bucket.meta.client.get_paginator('list_objects')

        sep = _infer_sep(dir_path)

        delimiter = '' if recursively else sep

        def page_iterator():
            yield from paginator.paginate(
                Bucket=self.__bucket_name,
                Prefix=dir_path,
                Delimiter=delimiter)

        def object_iterator(response):
            for obj in response.get('Contents', []):
                file_path = obj['Key']
                yield file_path if show_abs_path else \
                    _relativize(dir_path, file_path, sep)
                
        if recursively:
            for response in page_iterator():
                yield from object_iterator(response)
        else:

            def dir_iterator(response):
                for dir in response.get('CommonPrefixes', []):
                    path = dir['Prefix']
                    yield path if show_abs_path else \
                        _relativize(dir_path, path, sep)
                    
            for response in page_iterator():
                        
                obj_iter = object_iterator(response)
                dir_iter = dir_iterator(response)

                obj = next(obj_iter, None)
                dir = next(dir_iter, None)

                while True:
                    if obj is None and dir is None:
                        break
                    elif obj is None and dir is not None:
                        yield dir
                        dir = next(dir_iter, None)
                    elif obj is not None and dir is None:
                        yield obj
                        obj = next(obj_iter, None)
                    elif obj > dir:
                        yield dir
                        dir = next(dir_iter, None)
                    else:
                        yield obj
                        obj = next(obj_iter, None)


class AzureClientHandler(ClientHandler):
    '''
    A class used in handling the HTTP \
    connection to an Azure blob container.

    :param AzureAuth auth: An ``AzureAuth`` instance \
        used in authenticating with Microsoft Azure.
    :param str container: The name of the Azure blob \
        container to which a connection is to be established.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access.
    '''

    def __init__(
        self,
        auth: _AzureAuth,
        container: str,
        cache: bool
    ):
        '''
        A class used in handling the HTTP \
        connection to an Azure blob container.

        :param AzureAuth auth: An ``AzureAuth`` instance \
            used in authenticating with Microsoft Azure.
        :param str container: The name of the Azure blob \
            container to which a connection is to be established.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access.
        '''
        super().__init__(cache=cache)
        self.__auth = auth
        self.__container_name = container
        self.__container = None


    def container_exists(self) -> bool:
        '''
        Returns a value indicating whether the \
        specified container exists or not.
        '''
        return self.__container.exists()


    def get_container_name(self) -> str:
        '''
        Returns the name of the container to which \
        a connection has been established.
        '''
        return self.__container_name
    

    def is_open(self) -> bool:
        '''
        Returns a value indicating whether \
        this handler's underlying client connection \
        is open or not.
        '''
        return self.__container is not None


    def open_connections(self) -> None:
        '''
        Opens an HTTP connection to the Azure blob container.
        '''
        if self.__container is not None:
            return

        credentials = self.__auth.get_credentials()

        print(f"\nEstablishing connection to '{self.__container_name}' Azure blob container...")
        if 'conn_string' in credentials:
            self.__container = _ContainerClient.from_connection_string(
                conn_str=credentials['conn_string'],
                container_name=self.__container_name)
        else:
            self.__container = _ContainerClient(
                account_url=credentials.pop('account_url'),
                container_name=self.__container_name,
                credential=_CSC(**credentials))
        print("Connection established!")

    def close_connections(self):
        '''
        Closes the HTTP connection to the Azure blob container.
        '''
        if self.__container is not None:
            self.__container.close()
            self.__container = None


    def path_exists(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path exists \
        within the directory, else returns ``False``.

        :param str path: An absolute path.
        '''
        with self.__container.get_blob_client(blob=path) as blob:
            return blob.exists()
        

    def is_file(self, file_path: str) -> bool:
        '''
        Returns ``True`` if the provided path points \
        to a file, else returns ``False``.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        return not file_path.endswith('/')
    

    def mkdir(self, path: str) -> None:
        '''
        Creates a directory into the provided path.

        :param str path: The path of the directory \
            that is to be created.
        '''
        with self.__container.get_blob_client(blob=f"{path}DUMMY") as blob:
            blob.create_append_blob()
            blob.delete_blob()


    def get_reader(self, file_path: str) -> _AzureBlobReader:
        '''
        Returns an ``AzureBlobReader`` class instance \
        used for reading from a file which resides within \
        an Azure blob container.

        :param str file_path: The absolute path of \
            the file in question.
        '''
        return _AzureBlobReader(
            file_path=file_path,
            file_size=self.get_file_size(file_path),
            container=self.__container)


    def get_writer(
        self,
        file_path: str,
        metadata: _Optional[dict[str, str]],
        chunk_size: _Optional[int]
    ) -> _AzureBlobWriter:
        '''
        Returns an ``AzureBlobWriter`` class instance \
        used for writing to a file which resides within \
        an Azure blob container.

        :param str file_path: The absolute path of \
            the file in question.
        :param dict[str, str] | None metadata: A \
            dictionary containing the metadata that \
            are to be assigned to the file in question. \
            If ``None``, then no metadata are assigned.
        :param int | None chunk_size: Indicates whether \
            the size of distinct chunks in which the file \
            is written. If ``None``, then the file is to be \
            written as a single chunk of bytes.
        '''
        return _AzureBlobWriter(
            file_path=file_path,
            metadata=metadata,
            chunk_size=chunk_size,
            container=self.__container)
     

    def _get_file_size_impl(self, file_path) -> int:
        '''
        Fetches and returns the size of a file in bytes.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        return self.__container.download_blob(blob=file_path).size
    

    def _get_file_metadata_impl(self, file_path: str) -> dict[str, str]:
        '''
        Fetches and returns a dictionary containing the \
        metadata of a file.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        return self.__container.download_blob(
            blob=file_path).properties.metadata


    def _traverse_dir_impl(
        self,
        dir_path: str,
        recursively: bool,
        show_abs_path: bool
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through \
        the directory's contents as their paths.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories.
        :param bool show_abs_path: Indicates whether it \
            should be displayed the absolute or the relative \
            path of the contents.

        :note: The resulting iterator may vary depending on the \
            value of parameter ``recursively``.
        '''
        sep = _infer_sep(dir_path)

        if recursively:
            '''
            NOTE:
                Due to certain storage accounts having the
                hierarchical namespace feature enabled,
                virtual folders may appear as ordinary
                blobs. For this reason, consider that any
                blob whose size is equal to zero is a virtual folder.

                Issue: https://github.com/Azure/azure-sdk-for-python/issues/29026
            '''
            iterable = filter(
                lambda p: p['size'] > 0,
                self.__container.list_blobs(
                    name_starts_with=dir_path))
        else:
            iterable = self.__container.walk_blobs(
                name_starts_with=dir_path, delimiter=sep)
                    
        for properties in iterable:
            if show_abs_path:
                yield properties['name']
            else:
                yield _relativize(dir_path, properties['name'], sep)



class GCPClientHandler(ClientHandler):
    '''
    A class used in handling the HTTP \
    connection to a Google Cloud Storage bucket.

    :param GCPAuth auth: A ``GCPAuth`` instance \
        used in authenticating with Google Cloud Platform.
    :param str bucket: The name of the Google Cloud Storage \
        bucket to which a connection is to be established.
    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access.
    '''

    # NOTE: This is used instead of directly instantiating a client
    #       due to certain issues while attempting to mock the client
    #       by patching the class' method ``__new__``.
    _CLIENT_GENERATOR = lambda _, project_id: _GCSClient(project=project_id)

    def __init__(
        self,
        auth: _GCPAuth,
        bucket: str,
        cache: bool
    ):
        '''
        A class used in handling the HTTP \
        connection to a Google Cloud Storage bucket.

        :param GCPAuth auth: A ``GCPAuth`` instance \
            used in authenticating with Google Cloud Platform.
        :param str bucket: The name of the Google Cloud Storage \
            bucket to which a connection is to be established.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access.
        '''
        super().__init__(cache=cache)
        self.__auth = auth
        self.__bucket_name = bucket
        self.__bucket = None


    def bucket_exists(self) -> bool:
        '''
        Returns a value indicating whether the \
        specified bucket exists or not.
        '''
        self.__bucket.exists()


    def get_bucket_name(self) -> str:
        '''
        Returns the name of the bucket to which \
        a connection has been established.
        '''
        return self.__bucket_name
    

    def is_open(self) -> bool:
        '''
        Returns a value indicating whether \
        this handler's underlying client connection \
        is open or not.
        '''
        return self.__bucket is not None


    def open_connections(self) -> None:
        '''
        Opens an HTTP connection to the \
        Google Cloud Storage bucket.
        '''
        if self.__bucket is not None:
            return

        credentials = self.__auth.get_credentials()

        print(f"\nEstablishing connection to '{self.__bucket_name}' Google Cloud Storage bucket...")

        if _GCPAuth._APPLICATION_DEFAULT_CREDENTIALS in credentials:
            _os.environ.update({
                "GOOGLE_APPLICATION_CREDENTIALS":
                credentials[_GCPAuth._APPLICATION_DEFAULT_CREDENTIALS]
            })
            client = self._CLIENT_GENERATOR(
                project_id=credentials[_GCPAuth._PROJECT_ID])
        elif _GCPAuth._SERVICE_ACCOUNT_KEY in credentials:           
            client = _GCSClient.from_service_account_json(
                json_credentials_path=credentials[_GCPAuth._SERVICE_ACCOUNT_KEY])
        for bucket in client.list_buckets():
            if bucket.name == self.__bucket_name:
                self.__bucket = bucket
                break
        else:
            client.close()
            raise _BNFE(bucket=self.__bucket_name)

        print("Connection established!")


    def close_connections(self):
        '''
        Closes the HTTP connection to Google Cloud Storage.
        '''
        if self.__bucket is not None:
            self.__bucket.client.close()
            self.__bucket = None


    def path_exists(self, path: str) -> bool:
        '''
        Returns ``True`` if the provided path exists \
        within the directory, else returns ``False``.

        :param str path: An absolute path.
        '''
        for _ in self.__bucket.list_blobs(
            prefix=path,
            delimiter='/'
        ):    
            return True
        return False
        

    def is_file(self, file_path: str) -> bool:
        '''
        Returns ``True`` if the provided path points \
        to a file, else returns ``False``.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        return not file_path.endswith('/')
    

    def mkdir(self, path: str) -> None:
        '''
        Creates a directory into the provided path.

        :param str path: The path of the directory \
            that is to be created.
        '''
        blob = self.__bucket.blob(blob_name=f"{path}DUMMY")
        blob.upload_from_string(
            '',
            content_type='application/octet-stream')
        blob.delete()


    def get_reader(self, file_path: str) -> _GCPFileReader:
        '''
        Returns a ``GCPFileReader`` class instance \
        used for reading from a file which resides within \
        a Google Cloud Storage bucket.

        :param str file_path: The absolute path of \
            the file in question.
        '''
        return _GCPFileReader(
            file_path=file_path,
            file_size=self.get_file_size(file_path),
            bucket=self.__bucket)


    def get_writer(
        self,
        file_path: str,
        metadata: _Optional[dict[str, str]],
        chunk_size: _Optional[int]
    ) -> _GCPFileWriter:
        '''
        Returns a ``GCPFileWriter`` class instance \
        used for writing to a file which resides within \
        a Google Cloud Storage bucket.

        :param str file_path: The absolute path of \
            the file in question.
        :param dict[str, str] | None metadata: A \
            dictionary containing the metadata that \
            are to be assigned to the file in question. \
            If ``None``, then no metadata are assigned.
        :param int | None chunk_size: Indicates whether \
            the size of distinct chunks in which the file \
            is written. If ``None``, then the file is to be \
            written as a single chunk of bytes.
        '''
        return _GCPFileWriter(
            file_path=file_path,
            metadata=metadata,
            chunk_size=chunk_size,
            bucket=self.__bucket)
     

    def _get_file_size_impl(self, file_path) -> int:
        '''
        Fetches and returns the size of a file in bytes.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        return self.__bucket.get_blob(file_path).size
    

    def _get_file_metadata_impl(self, file_path: str) -> dict[str, str]:
        '''
        Fetches and returns a dictionary containing the \
        metadata of a file.

        :param str file_path: The absolute path of the \
            file in question.
        '''
        return dict() if (
            metadata := self.__bucket.get_blob(file_path).metadata
        ) is None else metadata
        


    def _traverse_dir_impl(
        self,
        dir_path: str,
        recursively: bool,
        show_abs_path: bool
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through \
        the directory's contents as their paths.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
        :param bool recursively: Indicates whether the directory \
            is to be traversed recursively or not. If set to  ``False``, \
            then only those files that reside directly within the \
            directory are to be considered. If set to ``True``, \
            then all files are considered, no matter whether they \
            reside directly within the directory or within any of \
            its subdirectories.
        :param bool show_abs_path: Indicates whether it \
            should be displayed the absolute or the relative \
            path of the contents.

        :note: The resulting iterator may vary depending on the \
            value of parameter ``recursively``.
        '''
        sep = '/'

        if recursively:
            for blob in self.__bucket.list_blobs(
                prefix=dir_path
            ):
                if blob.name != dir_path:  
                    if show_abs_path:
                        yield blob.name
                    else:
                        yield _relativize(dir_path, blob.name, sep)
        else:

            from google.api_core import page_iterator

            dir_iter = page_iterator.HTTPIterator(
                client=self.__bucket.client,
                api_request=self.__bucket.client._connection.api_request,
                path=f"/b/{self.get_bucket_name()}/o",
                items_key='prefixes',
                item_to_value=lambda _, item: item,
                extra_params={
                    "projection": "noAcl",
                    "prefix": dir_path,
                    "delimiter": '/'})

            obj_iter = filter(
                lambda obj: obj != dir_path,
                map(
                    lambda obj: obj.name,
                    self.__bucket.list_blobs(
                        prefix=dir_path,
                        delimiter='/')))

            obj = next(obj_iter, None)
            dir = next(dir_iter, None)
        
            name_fun = lambda name: (
                name if show_abs_path
                else _relativize(dir_path, name, sep))

            while True:
                if obj is None and dir is None:
                    break
                elif obj is None and dir is not None:
                    yield name_fun(dir)
                    dir = next(dir_iter, None)
                elif obj is not None and dir is None:
                    yield name_fun(obj)
                    obj = next(obj_iter, None)
                elif obj > dir:
                    yield name_fun(dir)
                    dir = next(dir_iter, None)
                else:
                    yield name_fun(obj)
                    obj = next(obj_iter, None)
