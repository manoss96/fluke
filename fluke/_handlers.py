import os as _os
import io as _io
from abc import ABC as _ABC
from abc import abstractmethod as _absmethod
from shutil import copyfileobj as _copyfileobj
from typing import Optional as _Optional
from typing import Iterator as _Iterator


import boto3 as _boto3
import paramiko as _prmk
from azure.identity import ClientSecretCredential as _CSC
from azure.storage.blob import ContainerClient as _ContainerClient


from .auth import AWSAuth as _AWSAuth
from .auth import AzureAuth as _AzureAuth
from .auth import RemoteAuth as _RemoteAuth
from ._cache import CacheManager as _CacheManager
from ._exceptions import UnknownKeyTypeError as _UKTE
from ._helper import join_paths as _join_paths
from ._helper import infer_separator as _infer_sep
from ._helper import relativize_path as _relativize


class ClientHandler(_ABC):
    '''
    An abstract class which serves as the \
    base class for all client-like classes.

    :param bool cache: Indicates whether it is allowed for \
        any fetched data to be cached for faster subsequent \
        access. Defaults to ``False``.
    '''

    def __init__(self, cache: bool):
        '''
        An abstract class which serves as the \
        base class for all client-like classes.

        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access. Defaults to ``False``.
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
            self.__cache_manager = _CacheManager()


    def get_file_size(self, file_path: str) -> int:
        '''
        Returns the size of a file in bytes.
        
        :param str file_path: The path of the file in question.

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
        
        :param str file_path: The path of the file in question.
        
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
            if (metadata := self.__cache_manager.get_metadata(file_path=file_path)) is not None:
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
        Returns an iterator capable of going through the paths \
        of the dictionary's contents as strings.

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
                    iterator=iterator,
                    recursively=recursively,
                    is_file=self.is_file)
                # Reset iterator by grabbing it from cache.
                iterator = self.__cache_manager.get_content_iterator(
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
    def read(
        self,
        file_path: str,
        buffer: _io.BytesIO,
        include_metadata: bool
    ) -> _Optional[dict[str, str]]:
        '''
        Reads the bytes of a file into the provided buffer. \
        Returns either ``None`` or a dictionary containing \
        file metadata, depending on the value of parameter \
        ``include_metadata``.

        :param str file_path: The absolute path of the \
            file in question.
        :param BytesIO buffer: A buffer to download the \
            file into.
        :param bool include_metadata: Indicates whether \
            to download any existing file metadata as well.
        '''
        pass


    @_absmethod
    def write(
        self,
        file_path: str,
        buffer: _io.BytesIO,
        metadata: _Optional[dict[str, str]]
    ) -> None:
        '''
        Writes the bytes contained within the provided \
        buffer into the specified path.

        :param str file_path: The absolute path of the \
            file in question.
        :param BytesIO buffer: A buffer containing the \
            file's bytes.
        :param dict[str, str] | None: If not ``None``, \
            then assigns the provided metadata to the file \
            during the upload.
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
        Returns an iterator capable of going through the paths \
        of the dictionary's contents as strings.

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

        :param str path: Either an absolute path or a \
            path relative to the directory.
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
    

    def read(
        self,
        file_path: str,
        buffer: _io.BytesIO,
        include_metadata: bool
    ) -> _Optional[dict[str, str]]:
        '''
        Reads the bytes of a file into the provided buffer. \
        Returns either ``None`` or a dictionary containing \
        file metadata, depending on the value of parameter \
        ``include_metadata``.

        :param str file_path: The absolute path of the \
            file in question.
        :param BytesIO buffer: A buffer to download the \
            file into.
        :param bool include_metadata: Indicates whether \
            to download any existing file metadata as well.
        '''
        with open(file=file_path, mode='rb') as file:
            _copyfileobj(fsrc=file, fdst=buffer)


    def write(
        self,
        file_path: str,
        buffer: _io.BytesIO,
        metadata: _Optional[dict[str, str]]
    ) -> None:
        '''
        Writes the bytes contained within the provided \
        buffer into the specified path.

        :param str file_path: The absolute path of the \
            file in question.
        :param BytesIO buffer: A buffer containing the \
            file's bytes.
        :param dict[str, str] | None: If not ``None``, \
            then assigns the provided metadata to the file \
            during the upload.
        '''
        # Create any necessary directories.
        self.mkdir(_os.path.dirname(file_path))
        # Write file.
        with open(file=file_path, mode='wb') as file:
            _copyfileobj(fsrc=buffer, fdst=file)
    

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
        Returns an iterator capable of going through the paths \
        of the dictionary's contents as strings.

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
        access. Defaults to ``False``.
    '''

    def __init__(self, auth: _RemoteAuth, cache: bool):
        '''
        A class used in handling the SSH and SFTP \
        connections to a remote server.

        :param RemoteAuth auth: A ``RemoteAuth`` instance used \
            for authenticating with a remote machine.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access. Defaults to ``False``.
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

        :param str path: Either an absolute path or a \
            path relative to the directory.
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
    

    def read(
        self,
        file_path: str,
        buffer: _io.BytesIO,
        include_metadata: bool
    ) -> _Optional[dict[str, str]]:
        '''
        Reads the bytes of a file into the provided buffer. \
        Returns either ``None`` or a dictionary containing \
        file metadata, depending on the value of parameter \
        ``include_metadata``.

        :param str file_path: The absolute path of the \
            file in question.
        :param BytesIO buffer: A buffer to download the \
            file into.
        :param bool include_metadata: Indicates whether \
            to download any existing file metadata as well.
        '''
        self.__sftp.getfo(remotepath=file_path, fl=buffer)


    def write(
        self,
        file_path: str,
        buffer: _io.BytesIO,
        metadata: _Optional[dict[str, str]]
    ) -> None:
        '''
        Writes the bytes contained within the provided \
        buffer into the specified path.

        :param str file_path: The absolute path of the \
            file in question.
        :param BytesIO buffer: A buffer containing the \
            file's bytes.
        :param dict[str, str] | None: If not ``None``, \
            then assigns the provided metadata to the file \
            during the upload.
        '''

        sep = _infer_sep(file_path)

        def get_parent_dir(file_path: str) -> _Optional[str]:
            '''
            Returns the path to the parent directory \
            of the provided file path. Returns ``None`` \
            if said directory is the root directory.

            :param str file_path: The path of the file \
                in question.
            '''
            file_path = file_path.rstrip(sep)
            if sep in file_path:
                return f"{sep.join(file_path.split(sep)[:-1])}{sep}"
            return None

        # Create any directories necessary.
        parent_dir, non_existing_dirs = file_path, []
        while (parent_dir := get_parent_dir(parent_dir)) is not None:
            if not self.path_exists(path=parent_dir):
                non_existing_dirs.append(parent_dir)
        for dir in reversed(non_existing_dirs):
            self.mkdir(path=dir)

        # Write file from buffer.
        self.__sftp.putfo(fl=buffer, remotepath=file_path)


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
        Returns an iterator capable of going through the paths \
        of the dictionary's contents as strings.

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
        access. Defaults to ``False``.
    '''

    def __init__(self, auth: _AWSAuth, bucket: str, cache: bool):
        '''
        A class used in handling the HTTP \
        connection to an Amazon S3 bucket.

        :param AWSAuth auth: An ``AWSAuth`` instance \
            used in authenticating with AWS.
        :param str bucket: The name of the Amazon S3 bucket \
            to which a connection is to be established.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access. Defaults to ``False``.
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

        self.__bucket = _boto3.resource(
            service_name='s3',
            **self.__auth.get_credentials()
        ).Bucket(self.__bucket_name)


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

        :param str path: Either an absolute path or a \
            path relative to the directory.
        '''
        from botocore.exceptions import ClientError as _CE
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
    

    def read(
        self,
        file_path: str,
        buffer: _io.BytesIO,
        include_metadata: bool
    ) -> _Optional[dict[str, str]]:
        '''
        Reads the bytes of a file into the provided buffer. \
        Returns either ``None`` or a dictionary containing \
        file metadata, depending on the value of parameter \
        ``include_metadata``.

        :param str file_path: The absolute path of the \
            file in question.
        :param BytesIO buffer: A buffer to download the \
            file into.
        :param bool include_metadata: Indicates whether \
            to download any existing file metadata as well.
        '''
        obj = self.__bucket.Object(key=file_path)
        obj.download_fileobj(Fileobj=buffer)
        if include_metadata:
            return obj.metadata
        

    def write(
        self,
        file_path: str,
        buffer: _io.BytesIO,
        metadata: _Optional[dict[str, str]]
    ) -> None:
        '''
        Writes the bytes contained within the provided \
        buffer into the specified path.

        :param str file_path: The absolute path of the \
            file in question.
        :param BytesIO buffer: A buffer containing the \
            file's bytes.
        :param dict[str, str] | None: If not ``None``, \
            then assigns the provided metadata to the file \
            during the upload.
        '''
        self.__bucket.upload_fileobj(
            Key=file_path,
            Fileobj=buffer,
            ExtraArgs={ "Metadata": metadata }
                if metadata is not None else None)
        

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
        Returns an iterator capable of going through the paths \
        of the dictionary's contents as strings.

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

        delimiter = '' if recursively else '/'

        sep = _infer_sep(dir_path)

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
        access. Defaults to ``False``.
    '''

    def __init__(self, auth: _AzureAuth, container: str, cache: bool):
        '''
        A class used in handling the HTTP \
        connection to an Azure blob container.

        :param AzureAuth auth: An ``AzureAuth`` instance \
            used in authenticating with Microsoft Azure.
        :param str container: The name of the Azure blob \
            container to which a connection is to be established.
        :param bool cache: Indicates whether it is allowed for \
            any fetched data to be cached for faster subsequent \
            access. Defaults to ``False``.
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

        if 'conn_string' in credentials:
            self.__container = _ContainerClient.from_connection_string(
                conn_str=credentials['conn_string'],
                container_name=self.__container_name)
        else:
            self.__container = _ContainerClient(
                account_url=credentials.pop('account_url'),
                container_name=self.__container_name,
                credential=_CSC(**credentials))


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

        :param str path: Either an absolute path or a \
            path relative to the directory.
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
    

    def read(
        self,
        file_path: str,
        buffer: _io.BytesIO,
        include_metadata: bool
    ) -> _Optional[dict[str, str]]:
        '''
        Reads the bytes of a file into the provided buffer. \
        Returns either ``None`` or a dictionary containing \
        file metadata, depending on the value of parameter \
        ``include_metadata``.

        :param str file_path: The absolute path of the \
            file in question.
        :param BytesIO buffer: A buffer to download the \
            file into.
        :param bool include_metadata: Indicates whether \
            to download any existing file metadata as well.
        '''
        blob = self.__container.download_blob(blob=file_path)
        blob.readinto(stream=buffer)
        if include_metadata:
            return blob.properties.metadata
        

    def write(
        self,
        file_path: str,
        buffer: _io.BytesIO,
        metadata: _Optional[dict[str, str]]
    ) -> None:
        '''
        Writes the bytes contained within the provided \
        buffer into the specified path.

        :param str file_path: The absolute path of the \
            file in question.
        :param BytesIO buffer: A buffer containing the \
            file's bytes.
        :param dict[str, str] | None: If not ``None``, \
            then assigns the provided metadata to the file \
            during the upload.
        '''
        self.__container.upload_blob(
            name=file_path,
            data=buffer,
            metadata=metadata if metadata is not None else None,
            overwrite=True)
        

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
        Returns an iterator capable of going through the paths \
        of the dictionary's contents as strings.

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