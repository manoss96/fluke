from typing import Any as _Any
from typing import Optional as _Optional
from typing import Iterator as _Iterator
from abc import ABC as _ABC
from abc import abstractmethod as _absmethod


import boto3 as _boto3
import paramiko as _prmk
from azure.identity import ClientSecretCredential as _CSC
from azure.storage.blob import ContainerClient as _ContainerClient


from ._helper import join_paths as _join_paths
from ._helper import infer_separator as _infer_sep
from ._helper import relativize_path as _relativize
from .auth import AWSAuth as _AWSAuth
from .auth import AzureAuth as _AzureAuth
from .auth import RemoteAuth as _RemoteAuth
from ._exceptions import UnknownKeyTypeError as _UKTE


class ClientHandler(_ABC):
    '''
    An abstract class which serves as the \
    base class for all client-like classes.
    '''

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
    def get_file_size(self, file_path: str) -> int:
        '''
        Returns the size of a file in bytes.

        :param str file_path: The path of the file in question.
        '''
        pass


    @_absmethod
    def iterate_contents(
        self,
        dir_path: str,
        recursively: bool = False,
        show_abs_path: bool = False
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through the paths \
        of the dictionary's contents as strings.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
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
        pass


class SSHClientHandler(ClientHandler):
    '''
    A class used in handling the SSH and SFTP \
    connections to a remote server.

    :param RemoteAuth auth: A ``RemoteAuth`` \
        instance used in authenticating with a remote machine.
    '''

    def __init__(self, auth: _RemoteAuth):
        '''
        A class used in handling the SSH and SFTP \
        connections to a remote server.

        :param RemoteAuth auth: A ``RemoteAuth`` \
            instance used in authenticating with a remote machine.
        '''
        self.__auth: _RemoteAuth = auth
        self.__ssh: _prmk.SSHClient = None
        self.__sftp: _prmk.SFTPClient = None


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


    def get_file_size(self, file_path) -> int:
        '''
        Returns the size of a file in bytes.

        :param str file_path: The path of the file in question.
        '''
        return self.__sftp.stat(path=file_path).st_size


    def iterate_contents(
        self,
        dir_path: str,
        recursively: bool = False,
        show_abs_path: bool = False
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through the paths \
        of the dictionary's contents as strings.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
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
                        else self._relativize(path=file_path))
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
    '''

    def __init__(self, auth: _AWSAuth, bucket: str):
        '''
        A class used in handling the HTTP \
        connection to an Amazon S3 bucket.

        :param AWSAuth auth: An ``AWSAuth`` instance \
            used in authenticating with AWS.
        :param str bucket: The name of the Amazon S3 bucket \
            to which a connection is to be established.
        '''
        self.__auth = auth
        self.__bucket_name = bucket
        self.__bucket = None


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


    def get_file_size(self, file_path) -> int:
        '''
        Returns the size of a file in bytes.

        :param str file_path: The path of the file in question.
        '''
        return self.__bucket.Object(key=file_path).content_length


    def iterate_contents(
        self,
        dir_path: str,
        recursively: bool = False,
        show_abs_path: bool = False
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through the paths \
        of the dictionary's contents as strings.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
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
    '''

    def __init__(self, auth: _AzureAuth, container: str):
        '''
        A class used in handling the HTTP \
        connection to an Azure blob container.

        :param AzureAuth auth: An ``AzureAuth`` instance \
            used in authenticating with Microsoft Azure.
        :param str container: The name of the Azure blob \
            container to which a connection is to be established.
        '''
        self.__auth = auth
        self.__container_name = container
        self.__container = None


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


    def get_file_size(self, file_path) -> int:
        '''
        Returns the size of a file in bytes.

        :param str file_path: The path of the file in question.
        '''
        return self.__container.download_blob(blob=file_path).size


    def iterate_contents(
        self,
        dir_path: str,
        recursively: bool = False,
        show_abs_path: bool = False
    ) -> _Iterator[str]:
        '''
        Returns an iterator capable of going through the paths \
        of the dictionary's contents as strings.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
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