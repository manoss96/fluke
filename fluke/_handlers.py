from typing import Any as _Any
from typing import Optional as _Optional
from abc import ABC as _ABC
from abc import abstractmethod as _absmethod


import boto3 as _boto3
import paramiko as _prmk
from azure.identity import ClientSecretCredential as _CSC
from azure.storage.blob import ContainerClient as _ContainerClient


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
    def get_client(self) -> _Any:
        '''
        Returns the client through which the handler's \
        underlying connection is established.
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
    

    def get_client(self) -> _Optional[_prmk.SFTPClient]:
        '''
        Returns an SFTP client.
        '''
        return self.__sftp


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
        self.__s3_bucket = None
    

    def get_client(self) -> _Optional['_boto3.resources.factory.bucket.s3.Bucket']:
        '''
        Returns an Amazon S3 bucket resource.
        '''
        return self.__s3_bucket


    def open_connections(self) -> None:
        '''
        Opens an HTTP connection to the Amazon S3 bucket.
        '''

        if self.__s3_bucket is not None:
            return

        self.__s3_bucket = _boto3.resource(
            service_name='s3',
            **self.__auth.get_credentials()
        ).Bucket(self.__bucket_name)


    def close_connections(self):
        '''
        Closes the HTTP connection to the Amazon S3 bucket.
        '''
        if self.__s3_bucket is not None:
            self.__s3_bucket.meta.client.close()
            self.__s3_bucket = None


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
        self.__azr_container = None

    
    def get_client(self) -> _Optional[_ContainerClient]:
        '''
        Returns an Azure blob container client.
        '''
        return self.__azr_container


    def open_connections(self) -> None:
        '''
        Opens an HTTP connection to the Azure blob container.
        '''

        if self.__azr_container is not None:
            return

        credentials = self.__auth.get_credentials()

        if 'conn_string' in credentials:
            self.__azr_container = _ContainerClient.from_connection_string(
                conn_str=credentials['conn_string'],
                container_name=self.__container_name)
        else:
            self.__azr_container = _ContainerClient(
                account_url=credentials.pop('account_url'),
                container_name=self.__container_name,
                credential=_CSC(**credentials))


    def close_connections(self):
        '''
        Closes the HTTP connection to the Azure blob container.
        '''
        if self.__azr_container is not None:
            self.__azr_container.close()
            self.__azr_container = None