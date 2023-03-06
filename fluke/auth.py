

__all__ = [
    'AWSAuth',
    'AzureAuth',
    'RemoteAuth'
]


import re as _re
from enum import Enum as _Enum
from typing import Optional as _Optional


class RemoteAuth():
    '''
    This class is used for authenticating with a \
    remote machine via the SSH protocol.
    '''

    class KeyType(_Enum):
        '''
        This enum-class represents various types \
        of SSH keys.
        '''
        SSH_RSA = "ssh-rsa"
        SSH_DSS = "ssh-dss"
        SSH_ED25519 = "ssh-ed25519"
        ECDSA_SHA2_NISTP256 = "ecdsa-sha2-nistp256"
        ECDSA_SHA2_NISTP384 = "ecdsa-sha2-nistp384"
        ECDA_SHA_NISTP521 = "ecdsa-sha2-nistp521"


    @classmethod
    def from_password(
        cls,
        hostname: str,
        username: str,
        password: str,
        port: int = 22,
        public_key: _Optional[str] = None,
        key_type: _Optional[KeyType] = None,
        verify_host: bool = True
    )-> 'RemoteAuth':
        '''
        Returns a ``RemoteAuth`` instance used in authenticating \
        with a remote machine via password.

        :param str hostname: The remote machine's host name.
        :param str username: The name of the user you will \
            be logging in as.
        :param str password: The user's password.
        :param int port: The port to which you will be connecting. \
            Defaults to ``22``.
        :param str | None public_key: The host's public SSH key. \
            Defaults to ``None``.
        :param KeyType | None key_type: The type of the host's \
            public SSH key. Defaults to ``None``.
        :param bool verify_host: Unless set to ``False``, a connection \
            can only be established if the host is known to the local \
            machine. Defaults to ``True``.
        '''
        auth = cls()
        auth.__credentials = {
            'hostname': hostname,
            'username': username,
            'password': password,
            'port': port,
            'public_key': public_key,
            'key_type': key_type if key_type is None else key_type.value,
            'verify_host': verify_host
        }  
        return auth


    @classmethod
    def from_key(
        cls,
        hostname: str,
        username: str,
        pkey: str,
        passphrase: _Optional[str] = None,
        port: int = 22,
        public_key: _Optional[str] = None,
        key_type: _Optional[KeyType] = None,
        verify_host: bool = True
    )-> 'RemoteAuth':
        '''
        Returns a ``RemoteAuth`` instance used in authenticating \
        with a remote machine via an SSH key.

        :param str hostname: The remote machine's host name.
        :param str username: The name of the user you will \
            be logging in as.
        :param str pkey: A path pointing to a file containing \
            your machine's private SSH key.
        :param str | None passphrase: A passphrase used for decrypting \
            the private key, only to be used in case it has been previously \
            encrypted. Defaults to ``None``.
        :param int port: The port to which you will be connecting. \
            Defaults to ``22``.
        :param str | None public_key: The host's public SSH key. \
            Defaults to ``None``.
        :param KeyType | None key_type: The type of the host's \
            public SSH key. Defaults to ``None``.
        :param bool verify_host: Unless set to ``False``, a connection \
            can only be established if the host is known to the local \
            machine. Defaults to ``True``.
        '''
        auth = cls()
        auth.__credentials = {
            'hostname': hostname,
            'username': username,
            'pkey': pkey,
            'passphrase': passphrase,
            'port': port,
            'public_key': public_key,
            'key_type': key_type if key_type is None else key_type.value,
            'verify_host': verify_host
        }  
        return auth


    def get_credentials(self) -> dict[str, str]:
        '''
        Returns the provided credentials stored \
        within a dictionary.
        '''
        return dict(self.__credentials)


class AWSAuth():
    '''
    This class is used for authenticating with AWS.

    :param str aws_access_key_id: The access key for your AWS account.
    :param str aws_secret_access_key: The secret key for your AWS account.
    :param str | None aws_session_token: The session key for your AWS account. \
        Defaults to ``None``.
    :param AWSAuth.Region | None region: The AWS Region used in instantiating \
        the client. Defaults to ``None``.
    '''


    class Region(_Enum):
        '''
        This enum-class represents various \
        AWS regions.
        '''
        AFRICA_SOUTH_1 = "af-south-1"
        ASIA_PACIFIC_EAST_1 = "ap-east-1"
        ASIA_PACIFIC_NORTHEAST_1 = "ap-northeast-1"
        ASIA_PACIFIC_NORTHEAST_2 = "ap-northeast-2"
        ASIA_PACIFIC_NORTHEAST_3 = "ap-northeast-3"
        ASIA_PACIFIC_SOUTH_1 = "ap-south-1"
        ASIA_PACIFIC_SOUTH_2 = "ap-south-2"
        ASIA_PACIFIC_SOUTHEAST_1 = "ap-southeast-1"
        ASIA_PACIFIC_SOUTHEAST_2 = "ap-southeast-2"
        ASIA_PACIFIC_SOUTHEAST_3 = "ap-southeast-3"
        ASIA_PACIFIC_SOUTHEAST_4 = "ap-southeast-4"
        CANADA_CENTRAL_1 = "ca-central-1"
        EUROPE_CENTRAL_1 = "eu-central-1"
        EUROPE_CENTRAL_2 = "eu-central-2"
        EUROPE_NORTH_1 = "eu-north-1"
        EUROPE_SOUTH_1 = "eu-south-1"
        EUROPE_WEST_1 = "eu-west-1"
        EUROPE_WEST_2 = "eu-west-2"
        EUROPE_WEST_3 = "eu-west-3"
        MIDDLE_EAST_CENTRAL_1 = "me-central-1"
        SOUTH_AMERICA_EAST_1 = "sa-east-1"
        US_EAST_1 = "us-east-1"
        US_EAST_2 = "us-east-2"
        US_GOV_EAST_1 = "us-gov-east-1"
        US_GOV_EAST_2 = "us-gov-east-2"
        US_WEST_1 = "us-west-1"
        US_WEST_2 = "us-west-2"


    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: _Optional[str] = None,
        region: _Optional[Region] = None
    ):
        '''
        This class is used for authenticating with AWS.

        :param str aws_access_key_id: The access key for your AWS account.
        :param str aws_secret_access_key: The secret key for your AWS account.
        :param str | None aws_session_token: The session key for your AWS account. \
            Defaults to ``None``.
        :param AWSAuth.Region | None region: The AWS Region used in instantiating \
            the client. Defaults to ``None``.
        '''
        
        self.__credentials = {
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
            'aws_session_token': aws_session_token,
            'region_name': region.value if region is not None else None
        }


    def get_credentials(self) -> dict[str, str]:
        '''
        Returns the provided credentials stored \
        within a dictionary.
        '''
        return dict(self.__credentials)


class AzureAuth():
    '''
    This class is used for authenticating with Microsoft Azure.

    :param str account_url: The URI to the storage account.
    :param str tenant_id: ID of the service principal's tenant.
    :param str client_id: The service principal's client ID.
    :param str client_secret: One of the service principal's client secrets.
    '''

    def __init__(
        self,
        account_url: str,
        tenant_id: str,
        client_id: str,
        client_secret: str
    ):
        '''
        This class is used for authenticating with Microsoft Azure.

        :param str account_url: The URI to the storage account.
        :param str tenant_id: ID of the service principal's tenant.
        :param str client_id: The service principal's client ID.
        :param str client_secret: One of the service principal's client secrets.
        '''
        self.__credentials = {
            'account_url': account_url,
            'tenant_id': tenant_id,
            'client_id': client_id,
            'client_secret': client_secret
        }
        if account_url is not None:
            match = _re.match(
                pattern=r"https?://([^.]+).blob.core.windows.net/?",
                string=account_url)
            if match is not None:
                self.__storage_account = match.group(1)
        else:
            self.__storage_account = None


    @classmethod
    def from_conn_string(cls, conn_string: str) -> 'AzureAuth':
        '''
        Returns an ``AzureAuth`` instance used for \
        authenticating with Microsoft Azure via a \
        connection string.
        '''
        auth = cls(None, None, None, None)
        auth.__credentials = {
            'conn_string': conn_string
        }
        match = _re.search(
            pattern=r"AccountName=([^.;]+);?",
            string=conn_string)
        if match is not None:
            auth.__storage_account = match.group(1)     
        return auth


    def get_credentials(self) -> dict[str, str]:
        '''
        Returns the provided credentials stored \
        within a dictionary.
        '''
        return dict(self.__credentials)
    

    def _get_storage_account(self) -> str:
        '''
        Returns the storage account that is \
        associated with this instance.
        '''
        return self.__storage_account