from typing import Any as _Any


class InvalidPathError(Exception):
    '''
    This exception is thrown whenever the user \
    a path that does not exist.

    :param str path: The path that was provided by the user.
    '''

    def __init__(self, path: str):
        '''
        This exception is thrown whenever the user \
        a path that does not exist.

        :param str path: The path that was provided by the user.
        '''
        msg = f'Path "{path}" does not exist.'
        super().__init__(msg)


class InvalidDirectoryError(Exception):
    '''
    This exception is thrown whenever the user provides \
    a path that does not point to a directory.

    :param str path: The path that was provided by the user.
    '''

    def __init__(self, path: str):
        '''
        This exception is thrown whenever the user provides \
        a path that does not point to a directory.
        
        :param str path: The path that was provided by the user.
        '''
        msg = f'Path "{path}" does not point to a directory.'
        super().__init__(msg)


class InvalidFileError(Exception):
    '''
    This exception is thrown whenever the user provides \
    a path that does not point to a file.

    :param str path: The path that was provided by the user.
    '''

    def __init__(self, path: str):
        '''
        This exception is thrown whenever the user provides \
        a path that does not point to a file.
        
        :param str path: The path that was provided by the user.
        '''
        msg = f'Path "{path}" does not point to a file.'
        super().__init__(msg)


class NonStringMetadataKeyError(Exception):
    '''
    This exception is thrown whenever the user provides \
    a metadata key that is not of type ``str``.

    :param Any key: The key provided by the user.
    '''

    def __init__(self, key: _Any):
        '''
        This exception is thrown whenever the user provides \
        a metadata key that is not of type ``str``.

        :param Any key: The key provided by the user.
        '''
        msg = f'Key "{key}" is not of type "str".'
        super().__init__(msg)


class NonStringMetadataValueError(Exception):
    '''
    This exception is thrown whenever the user provides \
    a metadata value that is not of type ``str``.

    :param Any val: The value provided by the user.
    '''

    def __init__(self, val: _Any):
        '''
        This exception is thrown whenever the user provides \
        a metadata value that is not of type ``str``.

        :param Any val: The value provided by the user.
        '''
        msg = f'Value "{val}" is not of type "str".'
        super().__init__(msg)


class AzureBlobContainerNotFoundError(Exception):
    '''
    This exception is thrown whenever the user provides \
    an Azure blob container that does not exist.

    :param str container: The name of the container that \
        was provided by the user.
    '''

    def __init__(self, container: str):
        '''
        This exception is thrown whenever the user provides \
        a container that does not exist.

        :param str container: The name of the container that \
            was provided by the user.
        '''
        msg = f'No container having the name "{container}" exists.'
        super().__init__(msg)


class UnknownKeyTypeError(Exception):
    '''
    This exception is thrown whenever the user provides \
    an unknown public key type.

    :param str key_type: The type of the key that was provided.
    '''

    def __init__(self, key_type: str):
        '''
        This exception is thrown whenever the user provides \
        an unknown public key type.

        :param str key_type: The type of the key that was provided.
        '''
        msg = f'Key type "{key_type}" is not supported.'
        super().__init__(msg)


class OverwriteError(Exception):
    '''
    This exception is thrown whenever the user tries \
    to overwrite a file without having set ``overwrite`` \
    to ``True``.

    :param str file_path: The path of the file that is \
        attempted to be overwritten.
    '''

    def __init__(self, file_path: str):
        '''
        This exception is thrown whenever the user tries \
        to overwrite a file without having set ``overwrite`` \
        to ``True``.

        :param str file_path: The path of the file that is \
            attempted to be overwritten.
        '''
        msg = f'There already exists file "{file_path}". '
        msg += 'Try setting "overwrite" to "True".'
        super().__init__(msg)