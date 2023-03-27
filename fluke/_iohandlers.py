import os as _os
import io as _io
from abc import ABC as _ABC
from abc import abstractmethod as _absmethod
from typing import Optional as _Optional


import paramiko as _prmk
import boto3 as _boto3
from azure.storage.blob import ContainerClient as _ContainerClient
from botocore.exceptions import ClientError as _BotoClientError


from ._helper import infer_separator as _infer_sep


class _IOHandler(_ABC):
    '''
    An abstract class which serves as the \
    base class for all Reader/Writer classes.

    :param str file_path: The path of the \
        handler's underlying file.
    :param int offset: The byte offset.
    '''

    def __init__(self, file_path: str, offset: int):
        '''
        An abstract class which serves as the \
        base class for all Reader/Writer classes.

        :param str file_path: The path of the \
            handler's underlying file.
        :param int offset: The byte offset.
        '''
        self.__offset = offset
        self.__file_path = file_path


    def get_file_path(self) -> str:
        '''
        Returns the path of the handler's \
        underlying file.
        '''
        return self.__file_path


    def get_offset(self) -> int:
        '''
        Returns the current offset.
        '''
        return self.__offset
    

    def update_offset(self, n: int) -> None:
        '''
        Updates the current byte offset by adding \
        the provided number to it.

        :param int n: The number that is to be added \
            to the offset.
        '''
        self.__offset += n


    @_absmethod
    def get_mode(self) -> str:
        '''
        Returns the file handler's mode.
        '''
        pass


    @_absmethod
    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        pass


    def __exit__(self, exc_type, exc_value, traceback) -> None:
        '''
        Exit the runtime context related to this object. 
        '''
        self.close()


class _FileReader(_IOHandler, _ABC):
    '''
    An abstract class which serves as the \
    base class for all Reader classes.
    '''

    def get_mode(self) -> str:
        '''
        Returns the file handler's mode.
        '''
        return 'rb'
    

    def __enter__(self) -> '_FileReader':
        '''
        Enter the runtime context related to this instance.
        '''
        return self
    

    def read(self, chunk_size: _Optional[int] = None) -> bytes:
        '''
        Returns a chunk of bytes read from the opened file.

        :param int | None chunk_size: If ``None`, then \
            this method goes on to read the whole file, \
            else reads a chunk of bytes whose size is \
            equal to this value. Defaults to ``None``.
        '''
        chunk = self._read_impl(chunk_size)
        print(self.get_offset())
        self.update_offset(len(chunk))
        return chunk


    @_absmethod
    def _read_impl(self, chunk_size: _Optional[int]) -> bytes:
        '''
        Returns a chunk of bytes read from the opened file.

        :param int | None chunk_size: If not ``None``, \
            then this method will go on to read a chunk \
            of bytes whose size is equal to this value.
            Defaults to ``None``.
        '''
        pass


class _FileWriter(_IOHandler, _ABC):
    '''
    An abstract class which serves as the \
    base class for all Writer classes.
    '''

    def get_mode(self) -> str:
        '''
        Returns the file handler's mode.
        '''
        return 'wb'
    

    def __enter__(self) -> '_FileWriter':
        '''
        Enter the runtime context related to this instance.
        '''
        return self
    

    def write(self, chunk: bytes) -> int:
        '''
        Writes the provided chunk to the opened file,
        and returns the number of bytes written.

        :param bytes chunk: The chunk of bytes that is \
            to be written to the file.
        '''
        n = self._write_impl(chunk=chunk)
        self.update_offset(n)
        return n


    @_absmethod
    def _write_impl(self, chunk: bytes) -> int:
        '''
        Writes the provided chunk to the opened file,
        and returns the number of bytes written.

        :param bytes chunk: The chunk of bytes that is \
            to be written to the file.
        '''
        pass


class LocalFileReader(_FileReader):
    '''
    A class used in reading from files which \
    reside within the local file system.

    :param str file_path: The absolute path of \
        the file in question.
    :param int offset: The byte offset. Defaults \
        to ``0``.
    '''

    def __init__(self, file_path: str, offset: int = 0) -> None:
        '''
        A class used in reading from files which \
        reside within the local file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param int offset: The byte offset. Defaults \
            to ``0``.
        '''
        self.__file = open(file=file_path, mode=self.get_mode())
        if offset > 0:
            self.__file.seek(offset)
        super().__init__(file_path=file_path, offset=offset)


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        self.__file.close()


    def _read_impl(self, chunk_size: _Optional[int]) -> bytes:
        '''
        Returns a chunk of bytes read from the opened file.

        :param int | None chunk_size: If ``None`, then \
            this method goes on to read the whole file, \
            else reads a chunk of bytes whose size is \
            equal to this value. Defaults to ``None``.
        '''
        if chunk_size is None:
            return self.__file.read()
        return self.__file.read(chunk_size)


class LocalFileWriter(_FileWriter):
    '''
    A class used in writing to files which \
    reside within the local file system.

    :param str file_path: The absolute path of \
        the file in question.
    :param int offset: The byte offset. Defaults \
        to ``0``.
    '''

    def __init__(self, file_path: str, offset: int = 0) -> None:
        '''
        A class used in writing to files which \
        reside within the local file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param int offset: The byte offset. Defaults \
            to ``0``.
        '''
        # Create necessary directories if they do not exist.
        _os.makedirs(name=_os.path.dirname(file_path), exist_ok=True)
        # Open file for writing.
        self.__file = open(file=file_path, mode=self.get_mode())
        if offset > 0:
            self.__file.seek(offset)
        super().__init__(file_path=file_path, offset=offset)


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        self.__file.close()


    def _write_impl(self, chunk: bytes) -> int:
        '''
        Writes the provided chunk to the opened file,
        and returns the number of bytes written.

        :param bytes chunk: The chunk of bytes that \
            is to be written to the file.
        '''
        n = self.__file.write(chunk)
        self.__file.flush()
        return n
    

class RemoteFileReader(_FileReader):
    '''
    A class used in reading from files which \
    reside within a remote file system.

    :param str file_path: The absolute path of \
        the file in question.
    :param SFTPClient sftp: An ``SFTPClient`` \
        class instance.
    :param int offset: The byte offset. Defaults \
        to ``0``.
    '''

    def __init__(
        self,
        file_path: str,
        sftp: _prmk.SFTPClient,
        offset: int = 0
    ) -> None:
        '''
        A class used in reading from files which \
        reside within a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param SFTPClient sftp: An ``SFTPClient`` \
            class instance.
        :param int offset: The byte offset. Defaults \
            to ``0``.
        '''
        self.__file: _prmk.SFTPFile = sftp.open(
            filename=file_path, mode=self.get_mode())
        if offset > 0:
            self.__file.seek(offset)
        super().__init__(file_path=file_path, offset=offset)


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        self.__file.close()


    def _read_impl(self, chunk_size: _Optional[int]) -> bytes:
        '''
        Returns a chunk of bytes read from the opened file.

        :param int | None chunk_size: If ``None`, then \
            this method goes on to read the whole file, \
            else reads a chunk of bytes whose size is \
            equal to this value. Defaults to ``None``.
        '''
        if chunk_size is None:
            return self.__file.read()
        return self.__file.read(chunk_size)


class RemoteFileWriter(_FileWriter):
    '''
    A class used in writing to files which \
    reside within a remote file system.

    :param str file_path: The absolute path of \
        the file in question.
    :param SFTPClient sftp: An ``SFTPClient`` \
        class instance.
    :param int offset: The byte offset. Defaults \
        to ``0``.
    '''

    def __init__(
        self,
        file_path: str,
        sftp: _prmk.SFTPClient,
        offset: int = 0
    ) -> None:
        '''
        A class used in writing to files which \
        reside within a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param SFTPClient sftp: An ``SFTPClient`` \
            class instance.
        :param int offset: The byte offset. Defaults \
            to ``0``.
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
            try:
                sftp.stat(path=parent_dir)
            except FileNotFoundError:
                non_existing_dirs.append(parent_dir)
        for dir in reversed(non_existing_dirs):
            sftp.mkdir(path=dir)

        self.__file: _prmk.SFTPFile = sftp.open(
            filename=file_path, mode=self.get_mode())
        if offset > 0:
            self.__file.seek(offset)
        super().__init__(file_path=file_path, offset=offset)
        

    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        self.__file.close()


    def _write_impl(self, chunk: bytes) -> int:
        '''
        Writes the provided chunk to the opened file,
        and returns the number of bytes written.

        :param bytes chunk: The chunk of bytes that \
            is to be written to the file.
        '''
        self.__file.write(chunk)
        self.__file.flush()
        return len(chunk)
    

class AWSS3FileReader(_FileReader):
    '''
    A class used in reading from files which \
    reside within an Amazon S3 bucket.

    :param str file_path: The absolute path of \
        the file in question.
    :param Bucket bucket: A ``Bucket`` class instance.
    :param int offset: The byte offset. Defaults \
        to ``0``.
    '''

    def __init__(
        self,
        file_path: str,
        bucket: '_boto3.resources.factory.s3.Bucket',
        offset: int = 0
    ) -> None:
        '''
        A class used in reading from files which \
        reside within a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param Bucket bucket: A ``Bucket`` class instance.
        :param int offset: The byte offset. Defaults \
            to ``0``.
        '''
        self.__file = bucket.Object(key=file_path)
        super().__init__(file_path=file_path, offset=offset)


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        # NOTE: No need to close file, just the
        #       underlying HTTPS connection which
        #       will be closed by the ``AWSClientHandler``
        #       class instance.
        pass


    def _read_impl(self, chunk_size: _Optional[int]) -> bytes:
        '''
        Returns a chunk of bytes read from the opened file.

        :param int | None chunk_size: If ``None`, then \
            this method goes on to read the whole file, \
            else reads a chunk of bytes whose size is \
            equal to this value. Defaults to ``None``.
        '''
        if chunk_size is None:
            return self.__file.get()['Body'].read()
        offset = self.get_offset()
        range = f"bytes={offset}-{offset + chunk_size - 1}"
        try:
            return self.__file.get(Range=range)['Body'].read()
        except _BotoClientError as ex:
            if "InvalidRange" in str(ex):
                return b""
            raise ex

            


class AWSS3FileWriter(_FileWriter):
    '''
    A class used in writing to files which \
    reside within an Amazon S3 bucket.

    :param str file_path: The absolute path of \
        the file in question.
    :param SFTPClient sftp: An ``SFTPClient`` \
        class instance.
    :param int offset: The byte offset. Defaults \
        to ``0``.
    '''

    def __init__(
        self,
        file_path: str,
        bucket: '_boto3.resources.factory.s3.Bucket',
        offset: int = 0
    ) -> None:
        '''
        A class used in reading from files which \
        reside within a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param Bucket bucket: A ``Bucket`` class instance.
        :param int offset: The byte offset. Defaults \
            to ``0``.
        '''
        self.__file = bucket.Object(key=file_path)
        super().__init__(file_path=file_path, offset=offset)
        

    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        # NOTE: No need to close file, just the
        #       underlying HTTPS connection which
        #       will be closed by the ``AWSClientHandler``
        #       class instance.
        pass


    def _write_impl(self, chunk: bytes) -> int:
        '''
        Writes the provided chunk to the opened file,
        and returns the number of bytes written.

        :param bytes chunk: The chunk of bytes that \
            is to be written to the file.
        '''
        self.__file.put(Body=chunk)
        return len(chunk)


class AzureBlobReader(_FileReader):
    '''
    A class used in reading from files which \
    reside within an Azure blob container.

    :param str file_path: The absolute path of \
        the file in question.
    :param ContainerClient container: A \
        ``ContainerClient`` class instance.
    :param int offset: The byte offset. Defaults \
        to ``0``.
    '''

    def __init__(
        self,
        file_path: str,
        container: _ContainerClient,
        offset: int = 0
    ) -> None:
        '''
        A class used in reading from files which \
        reside within a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param ContainerClient container: A \
            ``ContainerClient`` class instance.
        :param int offset: The byte offset. Defaults \
            to ``0``.
        '''
        self.__file = container.get_blob_client(blob=file_path)
        super().__init__(file_path=file_path, offset=offset)


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        self.__file.close()


    def _read_impl(self, chunk_size: _Optional[int]) -> bytes:
        '''
        Returns a chunk of bytes read from the opened file.

        :param int | None chunk_size: If ``None`, then \
            this method goes on to read the whole file, \
            else reads a chunk of bytes whose size is \
            equal to this value. Defaults to ``None``.
        '''
        if chunk_size is None:
            return self.__file.download_blob().read()
        offset = self.get_offset()
        return self.__file.download_blob(
            offset=offset,
            length=offset + chunk_size).read()
        

class AzureBlobWriter(_FileWriter):
    '''
    A class used in writing to files which \
    reside within an Azure blob container.

    :param str file_path: The absolute path of \
        the file in question.
    :param ContainerClient container: A \
        ``ContainerClient`` class instance.
    :param int offset: The byte offset. Defaults \
        to ``0``.
    '''

    def __init__(
        self,
        file_path: str,
        container: _ContainerClient,
        offset: int = 0
    ) -> None:
        '''
        A class used in writing to files which \
        reside within an Azure blob container.

        :param str file_path: The absolute path of \
            the file in question.
        :param ContainerClient container: A \
            ``ContainerClient`` class instance.
        :param int offset: The byte offset. Defaults \
            to ``0``.
        '''
        self.__file = container.get_blob_client(blob=file_path)
        super().__init__(file_path=file_path, offset=offset)


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        self.__file.close()


    def _write_impl(self, chunk: bytes) -> int:
        '''
        Writes the provided chunk to the opened file,
        and returns the number of bytes written.

        :param bytes chunk: The chunk of bytes that \
            is to be written to the file.
        '''
        n = len(chunk)
        self.__file.upload_blob(
            data=chunk,
            length=n,
            overwrite=True)
        return n