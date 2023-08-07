import os as _os
import io as _io
from abc import ABC as _ABC
from abc import abstractmethod as _absmethod
from typing import Optional as _Optional
from typing import Iterator as _Iterator


import paramiko as _prmk
import boto3 as _boto3
from azure.storage.blob import ContainerClient as _ContainerClient
from azure.storage.blob import BlobType as _BlobType
from google.cloud.storage import Bucket as _GCPBucket
from google.resumable_media.requests import ResumableUpload as _ResumableUpload
from google.auth.transport.requests import AuthorizedSession as _AuthSession
from google.resumable_media.requests import MultipartUpload as _MultipartUpload
from botocore.exceptions import ClientError as _BotoClientError
from azure.core.exceptions import HttpResponseError as _AzureResponseError


from ._helper import infer_separator as _infer_sep


class _IOHandler(_ABC):
    '''
    An abstract class which serves as the \
    base class for all Reader/Writer classes.

    :param str file_path: The path of the \
        handler's underlying file.
    '''

    def __init__(self, file_path: str):
        '''
        An abstract class which serves as the \
        base class for all Reader/Writer classes.

        :param str file_path: The path of the \
            handler's underlying file.
        '''
        self.__file_path = file_path
        self.__offset = None


    def get_file_path(self) -> str:
        '''
        Returns the path of the handler's \
        underlying file.
        '''
        return self.__file_path
    

    def get_offset(self) -> _Optional[int]:
        '''
        Returns the current offset value.

        :note: This method will return ``None`` unless, \
            it is invoked during the reading/writing of \
            data in distinct chunks.
        '''
        return self.__offset
    

    def set_offset(self, offset: _Optional[int]) -> None:
        '''
        Sets the offset.

        :param int | None offset: The new offset.
        '''
        self.__offset = offset


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

    :param str file_path: The path of the \
        handler's underlying file.
    '''

    def __init__(self, file_path: str, file_size: int):
        '''
        An abstract class which serves as the \
        base class for all Reader classes.
        
        :param str file_path: The path of the \
            handler's underlying file.
        :param int file_size: The size in bytes of \
            the file in question.
        '''
        super().__init__(file_path)
        self.__file_size = file_size


    def get_file_size(self) -> int:
        '''
        Returns the size in bytes of the \
        handler's underlying file.
        '''
        return self.__file_size


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
    

    def read(self) -> bytes:
        '''
        Reads the whole file and returns its bytes.
        '''
        return self.read_range(start=None, end=None)
    

    def read_chunks(
        self,
        chunk_size: int,
        offset: int = 0
    ) -> _Iterator[bytes]:
        '''
        Returns an iterator capable of going through \
        the file's contents as distinct chunks of bytes.

        :param int chunk_size: The size of each file chunk.
        :param int offset: The point within the file to begin \
            reading bytes chunks from.
        '''
        self.set_offset(offset=offset)
        start = offset
        end = start + chunk_size
        file_size = self.get_file_size()
        while start < file_size:
            chunk = self._read_impl(start, end)
            n = len(chunk)
            start += n
            end += n
            self.set_offset(offset=start)
            yield chunk
        self.set_offset(offset=None)


    def read_range(
        self,
        start: _Optional[int],
        end: _Optional[int]
    ) -> bytes:
        '''
        Reads and returns the specified byte range.

        :param int | None start: The point in file from which \
            to begin reading bytes. If ``None``, then begin \
            reading from the start of the file.
        :param int | None end: The point in file at which \
            to stop reading bytes. If ``None``, then stop \
            reading at the end of the file.
        '''
        if start is None:
            start = 0
        else:
            start = max(0, start)

        if end is None:
            end = self.get_file_size()
        else:
            end = min(self.get_file_size(), end)

        if start >= end:
            return b""

        return self._read_impl(start=start, end=end)


    @_absmethod
    def _read_impl(
        self,
        start: int,
        end: int
    ) -> bytes:
        '''
        Reads and returns the specified byte range.

        :param int start: The point to start reading from.
        :param int end: The point to stop reading from.
        '''
        pass


class _FileWriter(_IOHandler, _ABC):
    '''
    An abstract class which serves as the \
    base class for all Writer classes.
    
    :param str file_path: The path of the \
        handler's underlying file.
    '''

    def __init__(self, file_path: str):
        '''
        An abstract class which serves as the \
        base class for all Writer classes.
        
        :param str file_path: The path of the \
            handler's underlying file.
        :param int file_size: The size in bytes of \
            the file in question.
        '''
        super().__init__(file_path)
        self.set_offset(offset=0)


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
        self.set_offset(self.get_offset() + n)
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
    :param int file_size: The size in bytes of \
        the file in question.
    '''

    def __init__(self, file_path: str, file_size: int) -> None:
        '''
        A class used in reading from files which \
        reside within the local file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param int file_size: The size in bytes of \
            the file in question.
        '''
        super().__init__(file_path=file_path, file_size=file_size)
        self.__file = open(file=file_path, mode=self.get_mode())


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        self.__file.close()


    def _read_impl(
        self,
        start: int,
        end: int
    ) -> bytes:
        '''
        Reads and returns the specified byte range.

        :param int start: The point to start reading from.
        :param int end: The point to stop reading from.
        '''
        self.__file.seek(start)
        return self.__file.read(end - start)


class LocalFileWriter(_FileWriter):
    '''
    A class used in writing to files which \
    reside within the local file system.

    :param str file_path: The absolute path of \
        the file in question.
    '''

    def __init__(self, file_path: str) -> None:
        '''
        A class used in writing to files which \
        reside within the local file system.

        :param str file_path: The absolute path of \
            the file in question.
        '''
        super().__init__(file_path=file_path)
        # Create necessary directories if they do not exist.
        _os.makedirs(name=_os.path.dirname(file_path), exist_ok=True)
        # Open file for writing.
        self.__file = open(file=file_path, mode=self.get_mode())


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
    :param int file_size: The size in bytes of \
        the file in question.
    :param SFTPClient sftp: An ``SFTPClient`` \
        class instance.
    '''

    def __init__(
        self,
        file_path: str,
        file_size: int,
        sftp: _prmk.SFTPClient
    ) -> None:
        '''
        A class used in reading from files which \
        reside within a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param int file_size: The size in bytes of \
            the file in question.
        :param SFTPClient sftp: An ``SFTPClient`` \
            class instance.
        '''
        super().__init__(file_path=file_path, file_size=file_size)
        self.__file: _prmk.SFTPFile = sftp.open(
            filename=file_path, mode=self.get_mode())


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        self.__file.close()


    def _read_impl(
        self,
        start: int,
        end: int
    ) -> bytes:
        '''
        Reads and returns the specified byte range.

        :param int start: The point to start reading from.
        :param int end: The point to stop reading from.
        '''
        self.__file.seek(start)
        return self.__file.read(end - start)


class RemoteFileWriter(_FileWriter):
    '''
    A class used in writing to files which \
    reside within a remote file system.

    :param str file_path: The absolute path of \
        the file in question.
    :param SFTPClient sftp: An ``SFTPClient`` \
        class instance.
    '''

    def __init__(
        self,
        file_path: str,
        sftp: _prmk.SFTPClient
    ) -> None:
        '''
        A class used in writing to files which \
        reside within a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param SFTPClient sftp: An ``SFTPClient`` \
            class instance.
        '''
        super().__init__(file_path=file_path)

        sep = _infer_sep(file_path)

        def get_parent_dir(file_path: str) -> _Optional[str]:
            '''
            Returns the path to the parent directory \
            of the provided file path. Returns ``None`` \
            if said directory is the root directory.

            :param str file_path: The path of the file \
                in question.
            '''
            file_path = file_path.removesuffix(sep)
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
    

class AmazonS3FileReader(_FileReader):
    '''
    A class used in reading from files which \
    reside within an Amazon S3 bucket.

    :param str file_path: The absolute path of \
        the file in question.
    :param int file_size: The size in bytes of \
        the file in question.
    :param Bucket bucket: A ``Bucket`` class instance.
    '''

    def __init__(
        self,
        file_path: str,
        file_size: int,
        bucket: '_boto3.resources.factory.s3.Bucket'
    ) -> None:
        '''
        A class used in reading from files which \
        reside within a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param int file_size: The size in bytes of \
            the file in question.
        :param Bucket bucket: A ``Bucket`` class instance.
        '''
        super().__init__(file_path=file_path, file_size=file_size)
        self.__file = bucket.Object(key=file_path)


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        # NOTE: No need to close file, just the
        #       underlying HTTPS connection which
        #       will be closed by the ``AWSClientHandler``
        #       class instance.
        pass


    def _read_impl(
        self,
        start: int,
        end: int
    ) -> bytes:
        '''
        Reads and returns the specified byte range.

        :param int start: The point to start reading from.
        :param int end: The point to stop reading from.
        ''' 
        range = f"bytes={start}-{end-1}"
        return self.__file.get(Range=range)['Body'].read()

            
class AmazonS3FileWriter(_FileWriter):
    '''
    A class used in writing to files which \
    reside within an Amazon S3 bucket.

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
    :param Bucket bucket: A ``Bucket`` class instance.
    '''
    def __init__(
        self,
        file_path: str,
        metadata: _Optional[dict[str, str]],
        chunk_size: _Optional[int],
        bucket: '_boto3.resources.factory.s3.Bucket'
    ) -> None:
        '''
        A class used in reading from files which \
        reside within a remote file system.

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
        :param Bucket bucket: A ``Bucket`` class instance.
        '''
        super().__init__(file_path=file_path)

        self.__file = bucket.Object(key=file_path)
        # NOTE: If uploading file in chunks, then initiate
        # multipart-upload, including any metadata that 
        # may exist. Else, store the metadata dictionary
        # so as to include them during the bulk upload.
        if chunk_size is not None:
            if metadata is None:
                self.__mpu = self.__file.initiate_multipart_upload()
            else:
                self.__mpu = self.__file.initiate_multipart_upload(
                    Metadata=metadata)
            self.__parts = list()
        else:
            self.__mpu = None
            self.__metadata = metadata
        

    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        # NOTE: No need to close file, just the
        #       underlying HTTPS connection which
        #       will be closed by the ``AWSClientHandler``
        #       class instance.
        if self.__mpu is not None:
            self.__mpu.complete(MultipartUpload={'Parts': self.__parts})


    def _write_impl(self, chunk: bytes) -> int:
        '''
        Writes the provided chunk to the opened file,
        and returns the number of bytes written.

        :param bytes chunk: The chunk of bytes that \
            is to be written to the file.
        '''
        if self.__mpu is None:
            with _io.BytesIO(chunk) as buffer:
                self.__file.upload_fileobj(
                    Fileobj=buffer,
                    ExtraArgs={ "Metadata": self.__metadata }
                        if self.__metadata is not None else None)
        else:
            part_number = len(self.__parts) + 1
            part = self.__mpu.Part(part_number=part_number)
            response = part.upload(Body=chunk)
            self.__parts.append({
                'PartNumber': part_number,
                'ETag': response['ETag']
            })
        return len(chunk)


class AzureBlobReader(_FileReader):
    '''
    A class used in reading from files which \
    reside within an Azure blob container.

    :param str file_path: The absolute path of \
        the file in question.
    :param int file_size: The size in bytes of \
        the file in question.
    :param ContainerClient container: A \
        ``ContainerClient`` class instance.
    '''

    def __init__(
        self,
        file_path: str,
        file_size: int,
        container: _ContainerClient
    ) -> None:
        '''
        A class used in reading from files which \
        reside within a remote file system.

        :param str file_path: The absolute path of \
            the file in question.
        :param int file_size: The size in bytes of \
            the file in question.
        :param ContainerClient container: A \
            ``ContainerClient`` class instance.
        '''
        super().__init__(file_path=file_path, file_size=file_size)
        self.__file = container.get_blob_client(blob=file_path)


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        self.__file.close()


    def _read_impl(
        self,
        start: int,
        end: int
    ) -> bytes:
        '''
        Reads and returns the specified byte range.

        :param int start: The point to start reading from.
        :param int end: The point to stop reading from.
        '''
        return self.__file.download_blob(
            offset=start,
            length=end-start).read()


class AzureBlobWriter(_FileWriter):
    '''
    A class used in writing to files which \
    reside within an Azure blob container.

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
    :param ContainerClient container: A \
        ``ContainerClient`` class instance.
    '''

    def __init__(
        self,
        file_path: str,
        metadata: _Optional[dict[str, str]],
        chunk_size: _Optional[int],
        container: _ContainerClient,
    ) -> None:
        '''
        A class used in writing to files which \
        reside within an Azure blob container.

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
        :param ContainerClient container: A \
            ``ContainerClient`` class instance.
        '''
        super().__init__(file_path=file_path)
        self.__file = container.get_blob_client(blob=file_path)
        self.__metadata = metadata
        self.__chunk_size = chunk_size
        # NOTE: If blob already exists, then it has to be deleted only
        #       in the case that its type has to change. Furthermore,
        #       in the case the blob is to be written in chunks, it must
        #       be re-created beforehands as an "Append" blob.
        if self.__file.exists():
            blob_type = self.__file.get_blob_properties().blob_type
            if blob_type == _BlobType.PAGEBLOB:
                self.__file.delete_blob()
                if chunk_size is not None:
                    self.__file.create_append_blob()
            elif chunk_size is not None and blob_type == _BlobType.BLOCKBLOB:
                self.__file.delete_blob()
                self.__file.create_append_blob()
            elif chunk_size is None and blob_type == _BlobType.APPENDBLOB:
                self.__file.delete_blob()
        elif chunk_size is not None:
            self.__file.create_append_blob()


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        # If blob was uploaded in chunks,
        # then set file metadatan here.
        if self.__chunk_size is not None:
            self.__file.set_blob_metadata(
                metadata=self.__metadata)
        # Close the file.
        self.__file.close()


    def _write_impl(self, chunk: bytes) -> int:
        '''
        Writes the provided chunk to the opened file,
        and returns the number of bytes written.

        :param bytes chunk: The chunk of bytes that \
            is to be written to the file.
        '''
        n = len(chunk)
        if self.__chunk_size is None:
            self.__file.upload_blob(
                data=chunk,
                length=n,
                metadata=self.__metadata,
                overwrite=True)
        else:
            self.__file.append_block(
                data=chunk,
                length=n)
        return n
    

class GCPFileReader(_FileReader):
    '''
    A class used in reading from files which \
    reside within a Google Cloud Storage bucket.

    :param str file_path: The absolute path of \
        the file in question.
    :param int file_size: The size in bytes of \
        the file in question.
    :param Bucket bucket: A ``Bucket`` class instance.
    '''

    def __init__(
        self,
        file_path: str,
        file_size: int,
        bucket: _GCPBucket
    ) -> None:
        '''
        A class used in reading from files which \
        reside within a Google Cloud Storage bucket.

        :param str file_path: The absolute path of \
            the file in question.
        :param int file_size: The size in bytes of \
            the file in question.
        :param Bucket bucket: A ``Bucket`` class instance.
        '''
        super().__init__(file_path=file_path, file_size=file_size)
        self.__file = bucket.get_blob(blob_name=file_path)


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        # NOTE: No need to close file, just the
        #       underlying HTTPS connection which
        #       will be closed by the `GCPClientHandler``
        #       class instance.
        pass


    def _read_impl(
        self,
        start: int,
        end: int
    ) -> bytes:
        '''
        Reads and returns the specified byte range.

        :param int start: The point to start reading from.
        :param int end: The point to stop reading from.
        '''
        return self.__file.download_as_bytes(start=start, end=end-1)

            
class GCPFileWriter(_FileWriter):
    '''
    A class used in writing to files which \
    reside within a Google Cloud Storage bucket.

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
    :param Bucket bucket: A ``Bucket`` class instance.
    '''
    def __init__(
        self,
        file_path: str,
        metadata: _Optional[dict[str, str]],
        chunk_size: _Optional[int],
        bucket: _GCPBucket
    ) -> None:
        '''
        A class used in reading from files which \
        reside within a remote file system.

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
        :param Bucket bucket: A ``Bucket`` class instance.
        '''
        super().__init__(file_path=file_path)

        # NOTE: If uploading file in chunks, then
        #       create a resumable upload session.
        if chunk_size is None:
            self.__file = bucket.blob(blob_name=file_path)
            self.__metadata = metadata
        else:
            self.__file = None
            self.__rus = self._create_resumable_upload_session(
                bucket=bucket.name,
                file_path=file_path,
                chunk_size=chunk_size)
            self.__transport = _AuthSession(
                credentials=bucket.client._credentials)
            self.__stream = _io.BytesIO()
            self.__rus.initiate(
                transport=self.__transport,
                content_type='application/octet-stream',
                stream=self.__stream,
                stream_final=False,
                metadata={'metadata': metadata})


    def close(self) -> None:
        '''
        Closes the handler's underlying file.
        '''
        # NOTE: No need to close file, just the
        #       underlying HTTPS connection which
        #       will be closed by the `GCPClientHandler``
        #       class instance.
        if self.__file is None:
            # NOTE: Due to setting ``stream_final`` to ``False``,
            #       the upload is automatically iferred to be over
            #       whenever a chunk whose size is less than the
            #       specified chunk size is uploaded. If a file
            #       happens to be divided into equally sized chunks,
            #       then the algorithm cannot infer the final chunk.
            #       In that case, a zero-byte chunk should be uploaded
            #       to indicate that the upload has been finalized.
            if not self.__rus.finished:
                self.__rus.transmit_next_chunk(
                    transport=self.__transport)
            self.__transport.close()
            self.__stream.close()
        elif self.__metadata is not None:
            # NOTE: Call ``patch`` in order to
            #       upload the file's metadata.
            self.__file.metadata = self.__metadata
            self.__file.patch()


    def _write_impl(self, chunk: bytes) -> int:
        '''
        Writes the provided chunk to the opened file,
        and returns the number of bytes written.

        :param bytes chunk: The chunk of bytes that \
            is to be written to the file.
        '''
        if self.__file is None:
            self.__stream.write(chunk)
            self.__stream.seek(-len(chunk), 1)
            self.__rus.transmit_next_chunk(
                transport=self.__transport)
        else:
            with _io.BytesIO(chunk) as buffer:
                self.__file.upload_from_file(
                    file_obj=buffer,
                    size=len(chunk))
        return len(chunk)
    

    @staticmethod
    def _create_resumable_upload_session(
        bucket: str,
        file_path: str,
        chunk_size: int
    ) -> _ResumableUpload:
        return _ResumableUpload(
            upload_url=(
                "https://storage.googleapis.com" +
                f"/upload/storage/v1/b/{bucket}/" +
                f"o?uploadType=resumable&name={file_path}"
            ),
            chunk_size=chunk_size)