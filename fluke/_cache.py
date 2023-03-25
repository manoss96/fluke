from typing import Optional as _Optional
from typing import Iterator as _Iterator
from typing import Callable as _Callable


class Cache():
    '''
    A class whose instances represent cached \
    information about a file.
    '''

    def __init__(self):
        '''
        A class whose instances represent cached \
        information about a file.
        '''
        self.__size: _Optional[int] = None
        self.__metadata: _Optional[dict[str, str]] = None


    def get_size(self) -> _Optional[int]:
        '''
        Returns the size of the file. Returns ``None`` \
        if no size has been cached.
        '''
        return self.__size


    def set_size(self, size: int) -> None:
        '''
        Sets the size of the file.
        '''
        self.__size = size


    def get_metadata(self) -> _Optional[dict[str, str]]:
        '''
        Returns a dictionary containing the metadata of the file. \
        Returns ``None`` if no metadata has been cached.
        '''
        if self.__metadata is not None:
            return dict(self.__metadata)


    def set_metadata(self, metadata: dict[str, str]) -> None:
        '''
        Sets the metadata of the file.
        '''
        self.__metadata = dict(metadata)


class CacheManager():
    '''
    A class used in managing ``Cache`` instances.
    '''

    def __init__(self):
        '''
        A class used in managing ``Cache`` instances.
        '''
        self.__top_level_files: list[str] = list()
        self.__top_level_dirs: list[str] = list()
        self.__cache: dict[str, Cache] = dict()


    def get_size(self, file_path: str) -> _Optional[int]:
        '''
        Returns the file's cached size if it exists, \
        else returns ``None``.

        :param str file_path: The file's absolute path.
        '''
        if file_path in self.__cache:
            return self.__cache[file_path].get_size()
    

    def cache_size(self, file_path: str, size: int) -> None:
        '''
        Caches the provided size.

        :param str file_path: The file's absolute path.
        :param int size: The file's size.
        '''
        if not self.is_in_cache(path=file_path):
            self.__cache.update({file_path: Cache()})
        self.__cache[file_path].set_size(size=size)
    

    def get_metadata(self, file_path: str) -> _Optional[dict[str, str]]:
        '''
        Returns the file's cached metadata if they exist, \
        else returns ``None``.

        :param str file_path: The file's absolute path.
        '''
        if file_path in self.__cache:
            return self.__cache[file_path].get_metadata()
        

    def cache_metadata(self, file_path: str, metadata: dict[str, str]) -> None:
        '''
        Caches the provided metadata.

        :param str file_path: The file's absolute path.
        :param dict[str, str]: The file's metadata.
        '''
        if not self.is_in_cache(path=file_path):
            self.__cache.update({file_path: Cache()})
        self.__cache[file_path].set_metadata(metadata=metadata)


    def purge(self) -> None:
        '''
        Purges cache.
        '''
        self.__cache = dict()
        self.__top_level_files = list()
        self.__top_level_dirs = list()


    def is_in_cache(self, path: str) -> bool:
        '''
        Returns ``True`` if the file that corresponds \
        to the provided path is currently stored within \
        the cache, else returns ``False``.

        :param str path: The file's absolute path.
        '''
        return path in self.__cache


    def get_content_iterator(
        self,
        recursively: bool,
        include_dirs: bool
    ) -> _Optional[_Iterator[str]]:
        '''
        Returns an iterator capable of going through all ``Cache`` \
        instances' keys, either within the ordinary cache or the top-level \
        cache depending on the value of ``recursively``. Returns ``None`` \
        if no ``Cache`` instances exist for the requested cache type.

        :param bool recursively: Indicates whether to iterate \
            instances recursively by looking in the ordinary \
            cache, or not by looking in the top-level cache.
        :param bool include_dirs: Indicates whether to include \
            any directories when ``recursively`` has been set \
            to ``False``.
        '''
        if recursively:
            if self._is_recursive_cache_empty():
                return None
            return (key for key in self.__cache)
        else:
            if self._is_top_level_empty():
                return None
            top_level = list(self.__top_level_files)
            if include_dirs:
                top_level += self.__top_level_dirs
            return (key for key in top_level)
    

    def cache_contents(
        self,
        iterator: _Iterator[str],
        recursively: bool,
        is_file: _Callable[[str], bool]
    ) -> None:
        '''
        Goes through the provided iterator \
        in order to cache its contents.

        :param Iterator[str] iterator: An iterator through \
            which a directory's contents can be traversed.
        :param bool recursively: Indicates whether the provided \
            iterator traverses a directory recursively or not.
        :param Callable[[str], bool] is_file: A function that \
            receives a string path and returns a value indicating \
            whether said path corresponds to a file or a directory.
        '''
        if recursively:
            for path in iterator:
                if not self.is_in_cache(path=path):
                    self.__cache.update({path: Cache()})
        else:
            for path in iterator:
                if is_file(path):
                    self.__top_level_files.append(path)
                    if not self.is_in_cache(path=path):
                        self.__cache.update({path: Cache()})
                else:
                    self.__top_level_dirs.append(path)
    

    def _is_recursive_cache_empty(self):
        '''
        Returns ``True`` if no items have been cached \
        recursively, else returns ``False``.
        '''
        # If top-level cache is empty, then check if
        # the recursive cache has items...
        if self._is_top_level_empty():
            return len(self.__cache) == 0
        
        # If top-level cache is not empty,
        # then consider the recursive cache
        # not empty if no sub-directories exist.
        if len(self.__top_level_dirs) == 0:
            return False
        
        # Else if sub-directories exist, check whether
        # this directory has been traversed recursively.
        return len([f for f in self.__cache if f not in set(self.__top_level_files)]) == 0
    

    def _is_top_level_empty(self):
        '''
        Returns ``True`` if the paths of any top-level \
        objects have not been stored, else returns ``False``.
        '''
        return len(self.__top_level_files + self.__top_level_dirs) == 0
    