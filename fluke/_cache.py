from typing import Optional as _Optional


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


    def is_recursive_cache_empty(self):
        '''
        Returns ``True`` if no items have been cached \
        recursively, else returns ``False``.
        '''
        return len([f for f in self.__cache if f not in set(self.__top_level_files)]) == 0
    

    def is_top_level_empty(self):
        '''
        Returns ``True`` if the paths of any top-level \
        objects have not been stored, else returns ``False``.
        '''
        return len(self.__top_level_files + self.__top_level_dirs) == 0


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


    def add_to_cache(self, path: str) -> None:
        '''
        Creates an entry within the cache based on the \
        provided file path key.

        :param str path: The file's absolute path.
        '''
        self.__cache.update({path: Cache()})
        

    def add_to_top_level(self, path: str, is_file: bool) -> None:
        '''
        Adds the provided path to the cache manager's \
        top-level list.

        :param str path: The object's absolute path.
        :param bool is_file: Indicates whether the \
            provided path points to a file or not.
        '''
        if is_file:
            self.__top_level_files.append(path)
            if not self.is_in_cache(path=path):
                self.add_to_cache(path=path)
        else:
            self.__top_level_dirs.append(path)


    def iterate_contents(
        self,
        recursively: bool,
        include_dirs: bool
    ):
        '''
        Iterate through all ``Cache`` instances stored. \
        either within the ordinary cache or the top-level cache.

        :param bool recursively: Indicates whether to iterate \
            instances recursively by looking in the ordinary \
            cache, or not by looking in the top-level cache.
        :param bool include_dirs: Indicates whether to include \
            any directories when ``recursively`` has been set \
            to ``False``.
        '''
        if recursively:
            iterable = (key for key in self.__cache)
        else:
            top_level = list(self.__top_level_files)
            if include_dirs:
                top_level += self.__top_level_dirs
            iterable = (key for key in top_level)

        return iterable
        

    def _get_cache(self, file_path: str) -> _Optional[Cache]:
        '''
        Returns a reference to the file's ``Cache`` instance, \
        if said instance exists, else returns ``None``.

        :param str file_path: The file's absolute path.
        '''
        if file_path in self.__cache:
            return self.__cache[file_path]
    