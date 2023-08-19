from enum import Enum
from itertools import chain as _chain
from typing import Optional as _Optional
from typing import Iterator as _Iterator
from typing import Callable as _Callable


class FileCache():
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


class DirCache():
    '''
    A class whose instances represent cached \
    information about a directory.

    :param str sep: The path seperator used.
    '''

    class State(Enum):
        NOT_TRAVERSED = 0
        TOP_LEVEL_TRAVERSED = 1
        RECURSIVELY_TRAVERSED = 2


    def relativize_path(func: _Callable):
        '''
        A decorator function used for relatizing \
        the path provided to the wrapped function.
        '''
        def wrapper(*args, **kwargs):
            if (path := kwargs.get('path', None)) is not None:
                kwargs['path'] = path.lstrip(args[0].__path)
            else:
                args[1] = args[1].lstrip(args[0].__path)
            func(*args, **kwargs)
        return wrapper


    def __init__(self, sep: str):
        '''
        A class whose instances represent cached \
        information about a directory.

        :param str sep: The path seperator used.
        '''
        self.__sep = sep
        self.purge()


    def purge(self) -> None:
        '''
        Purges the cache.
        '''
        self.__state = __class__.State.NOT_TRAVERSED
        self.__subdirs: dict[str, DirCache] = dict()
        self.__files: dict[str, FileCache] = dict()


    def get_size(self, path: str) -> _Optional[int]:
        '''
        Returns the file's cached size if it exists, \
        else returns ``None``.

        :param str path: The path of the file relative \
            to this directory.
        '''
        return (cache.get_size()
        if (cache := self.__get_file_cache(path)) is not None
        else None)
    

    def cache_size(self, path: str, size: int) -> None:
        '''
        Caches the provided size.

        :param str path: Either the file's absolute path \
            or the path relative to this directory.
        :param int size: The file's size.

        :note: This method goes on to creates any \
            necessary ``FileCache/DirCache`` instances \
            if they do not already exist.
        '''
        self.__create_file_cache(path)
        self.__get_file_cache(path).set_size(size)
    

    def get_metadata(self, path: str) -> _Optional[int]:
        '''
        Returns the file's cached metadata if they exist, \
        else returns ``None``.

        :param str path: The file's absolute path.
        '''
        return (cache.get_metadata()
        if (cache := self.__get_file_cache(path)) is not None
        else None)
    

    def cache_metadata(
        self,
        path: str,
        metadata: dict[str, str]
    ) -> None:
        '''
        Caches the provided metadata.

        :param str path: Either the file's absolute path \
            or the path relative to this directory.
        :param dict[str, str]: The file's metadata.

        :note: This method goes on to creates any \
            necessary ``FileCache/DirCache`` instances \
            if they do not already exist.
        '''
        self.__create_file_cache(path)
        self.__get_file_cache(path).set_metadata(metadata)


    def get_content_iterator(
        self,
        recursively: bool,
        include_dirs: bool
    ) -> _Optional[_Iterator[str]]:
        '''
        Returns an iterator capable of going through all ``FileCache`` \
        instances' keys, either within the ordinary cache or the top-level \
        cache depending on the value of ``recursively``. Returns ``None`` \
        if no ``FileCache`` instances exist for the requested cache type.

        :param bool recursively: Indicates whether to iterate \
            instances recursively by looking in the ordinary \
            cache, or not by looking in the top-level cache.
        :param bool include_dirs: Indicates whether to include \
            any directories when ``recursively`` has been set \
            to ``False``.
        '''
        def iterate_contents(
            dir_cache: DirCache,
            recursively: bool
        ) -> _Iterator[str]:
            '''
            Iterates through the contents of the \
            provided ``DirCache`` instance.
            '''
            if len(dir_cache.__subdirs) == 0:
                return (entity for entity in dir_cache.__files.keys)

            if recursively:
                for entity in _chain(
                    dir_cache.__files.keys(),
                    (
                        iterate_contents(subdir, recursively)
                        for subdir in self.__subdirs.values()
                    )
                ):
                    yield entity
            else:
                for entity in (
                    list(dir_cache.__files) + (
                        list(dir_cache.__subdirs)
                        if include_dirs
                        else list()
                )):
                    yield entity

        if recursively:
            if self.__state != __class__.State.RECURSIVELY_TRAVERSED:
                return None
        else:
            if self.__state == __class__.State.NOT_TRAVERSED:
                return None
            
        print("AAAAAAA")
        print(self.__files)
        print(self.__subdirs)
        print(self.__subdirs[''].__subdirs)
        for x in iterate_contents(self.__subdirs, recursively):
            print(x)
            
        return sorted(iterate_contents(self, recursively))


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
        if self.__state == __class__.State.NOT_TRAVERSED:
            if recursively:
                self.__state = __class__.State.RECURSIVELY_TRAVERSED
            else:
                self.__state = __class__.State.TOP_LEVEL_TRAVERSED
        elif self.__state == __class__.State.TOP_LEVEL_TRAVERSED and recursively:
            self.__state = __class__.State.RECURSIVELY_TRAVERSED

        for ep in sorted(iterator):
            if is_file(ep):
                self.__create_file_cache(ep)
            else:
                self.__create_dir_cache(ep)


    def __get_file_cache(
        self,
        path: str
    ) -> _Optional[FileCache]:
        '''
        Returns the ``FileCache`` instance that \
        corresponds to the provided path, or ``None``
        if no such instance exists.

        :param str path: The file's path relative \
            to the path of this ``DirCache`` instance's \
            underlying directory.
        '''
        entities = path.split(self.__sep)

        if len(entities) == 1:
            return self.__files.get(path, None)
        return (
            self.__subdirs[root_dir].__get_file_cache(
                path=self.__sep.join(entities[1:]))
            if (root_dir := entities[0]) in self.__subdirs
            else None)
    

    def __get_dir_cache(
        self,
        path: str,
    ) -> _Optional['DirCache']:
        '''
        Returns the ``DirCache`` instance that \
        corresponds to the provided path, or ``None``
        if no such instance exists.

        :param str path: The directory's path relative \
            to the path of this ``DirCache`` instance's \
            underlying directory.
        '''
        entities = path.split(self.__sep)

        if len(entities) == 1:
            return self.__subdirs.get(path, None)
        
        return (
            self.__subdirs[root_dir].__get_dir_cache(
                path=self.__sep.join(entities[1:]))
            if (root_dir := entities[0]) in self.__subdirs
            else None)
    

    def __create_file_cache(self, path: str) -> None:
        '''
        Creates a ``FileCache`` instance for the provided \
        path, along with any necessary ``DirCache`` instances.

        :param str path: The file's path relative \
            to the path of this ``DirCache`` instance's \
            underlying directory.
        '''
        entities = path.split(self.__sep)

        if len(entities) == 1:
            if path not in self.__files:
                self.__files.update({path: FileCache()})
            return
        
        root_dir = entities[0]

        if root_dir not in self.__subdirs:
            self.__subdirs.update({
                root_dir: DirCache(sep=self.__sep)
            })
        
        self.__subdirs[root_dir].__create_file_cache(
            path=self.__sep.join(entities[1:]))
        

    def __create_dir_cache(self, path: str) -> None:
        '''
        Creates a ``DirCache`` instance for the provided \
        path, along with any other ``DirCache`` instances \
        that are deemed as necessary.

        :param str path: The directory's path relative \
            to the path of this ``DirCache`` instance's \
            underlying directory.
        '''
        entities = path.split(self.__sep)

        if len(entities) == 1:
            if path not in self.__files:
                self.__files.update({
                    path: DirCache(sep=self.__sep)
                })
            return
        
        root_dir = entities[0]

        if root_dir not in self.__subdirs:
            self.__subdirs.update({
                root_dir: DirCache(sep=self.__sep)
            })
        
        self.__subdirs[root_dir].__create_dir_cache(
            path=self.__sep.join(entities[1:]))


class CacheManager():
    '''
    A class used in managing cached information.
    '''

    def __init__(self):
        '''
        A class used in managing cached information.
        '''
        self.__sep = '/'
        self.__cache: dict[str, DirCache] = dict()


    def purge(self) -> None:
        '''
        Purges the cache.
        '''
        for cd in self.__cache.values():
            cd.purge()
        self.__cache = dict()


    def get_size(self, path: str) -> _Optional[int]:
        '''
        Returns the file's cached size if it exists, \
        else returns ``None``.

        :param str path: The file's absolute path.
        '''
        if (
            cd_path := self.__get_optimal_dir_cache(path, True)
        ) is not None:
            return self.__cache[cd_path].get_size(path.removeprefix(cd_path))
    

    def cache_size(self, path: str, size: int) -> None:
        '''
        Caches the provided size.

        :param str path: The file's absolute path.
        :param int size: The file's size.
        '''
        self.__cache.cache_size(path, size)
    

    def get_metadata(self, path: str) -> _Optional[int]:
        '''
        Returns the file's cached metadata if they exist, \
        else returns ``None``.

        :param str path: The file's absolute path.
        '''
        if (
            cd_path := self.__get_optimal_dir_cache(path, True)
        ) is not None:
            return self.__cache[cd_path].get_metadata(path.removeprefix(cd_path))
    

    def cache_metadata(self, path: str, metadata: dict[str, str]) -> None:
        '''
        Caches the provided metadata.

        :param str path: The file's absolute path.
        :param dict[str, str]: The file's metadata.
        '''
        entities = path.split(self.__sep)
        for i in range(1, len(entities)):
            epath = self.__sep.join(entities[:-i])
            if epath in self.__cache:
                return self.__cache[epath].get_metadata(
                    path=self.__sep.join(entities[-i:]))


    def get_content_iterator(
        self,
        path: str,
        recursively: bool,
        include_dirs: bool
    ) -> _Optional[_Iterator[str]]:
        '''
        Returns an iterator capable of going through all ``FileCache`` \
        instances' keys, either within the ordinary cache or the top-level \
        cache depending on the value of ``recursively``. Returns ``None`` \
        if no ``FileCache`` instances exist for the requested cache type.

        :param str path: The directory's absolute path.
        :param bool recursively: Indicates whether to iterate \
            instances recursively by looking in the ordinary \
            cache, or not by looking in the top-level cache.
        :param bool include_dirs: Indicates whether to include \
            any directories when ``recursively`` has been set \
            to ``False``.
        '''
        return self.__cache.get_content_iterator(recursively, include_dirs)


    def cache_contents(
        self,
        path: str,
        iterator: _Iterator[str],
        recursively: bool,
        is_file: _Callable[[str], bool]
    ) -> None:
        '''
        Goes through the provided iterator \
        in order to cache its contents.

        :param str path: The directory's absolute path.
        :param Iterator[str] iterator: An iterator through \
            which a directory's contents can be traversed.
        :param bool recursively: Indicates whether the provided \
            iterator traverses a directory recursively or not.
        :param Callable[[str], bool] is_file: A function that \
            receives a string path and returns a value indicating \
            whether said path corresponds to a file or a directory.
        '''
        self.__cache.cache_contents(iterator, recursively, is_file)


    def __get_optimal_dir_cache(
        self,
        path: str,
        is_file: bool
    ) -> _Optional[str]:
        '''
        Given the absolute path of either a file or a directory \
        returns the absolute path that corresponds to the ``DirCache`` \
        instance that is closest to said file/directory, if said instance \
        exists, else returns ``None``.

        :param str path: The absolute path of either the file \
            or directory in question.
        :param bool is_file: Indicates whether the provided \
            path corresponds to a file or a directory.
        '''
        entities = path.split(self.__sep)
        v = 1 if is_file else 0
        for i in range(len(entities)):
            epath = self.__sep.join(
                entities[len(entities)-v-i:len(entities)])
            if epath in self.__cache:
                return epath