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


    def __init__(self, sep: str):
        '''
        A class whose instances represent cached \
        information about a directory.

        :param str sep: The path seperator used.
        '''
        self.__sep = sep
        self.purge()


    @staticmethod
    def __add_sep_prefix(path: str, sep: str) -> str:
        return f"{sep}{path.lstrip(sep)}"


    def add_sep_prefix(func: _Callable):
        '''
        A decorator function used for relatizing \
        the path provided to the wrapped function.
        '''
        def wrapper(*args, **kwargs):
            if (path := kwargs.get('path', None)) is not None:
                kwargs['path'] = __class__.__add_sep_prefix(path, args[0].__sep)
            else:
                args[1] = __class__.__add_sep_prefix(args[1], args[0].__sep)
            print(args, kwargs)
            return func(*args, **kwargs)
        return wrapper


    def purge(self) -> None:
        '''
        Purges the cache.
        '''
        self.__state = __class__.State.NOT_TRAVERSED
        self.__subdirs: dict[str, DirCache] = dict()
        self.__files: dict[str, FileCache] = dict()


    @add_sep_prefix
    def get_size(self, path: str) -> _Optional[int]:
        '''
        Returns the file's cached size if it exists, \
        else returns ``None``.

        :param str path: The file's absolute path.
        '''
        return (cache.get_size()
        if (cache := self.__get_file_cache(path)) is not None
        else None)
    

    @add_sep_prefix
    def cache_size(self, path: str, size: int) -> None:
        '''
        Caches the provided size.

        :param str path: The file's absolute path.
        :param int size: The file's size.

        :note: This method goes on to creates any \
            necessary ``FileCache/DirCache`` instances \
            if they do not already exist.
        '''
        self.__create_file_cache(path).set_size(size)
    

    @add_sep_prefix
    def get_metadata(self, path: str) -> _Optional[int]:
        '''
        Returns the file's cached metadata if they exist, \
        else returns ``None``.

        :param str path: The file's absolute path.
        '''
        return (cache.get_metadata()
        if (cache := self.__get_file_cache(path)) is not None
        else None)
    

    @add_sep_prefix
    def cache_metadata(
        self,
        path: str,
        metadata: dict[str, str]
    ) -> None:
        '''
        Caches the provided metadata.

        :param str path: The file's absolute path.
        :param dict[str, str]: The file's metadata.

        :note: This method goes on to creates any \
            necessary ``FileCache/DirCache`` instances \
            if they do not already exist.
        '''
        self.__create_file_cache(path).set_metadata(metadata)


    @add_sep_prefix
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

        :param path: The directory's absolute path.
        :param bool recursively: Indicates whether to iterate \
            instances recursively by looking in the ordinary \
            cache, or not by looking in the top-level cache.
        :param bool include_dirs: Indicates whether to include \
            any directories when ``recursively`` has been set \
            to ``False``.
        '''
        print(f"\nWHAT IS CACHE for {path}: ")

        cache = self.__get_dir_cache(path=path)

        print("CACHE FOUND: ", cache)

        if (
            cache is None or
            cache.__state == __class__.State.NOT_TRAVERSED or
            (recursively and cache.__state != __class__.State.RECURSIVELY_TRAVERSED)
        ):
            print("RETURNING NONEEE")
            return None
    
        def iterate_contents(
            dc: DirCache,
            recursively: bool
        ) -> _Iterator[str]:
            '''
            Iterates through the contents of the \
            provided ``DirCache`` instance.
            '''
            if recursively:
                if len(dc.__subdirs) == 0:
                    return dc.__files
                for subdir in dc.__subdirs.values():
                    for entity in iterate_contents(subdir, recursively):
                        yield entity
                for entity in list(dc.__files) + list(dc.__subdirs):
                    yield entity
                '''
                for entity in _chain(
                    dc.__files,
                    (
                        iterate_contents(subdir, recursively)
                        for subdir in self.__subdirs.values()
                    )
                ):
                    yield entity
                '''
            else:
                for entity in (
                    list(dc.__files) + (
                        list(dc.__subdirs)
                        if include_dirs
                        else list()
                )):
                    yield entity
            
        print("PASSSEEEEEED")
        print(cache.__files)
        print(cache.__subdirs)

        for x in sorted(iterate_contents(cache, recursively)):
            print(x)

        print(iterate_contents(cache, recursively))
        print("RETURNINGGGGGGGGG")
        return sorted(iterate_contents(cache, recursively))


    @add_sep_prefix
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
        print("\nCREATING APPROPRIATE DIR FOR CACHING CONTENTS...")
        cache = self.__create_dir_cache(path=path)

        if cache.__state == __class__.State.NOT_TRAVERSED:
            if recursively:
                cache.__state = __class__.State.RECURSIVELY_TRAVERSED
            else:
                cache.__state = __class__.State.TOP_LEVEL_TRAVERSED
        elif cache.__state == __class__.State.TOP_LEVEL_TRAVERSED and recursively:
            cache.__state = __class__.State.RECURSIVELY_TRAVERSED

        print("\nCACHING CONTENTS...")
        # NOTE: Cache contents using top-level
        #       ``DirCache`` instance.
        for ep in sorted(iterator):
            if is_file(ep):
                print(f"FILE: {ep}")
                self.__create_file_cache(self.__add_sep_prefix(ep, self.__sep))
            else:
                print(f"DIR: {ep}")
                self.__create_dir_cache(self.__add_sep_prefix(ep, self.__sep))


    def __get_file_cache(
        self,
        path: str,
        level: int = 1
    ) -> _Optional[FileCache]:
        '''
        Returns the ``FileCache`` instance that \
        corresponds to the provided path, or ``None``
        if no such instance exists.

        :param str path: The file's path relative \
            to the path of this ``DirCache`` instance's \
            underlying directory.
        :param int level: A helping variable that prevents \
            this method from recursing indefinitely. Defaults \
            to ``1``.
        '''
        entities = path.split(self.__sep)

        if len(entities) == level:
            # Return ``FileCache`` instance.
            return self.__files.get(path, None)
        
        # Else construct ``DirCache`` path,
        # and search if such cache exists.
        current = self.__sep.join(entities[:level]) + self.__sep
        return (
            self.__subdirs[current].__get_file_cache(path, level+1)
            if current in self.__subdirs
            else None)
    

    def __get_dir_cache(
        self,
        path: str,
        level: int = 1
    ) -> _Optional['DirCache']:
        '''
        Returns the ``DirCache`` instance that \
        corresponds to the provided path, or ``None``
        if no such instance exists.

        :param str path: The directory's path relative \
            to the path of this ``DirCache`` instance's \
            underlying directory.
        :param int level: A helping variable that prevents \
            this method from recursing indefinitely. Defaults \
            to ``1``.
        '''
        entities = path.split(self.__sep)

        print(f"\nFETCHING DIR CACHE for {path}")
        print(entities)
        print(self.__sep.join(entities[:level]))

        current = self.__sep.join(entities[:level]) + self.__sep

        if len(entities) - 1 == level:
            return self.__subdirs.get(current, None)
        
        return (
            self.__subdirs[current].__get_dir_cache(path, level+1)
            if current in self.__subdirs
            else None)
    

    def __create_file_cache(
        self,
        path: str,
        level: int = 1
    ) -> FileCache:
        '''
        Creates a ``FileCache`` instance for the provided \
        path, along with any necessary ``DirCache`` instances. \
        Returns the created ``FileCache`` instance.

        :param str path: The file's path relative \
            to the path of this ``DirCache`` instance's \
            underlying directory.
        :param int level: A helping variable that prevents \
            this method from recursing indefinitely. Defaults \
            to ``1``.
        '''
        print(f"\nCREATING FILE CACHE FOR {path}... (level={level})")
        entities = path.split(self.__sep)

        if len(entities) == level:
            if path not in self.__files:
                self.__files.update({path: FileCache()})
                print(f"CREATED FILE CACHE '{path}' in {self.__files} AS IT DIDNT EXIST...")
            return self.__files[path]
        
        current = self.__sep.join(entities[:level]) + self.__sep

        if current not in self.__subdirs:
            print(f"CREATED DIR CACHE '{current}' in {self.__subdirs} AS IT DIDNT EXIST...")
            self.__subdirs.update({
                current: DirCache(sep=self.__sep)
            })
        
        return self.__subdirs[current].__create_file_cache(path, level+1)
        

    def __create_dir_cache(
        self,
        path: str,
        level: int = 1
    ) -> 'DirCache':
        '''
        Creates a ``DirCache`` instance for the provided \
        path, along with any other ``DirCache`` instances \
        that are deemed as necessary. Returns the created \
        ``DirCache`` instance.

        :param str path: The directory's path relative \
            to the path of this ``DirCache`` instance's \
            underlying directory.
        :param int level: A helping variable that prevents \
            this method from recursing indefinitely. Defaults \
            to ``1``.
        '''
        entities = path.split(self.__sep)
        current = self.__sep.join(entities[:level]) + self.__sep

        print(f"\nCREATING DIR CACHE for {path} (level={level}), (current={current})...")
        print(entities)

        if current not in self.__subdirs:
            print(f"CREATED '{current}' in {self.__subdirs} AS IT DIDNT EXIST...")
            self.__subdirs.update({
                current: DirCache(sep=self.__sep)
            })

        if len(entities) - 1 == level:
            return self.__subdirs.get(path, None)
        
        return self.__subdirs[current].__create_dir_cache(
            path=path, level=level+1)