from enum import Enum as _Enum
from typing import Any as _Any
from typing import Optional as _Optional
from typing import Iterator as _Iterator
from typing import Callable as _Callable

from ._helper import infer_separator


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

    :param str | None path: The path of the directory \
        being cached. If ``None``, then the empty path \
        is inferred.
    '''

    class State(_Enum):
        NOT_TRAVERSED = 0
        TOP_LEVEL_TRAVERSED = 1
        RECURSIVELY_TRAVERSED = 2


    def __init__(self, path: _Optional[str]):
        '''
        A class whose instances represent cached \
        information about a directory.

        :param str | None path: The path of the directory \
            being cached. If ``None``, then the empty path \
            is inferred.
        '''
        if path is None:
            self.__sep = '/'
            self.__has_sep_root = False
        else:
            self.__sep = infer_separator(path)
            self.__has_sep_root = path.startswith(self.__sep)
        self.purge()


    def remove_sep_prefix(func: _Callable) -> _Any:
        '''
        A decorator function used for removing \
        any seperator prefixes from paths provided \
        to the functions being decorated.
        '''
        def wrapper(*args, **kwargs):
            if (path := kwargs.get('path', None)) is not None:
                kwargs['path'] = path.lstrip(args[0].__sep)
            else:
                args_new = []
                for i, arg in enumerate(args):
                    if i == 1:
                        arg = arg.lstrip(args[0].__sep)
                    args_new.append(arg)
                args = tuple(args_new)
            return func(*args, **kwargs)
        return wrapper


    def purge(self) -> None:
        '''
        Purges the cache.
        '''
        self.__state = __class__.State.NOT_TRAVERSED
        self.__subdirs: dict[str, DirCache] = dict()
        self.__files: dict[str, FileCache] = dict()


    @remove_sep_prefix
    def get_size(self, path: str) -> _Optional[int]:
        '''
        Returns the file's cached size if it exists, \
        else returns ``None``.

        :param str path: The file's absolute path.
        '''
        return (cache.get_size()
        if (cache := self.__get_file_cache(path)) is not None
        else None)
    

    @remove_sep_prefix
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
    

    @remove_sep_prefix
    def get_metadata(self, path: str) -> _Optional[int]:
        '''
        Returns the file's cached metadata if they exist, \
        else returns ``None``.

        :param str path: The file's absolute path.
        '''
        return (cache.get_metadata()
        if (cache := self.__get_file_cache(path)) is not None
        else None)
    

    @remove_sep_prefix
    def cache_metadata(
        self,
        path: str,
        metadata: dict[str, str]
    ) -> None:
        '''
        Caches the provided metadata.

        :param str path: The file's absolute path.
        :param dict[str, str] metadata: The file's metadata.

        :note: This method goes on to creates any \
            necessary ``FileCache/DirCache`` instances \
            if they do not already exist.
        '''
        self.__create_file_cache(path).set_metadata(metadata)


    @remove_sep_prefix
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
        if path == '':
            cache = self
        else:
            cache = self.__get_dir_cache(path=path)

        if (
            cache is None or
            cache.__state == __class__.State.NOT_TRAVERSED or
            (
                recursively and
                cache.__state != __class__.State.RECURSIVELY_TRAVERSED
            )
        ):
            return None
    
        def iterate_contents(
            dc: DirCache,
            recursively: bool
        ) -> _Iterator[str]:
            if recursively:
                for entity in dc.__files:
                        yield entity
                for subdir in dc.__subdirs.values():
                    for entity in iterate_contents(subdir, recursively):
                        yield entity
            else:
                for entity in (
                    dc.__files | (
                        dc.__subdirs
                        if include_dirs
                        else dict()
                )):
                    yield entity

        iterator = iterate_contents(cache, recursively)

        if self.__has_sep_root:
            iterator = map(
                lambda path: self.__sep + path,
                iterator)

        return sorted(iterator)


    @remove_sep_prefix
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
        if path != '':
            cache = self.__create_dir_cache(path=path)
        else:
            cache = self

        if cache.__state == __class__.State.NOT_TRAVERSED:
            if recursively:
                cache.__state = __class__.State.RECURSIVELY_TRAVERSED
            else:
                cache.__state = __class__.State.TOP_LEVEL_TRAVERSED
        elif cache.__state == __class__.State.TOP_LEVEL_TRAVERSED and recursively:
            cache.__state = __class__.State.RECURSIVELY_TRAVERSED

        # NOTE: Cache contents using top-level
        #       ``DirCache`` instance.
        for ep in sorted(iterator):
            if is_file(ep):
                self.__create_file_cache(ep.lstrip(self.__sep))
            else:
                self.__create_dir_cache(ep.lstrip(self.__sep))


    @classmethod
    def _create_dir_cache(
        cls,
        sep: str,
        has_sep_root: bool
    ) -> 'DirCache':
        '''
        Creates and returns a ``DirCache`` instance.

        :param str sep: The path separator used.
        :param has_sep_root: Indicates whether the \
            underlying storage system being cached \
            assumes a root directory whose name is \
            the separator.
        '''
        instance = cls.__new__(cls)
        instance.__sep = sep
        instance.__has_sep_root = has_sep_root
        instance.purge()
        return instance
    

    def __get_file_cache(
        self,
        path: str,
        level: int = 1
    ) -> _Optional[FileCache]:
        '''
        Returns the ``FileCache`` instance that \
        corresponds to the provided path, or ``None``
        if no such instance exists.

        :param str path: The file's absolute path.
        :param int level: A helping variable that prevents \
            this method from recursing indefinitely. Defaults \
            to ``1``.

        :note: The provided path must have had any leading \
            separators removed prior to being passes to this \
            method.
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

        :param str path: The directory's absolute path.
        :param int level: A helping variable that prevents \
            this method from recursing indefinitely. Defaults \
            to ``1``.

        :note: The provided path must have had any leading \
            separators removed prior to being passes to this \
            method.
        '''
        if path == '':
            return self
        
        entities = path.split(self.__sep)
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

        :param str path: The file's absolute path.
        :param int level: A helping variable that prevents \
            this method from recursing indefinitely. Defaults \
            to ``1``.

        :note: The provided path must have had any leading \
            separators removed prior to being passes to this \
            method.
        '''
        entities = path.split(self.__sep)

        if len(entities) == level:
            if path not in self.__files:
                self.__files.update({path: FileCache()})
            return self.__files[path]
        
        current = self.__sep.join(entities[:level]) + self.__sep

        if current not in self.__subdirs:
            self.__subdirs.update({
                current: DirCache._create_dir_cache(
                    sep=self.__sep,
                    has_sep_root=self.__has_sep_root)
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

        :param str path: The directory's absolute path.
        :param int level: A helping variable that prevents \
            this method from recursing indefinitely. Defaults \
            to ``1``.

        :note: The provided path must have had any leading \
            separators removed prior to being passes to this \
            method.
        '''        
        entities = path.split(self.__sep)
        current = self.__sep.join(entities[:level]) + self.__sep

        if current not in self.__subdirs:
            self.__subdirs.update({
                current: DirCache._create_dir_cache(
                    sep=self.__sep,
                    has_sep_root=self.__has_sep_root)
            })

        if len(entities) - 1 == level:
            return self.__subdirs.get(path, None)
        
        return self.__subdirs[current].__create_dir_cache(
            path=path, level=level+1)