from typing import Optional as _Optional
from typing import Iterator as _Iterator
from typing import Callable as _Callable
from typing import Union as _Union


from ._helper import join_paths as _join_paths
from ._helper import infer_separator as _infer_sep


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
        # __cache explained:
        #
        #   - __cache[file] --> Cache
        #   - __cache[dir] --> list[int, dict[str, dict | Cache]]
        #       - int:
        #           - 0 --> dir has not been traversed
        #           - 1 --> dir has been traversed top-level
        #           - 2 --> dir has traversed recursively
        #       - dict[str, dict | Cache]:
        #           - A __cache-like dictionary.
        #
        #  Example:
        #
        #  {
        #    'file1.txt': Cache(),
        #    'dir' : [
        #      1,
        #      {
        #        'file2.txt': Cache(),
        #        'subdir': [0, {}]
        #      }
        #    ]
        #  }
        self.__cache: dict[str, _Union[list[int, dict], Cache]] = [0, dict()]


    def purge(self) -> None:
        '''
        Purges cache.
        '''
        self.__cache = [0, dict()]


    def get_size(self, file_path: str) -> _Optional[int]:
        '''
        Returns the file's cached size if it exists, \
        else returns ``None``.

        :param str file_path: The file's absolute path.
        '''
        if (cache := self.__get_file_cache_ref(
                file_path=file_path,
                sep=_infer_sep(path=file_path),
                create_if_missing=False
        )) is not None:
            return cache.get_size()
    

    def cache_size(self, file_path: str, size: int) -> None:
        '''
        Caches the provided size.

        :param str file_path: The file's absolute path.
        :param int size: The file's size.
        '''
        self.__get_file_cache_ref(
            file_path=file_path,
            sep=_infer_sep(path=file_path),
            create_if_missing=True).set_size(size)
    

    def get_metadata(self, file_path: str) -> _Optional[dict[str, str]]:
        '''
        Returns the file's cached metadata if they exist, \
        else returns ``None``.

        :param str file_path: The file's absolute path.
        '''
        if (cache := self.__get_file_cache_ref(
                file_path=file_path,
                sep=_infer_sep(path=file_path),
                create_if_missing=False
        )) is not None:
            return cache.get_metadata()
        

    def cache_metadata(self, file_path: str, metadata: dict[str, str]) -> None:
        '''
        Caches the provided metadata.

        :param str file_path: The file's absolute path.
        :param dict[str, str]: The file's metadata.
        '''
        self.__get_file_cache_ref(
            file_path=file_path,
            sep=_infer_sep(path=file_path),
            create_if_missing=True).set_metadata(metadata)


    def get_content_iterator(
        self,
        dir_path: str,
        recursively: bool,
        include_dirs: bool
    ) -> _Optional[_Iterator[str]]:
        '''
        Returns an iterator capable of going through all ``Cache`` \
        instances' keys, either within the ordinary cache or the top-level \
        cache depending on the value of ``recursively``. Returns ``None`` \
        if no ``Cache`` instances exist for the requested cache type.

        :param str dir_path: The absolute path of the directory \
            whose contents are to be iterated.
        :param bool recursively: Indicates whether to iterate \
            instances recursively by looking in the ordinary \
            cache, or not by looking in the top-level cache.
        :param bool include_dirs: Indicates whether to include \
            any directories when ``recursively`` has been set \
            to ``False``.
        '''
        sep = _infer_sep(path=dir_path)

        dir_cache = self.__get_dir_cache_ref(
            dir_path=dir_path,
            sep=sep,
            create_if_missing=False)
        
        if dir_cache is None:
            return None
        else:
            n = dir_cache[0]
            if not ((n == 2) or (n == 1 and not recursively)):
                return None

        def iterate_recursively(dir_path: str) -> _Iterator[str]:
            '''
            Iterates the specified directory's \
            contents recursively.

            :param str dir_path: The absolute path \
                of the directory in question.
            '''
            for name in (dir_cache := self.__get_dir_cache_ref(
                dir_path=dir_path,
                sep=sep,
                create_if_missing=False
            )[1]):
                abs_path = _join_paths(sep, dir_path, name)
                if isinstance(dir_cache[name], Cache):
                    yield abs_path
                else:
                    yield from iterate_recursively(
                        dir_path=abs_path)
                    
        if recursively:
            return (fp for fp in iterate_recursively(dir_path=dir_path))
        else:
            dir_cache = self.__get_dir_cache_ref(
                dir_path=dir_path,
                sep=sep,
                create_if_missing=False)[1]
            return (_join_paths(sep, dir_path, name) for name in filter(
                    lambda name: isinstance(dir_cache[name], Cache) or include_dirs,
                    dir_cache))
    

    def cache_contents(
        self,
        dir_path: str,
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

        # Grab the cache corresponding to the directory.
        # Create it if it does not already exist.
        sep = _infer_sep(path=dir_path)

        dir_cache = self.__get_dir_cache_ref(
            dir_path=dir_path,
            sep=sep,
            create_if_missing=True)
        
        # Go through the directory's contents
        # and cache them.
        if recursively:
            subdirs = set()
            for path in sorted(iterator):
                _ = self.__get_file_cache_ref(
                    file_path=path,
                    sep=sep,
                    create_if_missing=True)
                subdirs.add(sep.join(path.split(sep)[:-1]))
            # TODO: Check the case where an intermediate subdir
            #       has no files, only subdirs, so it won't
            #       be set as having been recursively traversed.
            # Set every subdir as recursively traversed too.
            for subdir in subdirs:
                self.__get_dir_cache_ref(
                    dir_path=subdir,
                    sep=sep,
                    create_if_missing=False)[0] = 2
        else:
            for path in sorted(iterator):
                if path not in dir_cache[1]:
                    dir_cache[1].update({
                        path.removeprefix(dir_path):
                        Cache() if is_file(path) else [0, dict()]})

        # Set directory as traversed.
        dir_cache[0] = 2 if recursively else 1


    def __get_file_cache_ref(
        self,
        file_path: str,
        sep: str,
        create_if_missing: bool
    ) -> Cache:
        '''
        Returns a reference to the ``Cache`` instance \
        that corresponds to the specified file. If said \
        instance does not exist and ``create_if_missing`` \
        has been set to ``True``, then this method goes on \
        to create and return it, else returns ``None``.

        :param str file_path: The absolute path to the file \
            in question.
        :param str sep: The path's separator.
        :param bool create_if_missing: Read description.
        '''

        # Remove any left separator.
        *parent_dirs, file_name = file_path.lstrip(sep).split(sep)

        cache = self.__cache[1]

        for dir in parent_dirs:
            dir += sep
            if dir not in cache:
                cache.update({dir: [0, dict()]})
            cache = cache[dir][1]
        
        if file_name not in cache:
            if create_if_missing:
                cache.update({file_name: Cache()})
            else:
                return None

        return cache[file_name]
    

    def __get_dir_cache_ref(
        self,
        dir_path: str,
        sep: str,
        create_if_missing: bool
    ) -> _Optional[list[int, dict]]:
        '''
        Returns a reference to the list that corresponds \
        to the specified directory's cache. If said directory \
        does not exist and ``create_if_missing`` has been set \
        to ``True``, then this method goes on to create and \
        return it, else returns ``None``.

        :param str dir_path: The absolute path to the file \
            in question.
        :param str sep: The path's separator.
        :param bool create_if_missing: Read description.
        '''

        # Remove any left/right separator.
        dir_path = dir_path.strip(sep)

        *parent_dirs, dir_name = map(
            lambda name: name + sep,
            dir_path.split(sep))

        cache = self.__cache[1]

        for dir in parent_dirs:
            if dir not in cache:
                cache.update({dir: [0, dict()]})
            cache = cache[dir][1]
        
        if dir_name not in cache:
            if create_if_missing:
                cache.update({dir_name: [0, dict()]})
            else:
                return None

        return cache[dir_name]
    