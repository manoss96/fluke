import re as _re


def join_paths(sep: str, *paths: str) -> str:
    '''
    Joins the provided paths into a single path \
    by using the provided separator and returns the result.

    :param str sep: The path separator that is to be used.
    :param *str paths: The paths that are to be joined.
    '''
    paths = [p for p in paths if p != '']

    if len(paths) == 0:
        return ''

    path = paths[0]

    for i in range(1, len(paths)):
        path = f"{path.rstrip(sep)}{sep}{paths[i].lstrip(sep)}"

    return path


def relativize_path(parent: str, child: str, sep: str) -> str:
    '''
    Modifies the child path so that it is \
    relative to the parent path.

    :param str parent: The parent path.
    :param str child: The child path.
    :param str sep: The path separator used.
    '''
    return child.removeprefix(parent).lstrip(sep)


def infer_separator(path: str) -> str:
    '''
    Infers the separator from the provided path \
    and returns it. If no separator can be inferred, \
    this method will return ``/``.

    :param str path: The path from which the separator \
        is inferred.
    '''
    bs = '\\'
    seps = {'/', 2 * bs, '>'}

    if path in seps:
        return path
    
    seps = ''.join(seps)
    match = _re.fullmatch(
        pattern=fr"({4 * bs}|[{seps}])?(?:[^{seps}])+((?(1)\1|(?:{4 * bs}|[{seps}])))?(?:[^{seps}]+(?(1)\1|\2)?)*",
        string=path,
    )

    if match is None:
        return '/'

    return match.group(1) or match.group(2) or '/'