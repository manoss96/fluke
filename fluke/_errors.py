from typing import Optional as _Optional


class Error():
    '''
    A class whose instances represent any type of errors.

    :param str uri: The URI of the file related to the error.
    :param bool is_src: Indicates whether the error occurred \
        during the file's extraction or loading.
    :param Optional[str] msg: A message explaining the error.
    '''

    __NUM_LINES = 100

    def __init__(self, uri: str, is_src: bool, msg: _Optional[str]) -> None:
        '''
        A class whose instances represent any type of errors.

        :param str uri: The URI of the file related to the error.
        :param bool is_src: Indicates whether the error occurred \
            during the file's extraction or loading.
        :param Optional[str] msg: A message explaining the error.
        '''
        self.__uri = uri
        self.__is_src = is_src
        self.__msg = msg

    def __str__(self) -> str:
        '''
        Converts an instance of this class to a string.
        '''
        msg = "\n" + (self.__NUM_LINES * "=") + "\n"
        msg += f"{'SOURCE' if self.__is_src else 'DESTINATION'} ERROR: {self.__uri}"
        msg += "\n" + (self.__NUM_LINES  * "-") + "\n"
        msg += self.__msg
        msg += "\n" + (self.__NUM_LINES * "=") + "\n"
        return msg

    def get_message(self) -> str:
        '''
        Returns the error message.
        '''
        return self.__msg
        