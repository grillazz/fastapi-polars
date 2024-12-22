from attrs import define, field

from threading import Lock

import s3fs

class SingletonMetaNoArgs(type):
    """
    Singleton metaclass for classes without parameters on constructor,
    for compatibility with FastApi Depends() function.
    """

    _instances = {}

    _lock: Lock = Lock()

    def __call__(cls):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__()
                cls._instances[cls] = instance
        return cls._instances[cls]


@define
class S3Service(metaclass=SingletonMetaNoArgs):
    """
    Service to manage the initialization of an async S3FileSystem session.
    """