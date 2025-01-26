import io

from attrs import define, field

from threading import Lock

import s3fs
import polars as pl
from config import settings as global_settings


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
    s3_key: str = global_settings.s3_credentials.key
    s3_secret: str = global_settings.s3_credentials.secret
    s3_url: str = global_settings.s3_credentials.endpoint_url
    s3fs_client: s3fs.S3FileSystem = field(init=False)

    def __attrs_post_init__(self):
        self.s3fs_client = s3fs.S3FileSystem(
            key=self.s3_key,
            secret=self.s3_secret,
            endpoint_url=self.s3_url,
            asynchronous=True,
        )

    async def materialize_dataframe(self, dataframe: pl.DataFrame, path: str):
        session = await self.s3fs_client.set_session()
        _parquet_as_bytes = io.BytesIO()
        dataframe.write_parquet(_parquet_as_bytes)
        obj = await session.put_object(
            Bucket="daily", Key=path, Body=_parquet_as_bytes.getvalue()
        )
        await session.close()
        return obj

