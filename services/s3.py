import io
import s3fs
import polars as pl
from attrs import define, field
from config import settings as global_settings
from services.utlis import SingletonMetaNoArgs


@define
class S3Service(metaclass=SingletonMetaNoArgs):
    """
    Service class for interacting with S3 using s3fs and polars.

    Attributes:
        s3_key (str): S3 access key.
        s3_secret (str): S3 secret key.
        s3_url (str): S3 endpoint URL.
        s3fs_client (s3fs.S3FileSystem): S3 filesystem client.
    """

    s3_key: str = global_settings.s3_credentials.key
    s3_secret: str = global_settings.s3_credentials.secret
    s3_url: str = global_settings.s3_credentials.endpoint_url
    s3fs_client: s3fs.S3FileSystem = field(init=False)

    def __attrs_post_init__(self):
        """
        Post-initialization method to set up the S3 filesystem client.
        """
        self.s3fs_client = s3fs.S3FileSystem(
            key=self.s3_key,
            secret=self.s3_secret,
            endpoint_url=self.s3_url,
            asynchronous=True,
        )

    async def materialize_dataframe(self, dataframe: pl.DataFrame, path: str):
        """
        Asynchronously materialize a Polars DataFrame to S3 as a Parquet file.

        Args:
            dataframe (pl.DataFrame): The Polars DataFrame to be materialized.
            path (str): The S3 path where the Parquet file will be stored.

        Returns:
            dict: The response object from the S3 put operation.
        """
        session = await self.s3fs_client.set_session()
        parquet_bytes = io.BytesIO()
        dataframe.write_parquet(parquet_bytes)
        obj = await session.put_object(
            Bucket="daily", Key=path, Body=parquet_bytes.getvalue()
        )
        await session.close()
        return obj
