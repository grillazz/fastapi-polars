import io
from s3fs.core import S3FileSystem
import polars as pl
from attrs import define, field
from config import settings as global_settings
from services.utlis import SingletonMetaNoArgs


@define
class S3Service(metaclass=SingletonMetaNoArgs):
    s3_key: str = global_settings.s3_credentials.key
    s3_secret: str = global_settings.s3_credentials.secret
    s3_url: str = global_settings.s3_credentials.endpoint_url
    s3fs_client: S3FileSystem = field(init=False)

    def __attrs_post_init__(self):
        self.s3fs_client = S3FileSystem(
            key=self.s3_key,
            secret=self.s3_secret,
            endpoint_url=self.s3_url,
        )
        # yield self.s3fs_client

    def materialize_dataframe(self, dataframe: pl.DataFrame, path: str):
        parquet_bytes = io.BytesIO()
        dataframe.write_parquet(parquet_bytes)
        parquet_bytes.seek(0)  # Reset buffer to the beginning
        with self.s3fs_client.open(f"s3://daily/{path}", 'wb') as f:
            f.write(parquet_bytes.getvalue())

        return {"status": "success", "path": path}

    #
    # def list_parquet_files(self, bucket: str):
    #     return [f for f in self.s3fs_client.ls(bucket) if f.endswith('.parquet')]
    #
    # def read_parquet_file(self, path: str) -> pl.DataFrame:
    #     with self.s3fs_client.open(path, 'rb') as f:
    #         return pl.read_parquet(f)
    #
    # def merge_parquet_files(self, bucket: str) -> pl.DataFrame:
    #     parquet_files = self.list_parquet_files(bucket)
    #     dataframes = [self.read_parquet_file(f) for f in parquet_files]
    #     return pl.concat(dataframes)