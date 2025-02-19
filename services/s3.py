import io
from s3fs.core import S3FileSystem
import polars as pl
from attrs import define, field
from config import settings as global_settings
from services.utlis import SingletonMetaNoArgs


@define
class S3Service(metaclass=SingletonMetaNoArgs):
    """
    A service class for interacting with S3, providing methods to handle Parquet files.

    Attributes:
        s3_key (str): S3 access key.
        s3_secret (str): S3 secret key.
        s3_url (str): S3 endpoint URL.
        s3fs_client (S3FileSystem): S3 filesystem client.
    """

    s3_key: str = global_settings.s3_credentials.key
    s3_secret: str = global_settings.s3_credentials.secret
    s3_url: str = global_settings.s3_credentials.endpoint_url
    s3fs_client: S3FileSystem = field(init=False)

    def __attrs_post_init__(self):
        """
        Initializes the S3 filesystem client after the class is instantiated.
        """
        self.s3fs_client = S3FileSystem(
            key=self.s3_key,
            secret=self.s3_secret,
            endpoint_url=self.s3_url,
        )

    def materialize_dataframe(self, dataframe: pl.DataFrame, path: str):
        """
        Writes a Polars DataFrame to a Parquet file and uploads it to S3.

        Args:
            dataframe (pl.DataFrame): The DataFrame to be written.
            path (str): The S3 path where the Parquet file will be stored.

        Returns:
            dict: A dictionary containing the status and path of the uploaded file.
        """
        parquet_bytes = io.BytesIO()
        dataframe.write_parquet(parquet_bytes)
        parquet_bytes.seek(0)  # Reset buffer to the beginning
        with self.s3fs_client.open(f"s3://daily/{path}", "wb") as f:
            f.write(parquet_bytes.getvalue())

        return {"status": "success", "path": path}

    def list_parquet_files(self, bucket: str):
        """
        Lists all Parquet files in the specified S3 bucket.

        Args:
            bucket (str): The S3 bucket name.

        Returns:
            list: A list of Parquet file paths.
        """
        return [f for f in self.s3fs_client.ls(bucket) if f.endswith(".parquet")]

    def read_parquet_file(self, path: str) -> pl.DataFrame:
        """
        Reads a Parquet file from S3 into a Polars DataFrame.

        Args:
            path (str): The S3 path of the Parquet file.

        Returns:
            pl.DataFrame: The DataFrame read from the Parquet file.
        """
        with self.s3fs_client.open(path, "rb") as f:
            return pl.read_parquet(f)

    def delete_parquet_file(self, path: str):
        """
        Deletes a Parquet file from S3.

        Args:
            path (str): The S3 path of the Parquet file to be deleted.
        """
        self.s3fs_client.rm(path)

    def parquet_file_exists(self, path: str) -> bool:
        """
        Checks if a Parquet file exists at the specified S3 path.

        Args:
            path (str): The S3 path to check.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        return self.s3fs_client.exists(path)

    def merge_parquet_files(self, bucket: str) -> pl.DataFrame:
        """
        Merges all Parquet files in the specified S3 bucket into a single DataFrame.

        Args:
            bucket (str): The S3 bucket name.

        Returns:
            pl.DataFrame: The merged DataFrame.
        """
        parquet_files = self.list_parquet_files(bucket)
        dataframes = [self.read_parquet_file(f) for f in parquet_files]
        return pl.concat(dataframes)

    def list_buckets(self) -> list:
        """
        Lists all available buckets in the S3 storage.

        Returns:
            list: A list of bucket names.
        """
        return self.s3fs_client.ls("/")


    def create_bucket(self, bucket_name: str):
        """
        Creates a new bucket in the S3 storage.

        Args:
            bucket_name (str): The name of the bucket to be created.

        Returns:
            dict: A dictionary containing the status and bucket name.
        """
        self.s3fs_client.mkdir(bucket_name)
        return {"status": "success", "bucket_name": bucket_name}

    def list_files(self, bucket_name: str) -> list:
        """
        Lists all files in the specified S3 bucket.

        Args:
            bucket_name (str): The name of the bucket.

        Returns:
            list: A list of file paths in the bucket.
        """
        return self.s3fs_client.ls(bucket_name)