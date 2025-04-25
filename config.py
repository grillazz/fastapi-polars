import os

from pydantic import BaseModel, AnyHttpUrl, Field, PostgresDsn, computed_field
from pydantic_core._pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings


class S3Credentials(BaseModel):
    endpoint_url: AnyHttpUrl = Field(
        default_factory=lambda: os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    )
    key: str = Field(default_factory=lambda: os.getenv("S3_KEY", "minio"))
    secret: str = Field(default_factory=lambda: os.getenv("S3_SECRET", "minio123"))


class Settings(BaseSettings):
    dataframe_dump_size: int = Field(
        default=10, description="Size threshold for dumping the DataFrame in MB"
    )
    dataframe_name: str = Field(
        default="your_books_data", description="Name of the DataFrame"
    )
    index_engine: str = Field(
        default="adbc",
        description="ADBC: Arrow Database Connectivity https://arrow.apache.org/docs/format/ADBC.html",
    )
    index_table: str = Field(
        default="books_index_2",
        description="Name of the index table in the database",
    )

    s3_credentials: S3Credentials = S3Credentials()

    POSTGRES_USER: str = Field(default="metabase")
    POSTGRES_PASSWORD: str = Field(default="secret")
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_DB: str = Field(default="metabase")

    @computed_field
    @property
    def asyncpg_url(self) -> PostgresDsn:
        return MultiHostUrl.build(
            scheme="postgresql+asyncpg",
            username=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            host=self.POSTGRES_HOST,
            path=self.POSTGRES_DB,
        )

    @computed_field
    @property
    def pg_url(self) -> PostgresDsn:
        return MultiHostUrl.build(
            scheme="postgresql",
            username=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            host=self.POSTGRES_HOST,
            path=self.POSTGRES_DB,
        )


settings: Settings = Settings()
