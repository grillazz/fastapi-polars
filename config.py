import os

from pydantic import BaseModel, AnyHttpUrl, Field
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
        default="your_iced_data", description="Name of the DataFrame"
    )
    s3_credentials: S3Credentials = S3Credentials()


settings: Settings = Settings()
