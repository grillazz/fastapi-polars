import os

from pydantic import (
    BaseModel,
    AnyHttpUrl,
)
from pydantic_settings import BaseSettings

class S3Credentials(BaseModel):
    endpoint_url: AnyHttpUrl = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    key: str = os.getenv("S3_KEY", "minio")
    secret: str = os.getenv("S3_SECRET", "minio123")


class Settings(BaseSettings):

    s3_credentials: S3Credentials = S3Credentials()


settings: Settings = Settings()