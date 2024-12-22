import logging
import os

from pydantic import (
    BaseModel,
    AnyHttpUrl,
)
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class S3Credentials(BaseModel):
    endpoint_url: AnyHttpUrl = os.getenv("S3_ENDPOINT_URL")
    key: str = os.getenv("S3_KEY")
    secret: str = os.getenv("S3_SECRET")


class Settings(BaseSettings):

    s3_credentials: S3Credentials = S3Credentials()


settings = Settings()