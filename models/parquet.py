from uuid import uuid4

from sqlalchemy import String
from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy.dialects.postgresql import JSONB, UUID

from models.base import Base


class ParquetIndex(Base):
    __tablename__ = "parquet_index"
    uuid: Mapped[UUID] = mapped_column(UUID, primary_key=True, default=uuid4)
    s3_url: Mapped[str] = mapped_column(String, unique=True)

