from typing import Optional
from uuid import uuid4

from sqlalchemy import String, ForeignKey, CheckConstraint, Text, Integer
from sqlalchemy.orm import mapped_column, Mapped, relationship
from sqlalchemy.dialects.postgresql import UUID

from models.base import Base
from models.parquet import ParquetIndex



class BooksIndex(Base):
    __tablename__ = "books_index"

    uuid: Mapped[UUID] = mapped_column(UUID, primary_key=True, default=uuid4)
    pages: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    isbn: Mapped[str] = mapped_column(String(17), unique=True, nullable=False)
    author: Mapped[str] = mapped_column(String(255), nullable=False)
    parquet_uuid: Mapped[UUID] = mapped_column(
        UUID,
        ForeignKey("parquet_index.uuid", ondelete="CASCADE"),
        nullable=False
    )

    # Relationship to ParquetIndex
    parquet: Mapped[ParquetIndex] = relationship("ParquetIndex", back_populates="books")

    # # Add constraint for ISBN format validation
    # __table_args__ = (
    #     CheckConstraint(
    #         "(length(regexp_replace(isbn, '[-\\s]', '')) = 10 AND regexp_replace(isbn, '[-\\s]', '') ~ '^[0-9]{9}[0-9X]$') OR "
    #         "(length(regexp_replace(isbn, '[-\\s]', '')) = 13 AND regexp_replace(isbn, '[-\\s]', '') ~ '^[0-9]{13}$')",
    #         name="valid_isbn_format"
    #     ),
    # )