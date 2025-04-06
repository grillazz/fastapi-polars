from datetime import date
from typing import Optional

from sqlalchemy import ForeignKey, Text, BigInteger, Date
from sqlalchemy.orm import mapped_column, Mapped

from models.base import Base


class BooksIndex(Base):
    __tablename__ = 'books_index'

    isbn: Mapped[str] = mapped_column(Text)
    pages: Mapped[Optional[int]] = mapped_column(BigInteger)
    author: Mapped[Optional[str]] = mapped_column(Text)
    pub_date: Mapped[Optional[date]] = mapped_column(Date)
    pid: Mapped[Optional[int]] = mapped_column(BigInteger)
    hash: Mapped[Optional[int]] = mapped_column(BigInteger, primary_key=True)
    parquet_id: Mapped[int] = mapped_column(
            BigInteger,
            ForeignKey("parquet_index.id", ondelete="CASCADE"),
            nullable=False
    )
