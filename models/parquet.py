from uuid import uuid4

from sqlalchemy import String, select, func
from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncSession

from models.base import Base



class ParquetIndex(Base):
    __tablename__ = "parquet_index"
    uuid: Mapped[UUID] = mapped_column(UUID, primary_key=True, default=uuid4)
    s3_url: Mapped[str] = mapped_column(String, unique=True)

    @classmethod
    async def get_row_count(cls, db_session: AsyncSession) -> int:
        stmt = select(func.count(ParquetIndex.uuid))
        result = await db_session.execute(stmt)
        count = result.scalar()
        return count

