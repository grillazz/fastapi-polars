from typing import Any, TypeVar, Optional
import logging

from sqlalchemy.orm import declared_attr, DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncSession

T = TypeVar("T", bound="Base")
logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    id: Any
    __name__: str

    # Generate __tablename__ automatically

    @declared_attr
    def __tablename__(self) -> str:
        return self.__name__.lower()

    async def save(self, db_session: AsyncSession) -> Optional[T]:
        """Save the current instance to the database.

        Args:
            db_session: The async database session

        Returns:
            The saved instance or None if an error occurred
        """
        try:
            db_session.add(self)
            await db_session.commit()
            await db_session.refresh(self)
            return self
        except Exception as e:
            await db_session.rollback()
            logger.error(f"Error saving {self.__class__.__name__}: {str(e)}")
            raise
