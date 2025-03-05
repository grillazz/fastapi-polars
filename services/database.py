from collections.abc import AsyncGenerator
from attrs import define, field
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings as global_settings


@define
class DatabaseService:
    """
    A service class for managing the database connection and session.
    """

    engine = field(init=False)
    async_session_factory = field(init=False)

    def __attrs_post_init__(self):
        self.engine = create_async_engine(
            global_settings.asyncpg_url.unicode_string(),
            future=True,
            echo=True,
        )
        self.async_session_factory = async_sessionmaker(
            self.engine,
            autoflush=False,
            expire_on_commit=False,
        )

    async def get_db(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Dependency function to get an instance of the database session.

        Yields:
            AsyncSession: An asynchronous database session.
        """
        async with self.async_session_factory() as session:
            try:
                yield session
            except Exception as e:
                raise
