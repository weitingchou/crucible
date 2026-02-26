import contextlib
from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


class LeaseManager:
    """Acquires PostgreSQL advisory locks to ensure exclusive access to a SUT.

    Advisory lock IDs are integers; callers are responsible for mapping a
    resource name (e.g. cluster ID) to a stable integer key.

    Usage::

        lm = LeaseManager(db_url)
        async with lm.acquire(resource_id=42):
            # only one worker holds this lock at a time
            ...
    """

    def __init__(self, db_url: str) -> None:
        self._engine = create_async_engine(db_url)

    @contextlib.asynccontextmanager
    async def acquire(self, resource_id: int) -> AsyncIterator[None]:
        async with AsyncSession(self._engine) as session:
            await session.execute(
                text("SELECT pg_advisory_lock(:lock_id)"),
                {"lock_id": resource_id},
            )
            try:
                yield
            finally:
                await session.execute(
                    text("SELECT pg_advisory_unlock(:lock_id)"),
                    {"lock_id": resource_id},
                )

    async def close(self) -> None:
        await self._engine.dispose()
