from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import traceback
from db.query import AsyncQuerier
# the enums are generated from the postgres types in schema.sql
from db.models import JobStatus, Source, Status


load_dotenv()

TIME_BUFFER_BETWEEN_SCANS_MINUTES = int(os.getenv("TIME_BUFFER_BETWEEN_SCANS_MINUTES", 15))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/scan_data")
DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", 25))


async def update_job_based_on_exceptions(conn: AsyncConnection, job_id: int, all_exc: list[Exception | None]) -> None:
    querier = AsyncQuerier(conn)
    if not all_exc:
        await querier.update_job_entry(
            status=JobStatus.COMPLETED,
            error_msg=None,
            stop_time=datetime.now(tz=timezone.utc),
            id=job_id,
        )
    else:
        await querier.update_job_entry(
            status=JobStatus.ERROR,
            error_msg=str(["".join(traceback.format_exception(e)) for e in all_exc]),
            stop_time=datetime.now(tz=timezone.utc),
            id=job_id,
        )

async def get_last_completed_scan_with_buffer(conn: AsyncConnection) -> dict[str, datetime]:
    return_dict = {source.value: None for source in Source}
    async for row in AsyncQuerier(conn).get_last_completed_jobs():
        result = row.start_time.astimezone(timezone.utc)
        result = result - timedelta(minutes=TIME_BUFFER_BETWEEN_SCANS_MINUTES)
        return_dict[row.source.value] = result
    return return_dict


# the generated queries run on sqlalchemy, psycopg is still the driver underneath
_engine = create_async_engine(
    make_url(DATABASE_URL).set(drivername="postgresql+psycopg"),
    pool_size=DB_POOL_MAX_SIZE,
    max_overflow=0,
)

async def close_engine() -> None:
    await _engine.dispose()

def connection():
    """Borrow a connection from the pool, commits on success / rolls back on error."""
    return _engine.begin()
