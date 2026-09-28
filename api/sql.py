from psycopg import AsyncCursor
from psycopg_pool import AsyncConnectionPool
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pydantic import BaseModel, model_validator
from typing import Self
from dotenv import load_dotenv
import os
import traceback
from logger_config import get_logger


load_dotenv()
logger = get_logger(__name__)

TIME_BUFFER_BETWEEN_SCANS_MINUTES = int(os.getenv("TIME_BUFFER_BETWEEN_SCANS_MINUTES", 15))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/scan_data")
DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", 25))


class Status(StrEnum):
    FREESURFER_SUBJECT_ONLY_DIR="FREESURFER_SUBJECT_ONLY_DIR"
    FREESURFER_SUBJECT_SESSION_DIR="FREESURFER_SUBJECT_SESSION_DIR"
    FREESURFER_SUBJECT_SESSION_LONG_DIR="FREESURFER_SUBJECT_SESSION_LONG_DIR"
    UNKOWN = "UNKOWN"

class JobStatus(StrEnum):
    ONGOING = "ONGOING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"

class Source(StrEnum):
    QSIPREP = "QSIPREP"
    QSIRECON = "QSIRECON"
    FREESURFER = "FREESURFER"
    CAT = "CAT"
    TABULAR_CAT12 = "TABULAR_CAT12"
    GOOGLE_SHEETS = "GOOGLE_SHEETS"
    RAW_DATA = "RAW_DATA"
    TABULAR = "TABULAR"
    BIDS = "BIDS"
    ORCHESTRATORE = "ORCHESTRATORE" # the script that orchestrates all the scraping 
    # since it can fail it is an entry in the smb_scraping DB

class   ScanData(BaseModel):
    scan_time: datetime | None
    source: Source
    full_path: str | None
    status: Status | None 
    scanner_subject_id: str | None

    @model_validator(mode='after')
    def check_scan_time(self) -> Self:
        if not self.scan_time and ( self.source != Source.FREESURFER or not self.scanner_subject_id):
            raise ValueError("scan_time is allowed to be None only for freesurfer source given a subject ID")
        return self

async def create_scan_history_table(cursor: AsyncCursor) -> None:
    await cursor.execute("""
        CREATE TABLE IF NOT EXISTS scan_data_history (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            scraped_at TIMESTAMPTZ DEFAULT now(),
            scan_time TIMESTAMP NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            full_path TEXT,
            scanner_subject_id TEXT,
            UNIQUE(scan_time, source, status)  
        )
    """)

async def create_smb_scraping_jobs_table(cursor: AsyncCursor) -> None:
    await cursor.execute("""
        CREATE TABLE IF NOT EXISTS smb_scraping_jobs (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            start_time TIMESTAMPTZ DEFAULT now(),
            stop_time TIMESTAMPTZ,
            source TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ONGOING',
            error_msg TEXT
        )
    """)

async def insert_job_entry(cursor: AsyncCursor, source: Source) -> int:
    await cursor.execute(
        "INSERT INTO smb_scraping_jobs (source) VALUES (%s) RETURNING id", (source, )
    )
    return (await cursor.fetchone())[0]

async def upadate_job_entry(
        cursor: AsyncCursor, 
        id: int ,
        status: JobStatus, 
        error_msg: str | None = None, 
        stop_time: datetime | None = None
    ) -> None:
    await cursor.execute(
        """UPDATE smb_scraping_jobs 
        SET status=%s, error_msg=%s, stop_time=%s
        WHERE id=%s""", 
        (status, error_msg, stop_time ,id)
    )

async def update_successful_job(cursor: AsyncCursor, job_id: int):
    await upadate_job_entry(cursor, job_id, JobStatus.COMPLETED, stop_time=datetime.now(tz=timezone.utc))

async def update_job_based_on_exceptions(cursor: AsyncCursor, job_id: int, all_exc: list[Exception | None]) -> None:
    if not all_exc:
        await update_successful_job(cursor, job_id)
    else:
        await upadate_job_entry(
            cursor, 
            job_id, 
            JobStatus.ERROR,
            str(["".join(traceback.format_exception(e)) for e in all_exc]),
            datetime.now(tz=timezone.utc)
        )

async def get_last_completed_scan_with_buffer(cursor: AsyncCursor) -> dict[str, datetime]:
    await cursor.execute(
        "SELECT max(start_time), source from smb_scraping_jobs WHERE status='COMPLETED' GROUP BY source "
    )
    response = await cursor.fetchall()
    return_dict = {source.value: None for source in Source}
    for start_time, source in response:
        result = start_time.astimezone(timezone.utc)
        result = result - timedelta(minutes=TIME_BUFFER_BETWEEN_SCANS_MINUTES)
        return_dict[source] = result
    return return_dict


INSERT_SQL = """
            INSERT INTO scan_data_history (
                scan_time, 
                source, 
                status, 
                full_path, 
                scanner_subject_id
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (scan_time, source, status) DO NOTHING;
        """


async def insert_scan_single(cursor: AsyncCursor, scan_data: ScanData) -> None:
    # sqlite's INSERT OR IGNORE silently dropped rows violating NOT NULL,
    # postgres would raise and abort the transaction instead so skip them here
    if scan_data.scan_time is None:
        logger.debug(f"skipping row without scan_time: {scan_data}")
        return
    await cursor.execute(INSERT_SQL,
        (
            scan_data.scan_time, 
            scan_data.source, 
            scan_data.status, 
            scan_data.full_path,
            scan_data.scanner_subject_id
        )
    )


_pool = AsyncConnectionPool(DATABASE_URL, max_size=DB_POOL_MAX_SIZE, open=False)

async def open_pool() -> None:
    await _pool.open()

async def close_pool() -> None:
    await _pool.close()

def connection():
    """Borrow a connection from the pool, commits on success / rolls back on error."""
    return _pool.connection()
