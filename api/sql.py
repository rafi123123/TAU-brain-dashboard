from sqlite3  import Cursor
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pydantic import BaseModel, model_validator
from typing_extensions import Self
from dotenv import load_dotenv
import os
import traceback

load_dotenv()
CHECK_BEFORE_INSERT = bool(os.getenv("CHECK_BEFORE_INSERT", True))
AMOUNT_CHECK_BEFORE_INSERT = int(os.getenv("AMOUNT_CHECK_BEFORE_INSERT", 5))
TIME_BUFFER_BETWEEN_SCANS_MINUTES = int(os.getenv("TIME_BUFFER_BETWEEN_SCANS_MINUTES", 15))

class Status(StrEnum):
    COMPLETED = "COMPLETED"
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

class ScanData(BaseModel):
    scan_time: datetime | None
    source: Source
    full_path: str | None
    status: Status | None 
    scanner_subject_id: str | None

    @model_validator(mode='after')
    def check_passwords_match(self) -> Self:
        if not self.scan_time and ( self.source != Source.FREESURFER or not self.scanner_subject_id):
            raise ValueError("scan_time is allowed to be None only for freesurfer source given a subject ID")
        return self

def create_scan_history_table(cursor: Cursor) -> None:
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scan_data_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at DATETIME DEFAULT (datetime('now')),
            scan_time DATETIME NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            full_path TEXT,
            scanner_subject_id TEXT,
            UNIQUE(scan_time, source, status)  
        )
    """)

def create_smb_scraping_jobs_table(cursor: Cursor) -> None:
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS smb_scraping_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time DATETIME DEFAULT (datetime('now')),
            stop_time DATETIME,
            source TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ONGOING',
            error_msg TEXT
        )
    """)

def insert_job_entry(cursor: Cursor, source: Source) -> int:
    cursor.execute(
        "INSERT INTO smb_scraping_jobs (source) VALUES (?) RETURNING id", (source, )
    )
    return cursor.fetchone()[0]

def upadate_job_entry(
        cursor: Cursor, 
        id: int ,
        status: JobStatus, 
        error_msg: str | None = None, 
        stop_time: datetime | None = None
    ) -> None:
    cursor.execute(
        """UPDATE smb_scraping_jobs 
        SET status=?, error_msg=?, stop_time=?
        WHERE id=?""", 
        (status, error_msg, stop_time ,id)
    )

def update_successful_job(cursor: Cursor, job_id: int):
    upadate_job_entry(cursor, job_id, JobStatus.COMPLETED, stop_time=datetime.now(tz=timezone.utc))

def update_job_based_on_exceptions(cursor: Cursor, job_id: int, all_exc: list[Exception | None]) -> None:
    if not all_exc:
        update_successful_job(cursor, job_id)
    else:
        upadate_job_entry(
            cursor, 
            job_id, 
            JobStatus.ERROR,
            str(["".join(traceback.format_exception(e)) for e in all_exc]),
            datetime.now(tz=timezone.utc)
        )

def get_last_completed_scan_with_buffer(cursor: Cursor) -> dict[str, datetime]:
    cursor.execute(
        "SELECT max(start_time), source from smb_scraping_jobs WHERE status='COMPLETED' GROUP BY source "
    )
    response = cursor.fetchall()
    return_dict = {source.value: None for source in Source}
    for str_time, source in response:
        result = datetime.strptime(str_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        result = result - timedelta(minutes=TIME_BUFFER_BETWEEN_SCANS_MINUTES)
        return_dict[source] = result
    return return_dict


INSERT_SQL = """
            INSERT OR IGNORE INTO scan_data_history (
                scan_time, 
                source, 
                status, 
                full_path, 
                scanner_subject_id
            )
            VALUES (? , ?, ?, ?, ?);
        """


def insert_scan_single(cursor: Cursor, scan_data: ScanData) -> None:
    cursor.execute(INSERT_SQL,
        (
            scan_data.scan_time, 
            scan_data.source, 
            scan_data.status, 
            scan_data.full_path,
            scan_data.scanner_subject_id
        )
    )

def insert_scan_many(cursor: Cursor, scan_data_list: list[tuple]) -> None:
    if CHECK_BEFORE_INSERT:
        for data in scan_data_list[:AMOUNT_CHECK_BEFORE_INSERT]:
            ScanData(
                scan_time=data[0],
                source=data[1],
                status=data[2],
                full_path=data[3],
                scanner_subject_id=data[4],
            )
    cursor.executemany(INSERT_SQL,scan_data_list)