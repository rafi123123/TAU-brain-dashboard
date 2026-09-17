from sqlite3  import Cursor
from datetime import datetime
from enum import StrEnum
from pydantic import BaseModel, model_validator
from typing_extensions import Self
from dotenv import load_dotenv
import os

load_dotenv()
CHECK_BEFORE_INSERT = bool(os.getenv("CHECK_BEFORE_INSERT", True))
AMOUNT_CHECK_BEFORE_INSERT = int(os.getenv("AMOUNT_CHECK_BEFORE_INSERT", 5))

class Status(StrEnum):
    COMPLETED = "COMPLETED"
    FREESURFER_SUBJECT_ONLY_DIR="FREESURFER_SUBJECT_ONLY_DIR"
    FREESURFER_SUBJECT_SESSION_DIR="FREESURFER_SUBJECT_SESSION_DIR"
    FREESURFER_SUBJECT_SESSION_LONG_DIR="FREESURFER_SUBJECT_SESSION_LONG_DIR"

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

def create_scan_history_table(cursor: Cursor) -> None:
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scan_data_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at TEXT DEFAULT (datetime('now')),
            scan_time DATETIME,
            source TEXT NOT NULL,
            status TEXT,
            full_path TEXT,
            scanner_subject_id TEXT,
            UNIQUE(scan_time, source, status)  
        )
    """)



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
                full_path=data[2],
                status=data[3],
                scanner_subject_id=data[4],
            )
    cursor.executemany(INSERT_SQL,scan_data_list)