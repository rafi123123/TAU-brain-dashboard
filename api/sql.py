from sqlite3  import Cursor
from datetime import datetime
from enum import StrEnum
from pydantic import BaseModel

class Status(StrEnum):
    COMPLETED = "COMPLETED"
    FREESURFER_SUBJECT_ONLY_DIR="FREESURFER_SUBJECT_ONLY_DIR"
    FREESURFER_SUBJECT_SESSION_DIR="FREESURFER_SUBJECT_SESSION_DIR"
    FREESURFER_SUBJECT_SESSION_LONG_DIR="FREESURFER_SUBJECT_SESSION_LONG_DIR"

class Sources(StrEnum):
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
    scan_time: datetime
    source: str
    full_path: str | None
    status: Status | None = None
    scanner_subject_id: str | None = None

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


def insert_scan(cursor: Cursor, scan_data: ScanData) -> None:
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
    cursor.executemany(INSERT_SQL,scan_data_list)