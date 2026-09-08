import sqlite3
from sqlite3 import Cursor
from folder_audit import audit
from logger_config import setup_logging

logger = setup_logging(__name__)

def initialize() -> Cursor:
    logger.info("initializing DB")
    connection = sqlite3.connect("/data/scan-data/presentation_data.db")
    cursor: Cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scan_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            expected_count INTEGER NOT NULL,
            present_count INTEGER  NOT NULL,
            missing_count INTEGER  NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    return cursor

def init_and_commit():
    cursor = initialize()
    cursor.connection.commit()
    cursor.connection.close()

def update_db() -> None:
    "function to run periodically to save details in sqlite file to be read by grafana"
    logger.info("updating DB")
    cursor = initialize()
    audit_data = audit()
    
    cursor.execute(
        "INSERT INTO scan_data "
        "(expected_count, present_count, missing_count) VALUES (?, ?, ?)",
        (audit_data["expected_count"], audit_data["present_count"], audit_data["missing_count"])
    )
    cursor.connection.commit()
    cursor.connection.close()
