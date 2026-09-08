import sqlite3
from sqlite3 import Cursor
from folder_audit import audit
import sys


def initialize() -> Cursor:
    connection = sqlite3.connect("/data/scan-data/presentation_data.db")
    cursor: Cursor = connection.cursor()

    # 3. Create a table
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


def update_db() -> None:
    "function to run periodically to save details in sqlite file to be read by grafana"

    cursor = initialize()
    audit_data = audit()
    
    cursor.execute(
        "INSERT INTO scan_data "
        "(expected_count, present_count, missing_count) VALUES (?, ?, ?)",
        (audit_data["expected_count"], audit_data["present_count"], audit_data["missing_count"])
    )
    cursor.connection.commit()
    cursor.connection.close()



if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "init":
        cursor = initialize()
        cursor.connection.commit()
        cursor.connection.close()
    else:
        update_db()