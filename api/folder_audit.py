import base64
import json
import os
import re

import gspread
import smbclient
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import sqlite3
load_dotenv()

NAS_HOST = os.getenv("NAS_HOST")
NAS_USERNAME = os.getenv("NAS_USERNAME")
NAS_PASSWORD = os.getenv("NAS_PASSWORD")
NAS_SHARE = os.getenv("NAS_SHARE")
NAS_PATH = os.getenv("NAS_PATH", "")

SHEET_URL = os.getenv(
    "SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1YvhfZ8k3xafsN1ijtOl1uESG4w48-2jVnnWQM-s_OQM/edit",
)
SHEET_COLUMN = int(os.getenv("SHEET_COLUMN", "1"))

# Scope explicitly restricted to read-only access
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# Only scan identifiers count on either side; anything else (headers, notes, stray files) is ignored.
SCAN_ID = re.compile(r"\d{8}_\d{4}")


def load_credentials() -> Credentials:
    """Decodes the base64 service account key held in GOOGLE_CREDS, so the key travels as an
    environment variable instead of a file that has to be mounted into the container."""
    creds_json = base64.b64decode(os.environ["GOOGLE_CREDS"]).decode()
    return Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)


def list_present_folders() -> set[str]:
    """Registers an SMB session and returns the scan-identifier directories directly under the
    audited share path, ignoring plain files and any other naming."""
    smbclient.register_session(NAS_HOST, username=NAS_USERNAME, password=NAS_PASSWORD)
    root = f"\\\\{NAS_HOST}\\{NAS_SHARE}"
    if NAS_PATH:
        root = root + "\\" + NAS_PATH.strip("/\\").replace("/", "\\")
    return {e.name for e in smbclient.scandir(root) if e.is_dir() and SCAN_ID.fullmatch(e.name)}


def list_expected_folders() -> list[str]:
    """Reads the configured column of the sheet's first tab and returns its scan identifiers in order,
    without duplicates."""
    worksheet = gspread.authorize(load_credentials()).open_by_url(SHEET_URL).sheet1
    values = (v.strip() for v in worksheet.col_values(SHEET_COLUMN))
    return list(dict.fromkeys(v for v in values if SCAN_ID.fullmatch(v)))


def audit() -> dict:
    """Compares the sheet column against the share listing and reports which expected folders are absent."""
    expected = list_expected_folders()
    present = list_present_folders()
    missing = [name for name in expected if name not in present]
    return {
        "expected_count": len(expected),
        "present_count": len(present),
        "missing_count": len(missing),
        # "missing": missing,
    }

    
