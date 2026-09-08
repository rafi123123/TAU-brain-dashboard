from logger_config import setup_logging
import pandas as pd
import os
import re
import smbclient
from dotenv import load_dotenv

load_dotenv()

logger = setup_logging(__name__)

NAS_HOST = os.getenv("NAS_HOST")
NAS_USERNAME = os.getenv("NAS_USERNAME")
NAS_PASSWORD = os.getenv("NAS_PASSWORD")
NAS_SHARE = os.getenv("NAS_SHARE")
NAS_PATH = os.getenv("NAS_PATH", "")

SHEET_URL = os.getenv(
    "SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1YvhfZ8k3xafsN1ijtOl1uESG4w48-2jVnnWQM-s_OQM/export?format=csv",
)
# Only scan identifiers count on either side; anything else (headers, notes, stray files) is ignored.
SCAN_ID = re.compile(r"\d{8}_\d{4}")


def list_present_folders() -> set[str]:
    """Registers an SMB session and returns the scan-identifier directories directly under the
    audited share path, ignoring plain files and any other naming."""
    logger.info("getting smb folders")
    smbclient.register_session(NAS_HOST, username=NAS_USERNAME, password=NAS_PASSWORD)
    root = f"\\\\{NAS_HOST}\\{NAS_SHARE}"
    if NAS_PATH:
        root = root + "\\" + NAS_PATH.strip("/\\").replace("/", "\\")
    return {e.name for e in smbclient.scandir(root) if e.is_dir() and SCAN_ID.fullmatch(e.name)}


def list_expected_folders() -> list[str]:
    """Reads the configured column of the sheet's first tab and returns its scan identifiers in order,
    without duplicates."""
    logger.info("getting excel folder names")
    col_index = 0
    series = pd.read_csv(SHEET_URL, usecols=[col_index], header=None, dtype=str).iloc[:, 0]

    # Process values: strip whitespace, filter by regex, and preserve order while deduplicating
    values = (str(v).strip() for v in series.dropna())
    ret_val = list(dict.fromkeys(v for v in values if SCAN_ID.fullmatch(v)))
    print(ret_val[:4])
    return ret_val


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

    
