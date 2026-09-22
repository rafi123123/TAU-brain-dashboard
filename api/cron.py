import sqlite3
from logger_config import get_logger
import os
import sql
from datetime import datetime, timezone
import pandas as pd
import re
import smbclient
from smbclient import SMBDirEntry
from smbprotocol.exceptions import SMBException
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import time
from typing import Callable
import random
import traceback
load_dotenv()
logger = get_logger(__name__)

NAS_HOST = os.getenv("NAS_HOST")
NAS_USERNAME = os.environ["NAS_USERNAME"]
NAS_PASSWORD = os.environ["NAS_PASSWORD"]
SHEET_URL = os.getenv(
    "SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1YvhfZ8k3xafsN1ijtOl1uESG4w48-2jVnnWQM-s_OQM/export?format=csv",
)
DB_LOC = os.getenv("DB_LOCATION", "/data/scan-data/presentation_data.db")
SMB_WORKERS = int(os.getenv("SMB_WORKERS", 50))
MAX_SMB_QUERY_ATTEMPTS = int(os.getenv("SMB_QUERY_ATTEMPTS", 50))
MAX_SMB_SLEEP = int(os.getenv("SMB_MAX_SLEEP", 20))

TIMESTAMP_RE = re.compile(r"\d{8}_\d{4}") 
SUB_ID_RE = re.compile(r"sub-(S\d{6})")  
SES_TIMESTAMP_RE = re.compile(r"ses-(\d{12})")  
NAS_PATH = Path(f"//{NAS_HOST}/")


def _first_group_or_none(regex: re.Pattern[str], string: str) -> str | None:
    try:
        return regex.search(string).group(1)
    except:
        return None
    

def log_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception(f"--- Exception in thread task '{func.__name__}' with args: {args} {kwargs}---")
            raise e
    return wrapper

def _exponential_decay(
        func: Callable, 
        exc: Exception,
        max_attempts: int, 
        max_sleep: int, 
        *args, 
        **kwargs
    ):
    last_exc = None
    for i in range(max_attempts):
        try:
            return func(*args, **kwargs)
        except exc as e:
            last_exc = e
            sleep_time = min(max_sleep, 2**i)
            logger.warning(
                f"exception raised while trying to run {func} "
                f"attempt number: {i} "
                f"args: {args} {kwargs} "
                f"sleeping for {sleep_time} seconds"
            )
            time.sleep(sleep_time + 3*random.random())
    else:
        raise last_exc

def default_trying_smb(func: Callable, exc: Exception, *args, **kwargs):
    return _exponential_decay(
        func, exc, MAX_SMB_QUERY_ATTEMPTS, MAX_SMB_SLEEP, *args, **kwargs
    )

def _get_smb_dirs(path: str, modified_after: datetime | None, search: str= "*") -> list[SMBDirEntry]:
    return_list = []
    for ses in smbclient.scandir(path, search):
        last_time_write = ses._dir_info.last_write_time
        if not ses.is_dir():
            logger.info(f"skipping {ses.name} not dir")
            continue
        elif modified_after and last_time_write < modified_after.replace(tzinfo=timezone.utc):
            logger.info(
                f"skipping {ses.name}, "
                f"last_write_time: {ses._dir_info.last_write_time} "
                f"which is before: {modified_after}"
            )
            continue
        return_list.append(ses)
    return return_list

def get_smb_dirs(path: str, modified_after: datetime | None, search: str = "*") -> list[SMBDirEntry]:
    return default_trying_smb(_get_smb_dirs, SMBException ,path, modified_after, search)

def _format_datetime_raw(date_time_str: str) -> datetime:
    return datetime.strptime(date_time_str,"%Y%m%d_%H%M").strftime("%Y-%m-%d %H:%M:%S")

def _format_datetime_ses(date_time_str: str) -> datetime:
    return datetime.strptime(date_time_str,"%Y%m%d%H%M").strftime("%Y-%m-%d %H:%M:%S")

@log_exceptions
def _insert_sub_ses_file_structure(
        subject_dir: SMBDirEntry, 
        modified_after: datetime | None,
        name: sql.Source
    ) -> str:
    logger.info(f"enumerating {subject_dir.name}")
    sessions = get_smb_dirs(subject_dir.path, modified_after, "ses-*")
    with sqlite3.connect(DB_LOC, timeout=20) as conn:
        try:
            cursor = conn.cursor()
            sql.insert_scan_many(
                cursor, 
                [
                    (
                        _format_datetime_ses(ses.name[4:16]),
                        name,
                        None, # FIXME need to check how to find what status the session as actually at
                        ses.path,
                        subject_dir.name[4:16]
                    ) for ses in sessions 
                ]
            )
        except Exception as e:
            for ses in sessions:
                logger.warning(f'could not insert record in to db, session: {ses.name} ,path: {subject_dir.path}')
            raise e



@log_exceptions
def insert_sub_ses(
        *,
        executer: ThreadPoolExecutor, 
        path: str,
        name: sql.Source | str,
        modified_after: datetime
    ) -> None:
    logger.info(f"getting {path}, subject directories")
    with sqlite3.connect(DB_LOC, timeout=20) as conn:
        job_id = sql.insert_job_entry(conn.cursor(), name)

    subject_dirs = get_smb_dirs(path, modified_after, "sub-*")
    logger.info(f"got {len(subject_dirs)} subject directories")
    all_futures = []
    for directory in subject_dirs:
        all_futures.append(executer.submit(
            _insert_sub_ses_file_structure,
            subject_dir=directory,
            modified_after=modified_after,
            name=name, 
        ))

    all_exceptions = []
    for future in all_futures:
        exc = future.exception()
        if exc:
            all_exceptions.append(exc)

    with sqlite3.connect(DB_LOC, timeout=20) as conn:
        cursor = conn.cursor()
        sql.update_job_based_on_exceptions(cursor, job_id, all_exceptions)



def list_google_sheets() -> list[tuple]:
    logger.info("getting excel folder names")
    col_index = 0
    series = pd.read_csv(SHEET_URL, usecols=[col_index], header=None, dtype=str).iloc[:, 0]
    return [
        (
            _format_datetime_raw(scan_id),
            sql.Source.GOOGLE_SHEETS.value,
           None,
           None,
           None
        ) 
        for scan_id in series.dropna() if TIMESTAMP_RE.fullmatch(scan_id)
    ]

def insert_raw_data(modified_after: datetime | None)-> list:
    logger.info("getting raw data")
    all_raw_scans = [
        (
           _format_datetime_raw(scan_dir.name),
           sql.Source.RAW_DATA.value,
           None,
           scan_dir.path,
           None
        ) 
        for scan_dir in get_smb_dirs(NAS_PATH/"Raw_Data", modified_after) 
        if TIMESTAMP_RE.fullmatch(scan_dir.name)
    ]
    logger.info(f"raw data entries: {len(all_raw_scans)}")
    return all_raw_scans

def list_freesurfer(modified_after: datetime | None):
    logger.info("getting freesurfer data")
    all_entries = []
    # for directory in smbclient.scandir(NAS_PATH/"Processed_Data"/"derivatives"/"freesurfer"):
    for directory in get_smb_dirs(NAS_PATH/"Processed_Data"/"derivatives"/"freesurfer", modified_after=modified_after):
        if directory.is_dir():
            name = directory.name
            sub_id = _first_group_or_none(SUB_ID_RE, name)
            if not sub_id:
                continue
            ses_time_raw = _first_group_or_none(SES_TIMESTAMP_RE, name)
            if not ses_time_raw:
                ses_time = None
            else:
                ses_time = _format_datetime_ses(ses_time_raw)
            long = '.long.' in name
            if long and ses_time:
                status = sql.Status.FREESURFER_SUBJECT_SESSION_LONG_DIR.value
            elif ses_time:
                status = sql.Status.FREESURFER_SUBJECT_SESSION_DIR.value
            else:
                status = sql.Status.FREESURFER_SUBJECT_ONLY_DIR.value
            all_entries.append(
                (
                    ses_time, 
                    sql.Source.FREESURFER.value, 
                    status, 
                    directory.path,
                    sub_id
                )
            )
    return all_entries




def init_and_commit():
    logger.info("initializing DB")
    with sqlite3.connect(DB_LOC) as conn:
        cursor = conn.cursor()
        sql.create_scan_history_table(cursor)
        


def _update_db(last_modified_dict: dict[str, datetime]) -> list[Exception]:
    "function to run periodically to save details in sqlite file to be read by grafana"

    logger.info("starting smb scraping")

    worker_exceptions = []
    with ThreadPoolExecutor(max_workers=SMB_WORKERS) as executer:
        all_futures = []
        all_futures.append(
            executer.submit(
                insert_sub_ses, 
                executer=executer, 
                path=NAS_PATH/"Processed_Data"/"derivatives"/"qsirecon",
                name=sql.Source.QSIRECON.value,
                modified_after=last_modified_dict[sql.Source.QSIRECON.value]
            )
        )
        all_futures.append(
            executer.submit(
                insert_sub_ses, 
                executer=executer, 
                path=NAS_PATH/"Processed_Data"/"derivatives"/"qsiprep",
                name=sql.Source.QSIPREP.value,
                modified_after=last_modified_dict[sql.Source.QSIPREP.value]
            )
        )
        all_futures.append(
            executer.submit(
                insert_sub_ses, 
                executer=executer, 
                path=NAS_PATH/"Processed_Data"/"derivatives"/"tabular",
                name=sql.Source.TABULAR.value,
                modified_after=last_modified_dict[sql.Source.TABULAR.value]
            )
        )

        # trying to prevent sqlite db locked error and and smb threading errors
        worker_exceptions += [future.exception() for future in all_futures]
        all_futures = []

        all_futures.append(
            executer.submit(
                insert_sub_ses, 
                executer=executer, 
                path=NAS_PATH/"Bids",
                name=sql.Source.BIDS.value,
                modified_after=last_modified_dict[sql.Source.BIDS.value]
            )
        )
        all_futures.append(
            executer.submit(
                insert_sub_ses, 
                executer=executer, 
                path=NAS_PATH/"Processed_Data"/"derivatives"/"CAT",
                name=sql.Source.CAT.value,
                modified_after=last_modified_dict[sql.Source.CAT.value]
            )
        )
        all_futures.append(
            executer.submit(
                insert_sub_ses, 
                executer=executer, 
                path=NAS_PATH/"Processed_Data"/"derivatives"/"tabular_cat12",
                name=sql.Source.TABULAR_CAT12.value,
                modified_after=last_modified_dict[sql.Source.TABULAR_CAT12.value]
            )
        )

        worker_exceptions += [future.exception() for future in all_futures]

        with sqlite3.connect(DB_LOC, timeout=20) as conn:
            cursor = conn.cursor()
            sql.insert_scan_many(
                cursor, 
                insert_raw_data(last_modified_dict[sql.Source.RAW_DATA.value])
            )
            
            sql.insert_scan_many(
                cursor, 
                list_google_sheets()
            )

            sql.insert_scan_many(
                cursor,
                list_freesurfer(last_modified_dict[sql.Source.FREESURFER.value])
            )
            
    logger.info("finished update")
    return worker_exceptions

def update_db() -> None:
    logger.info("initiating DB update")

    logger.info("connecting to smb")
    smbclient.register_session(NAS_HOST, username=NAS_USERNAME, password=NAS_PASSWORD)

    with sqlite3.connect(DB_LOC, timeout=20) as conn:
        cursor = conn.cursor()

        logger.info("creating sqlite cron jobs table")
        sql.create_smb_scraping_jobs_table(cursor)


        logger.info("creating sqlite scan history table")
        sql.create_scan_history_table(cursor)
        orchestrator_id = sql.insert_job_entry(cursor, sql.Source.ORCHESTRATORE)
        scrape_from = sql.get_last_completed_scan_with_buffer(cursor)

    
    try:
        all_thread_exceptions = _update_db(scrape_from)
        all_thread_exceptions = [e for e in all_thread_exceptions if e]
        with sqlite3.connect(DB_LOC, timeout=20) as conn:
            cursor = conn.cursor()
            sql.update_job_based_on_exceptions(cursor,orchestrator_id,all_thread_exceptions)

    except Exception as e:
        with sqlite3.connect(DB_LOC, timeout=20) as conn:
            cursor = conn.cursor()
            sql.upadate_job_entry(
                cursor, 
                orchestrator_id, 
                sql.JobStatus.ERROR, 
                str(traceback.format_exc()), 
                datetime.now(tz=timezone.utc)
            )
        raise e


if __name__ == "__main__":
    update_db()
    # with sqlite3.connect(DB_LOC, timeout=20) as conn:
    #     cursor = conn.cursor()
    #     sql.create_smb_scraping_jobs_table(cursor)
    #     bids_id = sql.insert_job_entry(cursor, sql.Source.BIDS)
    #     freesurfer_id = sql.insert_job_entry(cursor, sql.Source.FREESURFER)
    #     orchestrator_id = sql.insert_job_entry(cursor, sql.Source.ORCHESTRATORE)

    #     sql.upadate_job_entry(cursor, bids_id, sql.JobStatus.COMPLETED)
    #     sql.upadate_job_entry(cursor, orchestrator_id, sql.JobStatus.ERROR)
    #     sql.upadate_job_entry(cursor, freesurfer_id, sql.JobStatus.COMPLETED)

    #     print(sql.get_last_completed_scan_with_buffer(cursor))

