from logger_config import get_logger
import os
import sql
from db.query import AsyncQuerier, InsertScanParams
from datetime import datetime, timezone
import pandas as pd
import re
import smbclient
from smbclient import SMBDirEntry
from smbprotocol.exceptions import SMBException
from pathlib import Path
from dotenv import load_dotenv
import asyncio
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
SMB_WORKERS = int(os.getenv("SMB_WORKERS", 20))
MAX_SMB_QUERY_ATTEMPTS = int(os.getenv("SMB_QUERY_ATTEMPTS", 50))
MAX_SMB_SLEEP = int(os.getenv("SMB_MAX_SLEEP", 20))

TIMESTAMP_RE = re.compile(r"\d{8}_\d{4}") 
SUB_ID_RE = re.compile(r"sub-(S\d{6})")  
SES_TIMESTAMP_RE = re.compile(r"ses-(\d{12})")  
NAS_PATH = Path(f"//{NAS_HOST}/")

# smbclient is blocking so its calls run in threads, this caps how many
# subject directories are scraped at once (replaces the old thread pool size)
_subject_semaphore = asyncio.Semaphore(SMB_WORKERS)


def _first_group_or_none(regex: re.Pattern[str], string: str) -> str | None:
    try:
        return regex.search(string).group(1)
    except:
        return None
    

def log_exceptions(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.exception(f"--- Exception in task '{func.__name__}' with args: {args} {kwargs}---")
            raise e
    return wrapper

async def _exponential_decay(
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
            return await func(*args, **kwargs)
        except exc as e:
            last_exc = e
            sleep_time = min(max_sleep, 2**i)
            logger.exception(
                f"exception raised while trying to run {func} "
                f"attempt number: {i} "
                f"args: {args} {kwargs} "
                f"sleeping for {sleep_time} seconds"
            )
            await asyncio.sleep(sleep_time + 3*random.random())
    else:
        raise last_exc

async def default_trying_smb(func: Callable, exc: Exception, *args, **kwargs):
    return await _exponential_decay(
        func, exc, MAX_SMB_QUERY_ATTEMPTS, MAX_SMB_SLEEP, *args, **kwargs
    )

def _get_smb_dirs(path: str, modified_after: datetime | None, search: str= "*") -> list[SMBDirEntry]:
    return_list = []
    total_dirs = 0
    total_skipped_not_dir = 0
    total_skipped_not_modified = 0
    for ses in smbclient.scandir(path, search):
        total_dirs+=1
        last_time_write = ses._dir_info.last_write_time
        if not ses.is_dir():
            logger.debug(f"skipping {ses.name} not dir")
            total_skipped_not_dir+=1
            continue
        elif modified_after and last_time_write < modified_after.replace(tzinfo=timezone.utc):
            logger.debug(
                f"skipping {ses.name}, "
                f"last_write_time: {ses._dir_info.last_write_time} "
                f"which is before: {modified_after}"
            )
            total_skipped_not_modified+=1
            continue
        return_list.append(ses)
    logger.info(
            f"total dirs in {path}: {total_dirs}, "
            f"amount of skipped files (not directories): {total_skipped_not_dir}, "
            f"amount of directories not modified after {modified_after}: {total_skipped_not_modified}"
        )

    return return_list

async def _get_smb_dirs_in_thread(path: str, modified_after: datetime | None, search: str) -> list[SMBDirEntry]:
    return await asyncio.to_thread(_get_smb_dirs, path, modified_after, search)

async def get_smb_dirs(path: str, modified_after: datetime | None, search: str = "*") -> list[SMBDirEntry]:
    return await default_trying_smb(_get_smb_dirs_in_thread, SMBException ,path, modified_after, search)

def _format_datetime_raw(date_time_str: str) -> datetime:
    return datetime.strptime(date_time_str,"%Y%m%d_%H%M").strftime("%Y-%m-%d %H:%M:%S")

def _format_datetime_ses(date_time_str: str) -> datetime:
    return datetime.strptime(date_time_str,"%Y%m%d%H%M").strftime("%Y-%m-%d %H:%M:%S")

async def get_ses_dirs(path: str, modified_after: datetime) -> list[SMBDirEntry]:
    return await get_smb_dirs(path, modified_after, "ses-*")

async def get_sub_dirs(path: str, modified_after: datetime) -> list[SMBDirEntry]:
    return await get_smb_dirs(path, modified_after, "sub-*")

@log_exceptions
async def _insert_sub_ses_file_structure(
        subject_dir: SMBDirEntry, 
        modified_after: datetime | None,
        name: sql.Source
    ) -> str:
    async with _subject_semaphore:
        logger.debug(f"enumerating {subject_dir.name}")
        sessions = await get_ses_dirs(subject_dir.path, modified_after)
        async with sql.connection() as conn:
            try:
                querier = AsyncQuerier(conn)
                for ses in sessions:
                    await querier.insert_scan(
                        InsertScanParams(
                            scan_time=_format_datetime_ses(ses.name[4:16]),
                            source=name,
                            status=sql.Status.UNKOWN, # FIXME need to check how to find what status the session as actually at
                            full_path=ses.path,
                            scanner_subject_id=subject_dir.name[4:16],
                        )
                    )
            except Exception as e:
                for ses in sessions:
                    logger.warning(f'could not insert record in to db, session: {ses.name} ,path: {subject_dir.path}')
                raise e



@log_exceptions
async def insert_sub_ses(
        *,
        path: str,
        name: sql.Source | str,
        modified_after: datetime
    ) -> None:
    logger.info(f"scraping {name} at getting {path}, subject directories after {modified_after}")
    async with sql.connection() as conn:
        job_id = await AsyncQuerier(conn).insert_job_entry(source=name)

    try:
        subject_dirs = await get_sub_dirs(path, modified_after)
    except Exception as e:
        # otherwise the job entry would stay ONGOING forever
        async with sql.connection() as conn:
            await sql.update_job_based_on_exceptions(conn, job_id, [e])
        raise e
    logger.debug(f"got {len(subject_dirs)} subject directories")
    results = await asyncio.gather(
        *(
            _insert_sub_ses_file_structure(
                subject_dir=directory,
                modified_after=modified_after,
                name=name,
            )
            for directory in subject_dirs
        ),
        return_exceptions=True,
    )
    all_exceptions = [result for result in results if isinstance(result, BaseException)]

    async with sql.connection() as conn:
        await sql.update_job_based_on_exceptions(conn, job_id, all_exceptions)
    logger.info(f"finished scraping {name}")



async def list_google_sheets() -> list[InsertScanParams]:
    logger.debug("getting excel folder names")
    col_index = 0
    df = await asyncio.to_thread(pd.read_csv, SHEET_URL, usecols=[col_index], header=None, dtype=str)
    series = df.iloc[:, 0]
    return [
        InsertScanParams(
            scan_time=_format_datetime_raw(scan_id),
            source=sql.Source.GOOGLE_SHEETS,
            status=sql.Status.UNKOWN,
            full_path=None,
            scanner_subject_id=None,
        ) 
        for scan_id in series.dropna() if TIMESTAMP_RE.fullmatch(scan_id)
    ]

async def insert_raw_data(modified_after: datetime | None) -> list[InsertScanParams]:
    logger.info(f"getting raw data after {modified_after}")
    all_raw_scans = [
        InsertScanParams(
            scan_time=_format_datetime_raw(scan_dir.name),
            source=sql.Source.RAW_DATA,
            status=sql.Status.UNKOWN,
            full_path=scan_dir.path,
            scanner_subject_id=None,
        ) 
        for scan_dir in await get_smb_dirs(NAS_PATH/"Raw_Data", modified_after) 
        if TIMESTAMP_RE.fullmatch(scan_dir.name)
    ]
    logger.info(f"raw data entries: {len(all_raw_scans)}")
    return all_raw_scans

async def list_freesurfer(modified_after: datetime | None) -> list[InsertScanParams]:
    logger.debug("getting freesurfer data")
    all_entries = []
    for directory in await get_smb_dirs(NAS_PATH/"Processed_Data"/"derivatives"/"freesurfer", modified_after=modified_after):
        if directory.is_dir():
            name = directory.name
            sub_id = _first_group_or_none(SUB_ID_RE, name)
            if not sub_id:
                continue
            ses_time_raw = _first_group_or_none(SES_TIMESTAMP_RE, name)
            if not ses_time_raw:
                logger.debug(f"skipping freesurfer dir without a session timestamp: {name}")
                continue
            if '.long.' in name:
                continue
            all_entries.append(
                InsertScanParams(
                    scan_time=_format_datetime_ses(ses_time_raw), 
                    source=sql.Source.FREESURFER, 
                    status=sql.Status.UNKOWN, 
                    full_path=directory.path,
                    scanner_subject_id=sub_id,
                )
            )
    return all_entries




async def init_and_commit():
    logger.info("initializing DB")
    async with sql.connection() as conn:
        await sql.create_tables(conn)
        


async def _insert_scans_as_job(conn, source: sql.Source, list_scans) -> None:
    "list the scans and insert them one by one, recording the outcome as a job entry"
    job_id = await AsyncQuerier(conn).insert_job_entry(source=source)
    try:
        # savepoint so a failed insert doesn't abort the rest of the transaction
        async with conn.begin_nested():
            querier = AsyncQuerier(conn)
            for scan_data in await list_scans():
                await querier.insert_scan(scan_data)
        await sql.update_job_based_on_exceptions(conn, job_id, [])
    except Exception as e:
        await sql.update_job_based_on_exceptions(conn, job_id, [e])


async def _update_db(last_modified_dict: dict[str, datetime]) -> list[BaseException]:
    "function to run periodically to save details in postgres to be read by grafana"

    logger.info("starting smb scraping")

    worker_exceptions = []
    results = await asyncio.gather(
        insert_sub_ses(
            path=NAS_PATH/"Processed_Data"/"derivatives"/"qsirecon",
            name=sql.Source.QSIRECON.value,
            modified_after=last_modified_dict[sql.Source.QSIRECON.value]
        ),
        insert_sub_ses(
            path=NAS_PATH/"Processed_Data"/"derivatives"/"qsiprep",
            name=sql.Source.QSIPREP.value,
            modified_after=last_modified_dict[sql.Source.QSIPREP.value]
        ),
        insert_sub_ses(
            path=NAS_PATH/"Processed_Data"/"derivatives"/"tabular",
            name=sql.Source.TABULAR.value,
            modified_after=last_modified_dict[sql.Source.TABULAR.value]
        ),
        return_exceptions=True,
    )

    # trying to prevent smb threading errors
    worker_exceptions += results

    results = await asyncio.gather(
        insert_sub_ses(
            path=NAS_PATH/"Bids",
            name=sql.Source.BIDS.value,
            modified_after=last_modified_dict[sql.Source.BIDS.value]
        ),
        insert_sub_ses(
            path=NAS_PATH/"Processed_Data"/"derivatives"/"CAT",
            name=sql.Source.CAT.value,
            modified_after=last_modified_dict[sql.Source.CAT.value]
        ),
        insert_sub_ses(
            path=NAS_PATH/"Processed_Data"/"derivatives"/"tabular_cat12",
            name=sql.Source.TABULAR_CAT12.value,
            modified_after=last_modified_dict[sql.Source.TABULAR_CAT12.value]
        ),
        return_exceptions=True,
    )

    worker_exceptions += results

    async with sql.connection() as conn:
        await _insert_scans_as_job(
            conn,
            sql.Source.RAW_DATA,
            lambda: insert_raw_data(last_modified_dict[sql.Source.RAW_DATA.value])
        )
        await _insert_scans_as_job(conn, sql.Source.GOOGLE_SHEETS, list_google_sheets)
        await _insert_scans_as_job(
            conn,
            sql.Source.FREESURFER,
            lambda: list_freesurfer(last_modified_dict[sql.Source.FREESURFER.value])
        )

    logger.info("finished update")
    return [e for e in worker_exceptions if isinstance(e, BaseException)]

async def update_db() -> None:
    logger.info("initiating DB update")
    async with sql.connection() as conn:
        logger.info("creating tables")
        await sql.create_tables(conn)
        orchestrator_id = await AsyncQuerier(conn).insert_job_entry(source=sql.Source.ORCHESTRATORE)
        scrape_from = await sql.get_last_completed_scan_with_buffer(conn)

    
    try:
        logger.info("connecting to smb")
        await asyncio.to_thread(
            smbclient.register_session, NAS_HOST, username=NAS_USERNAME, password=NAS_PASSWORD
        )
        all_task_exceptions = await _update_db(scrape_from)
        async with sql.connection() as conn:
            await sql.update_job_based_on_exceptions(conn,orchestrator_id,all_task_exceptions)

    except Exception as e:
        async with sql.connection() as conn:
            await AsyncQuerier(conn).update_job_entry(
                status=sql.JobStatus.ERROR,
                error_msg=str(traceback.format_exc()),
                stop_time=datetime.now(tz=timezone.utc),
                id=orchestrator_id,
            )
        raise e
