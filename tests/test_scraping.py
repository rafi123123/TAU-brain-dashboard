"""
Tests for the scraping jobs (api/cron.py) and the postgres layer (api/sql.py).

The SMB share and the google sheet are always faked, nothing here talks to the
real NAS. Postgres is real: set TEST_DATABASE_URL to use an existing (throwaway!)
database, otherwise a temporary postgres:17 container is started with docker.
Tables are dropped and recreated before every test.

run from the project root:
    pip install -r requirements-dev.txt
    pytest tests/
"""
import asyncio
import fnmatch
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool
from pydantic import ValidationError
from smbprotocol.exceptions import SMBException

# must be set before importing cron / sql: cron reads the NAS credentials at import
# and load_dotenv() never overrides variables that are already set, so the real
# values in api/.env can't leak into the tests
os.environ["NAS_HOST"] = "fake-nas"
os.environ["NAS_USERNAME"] = "fake-user"
os.environ["NAS_PASSWORD"] = "fake-password"
os.environ["SHEET_URL"] = "http://sheet.invalid/export?format=csv"
os.environ["DATABASE_URL"] = "postgresql://unused@localhost:1/unused"

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "api"))
import cron  # noqa: E402
import sql  # noqa: E402

pytestmark = pytest.mark.anyio

NAS_ROOT = "//fake-nas"
OLD_MTIME = datetime(2026, 1, 1, tzinfo=timezone.utc)

SUB_SES_SOURCES = {
    sql.Source.QSIRECON: "Processed_Data/derivatives/qsirecon",
    sql.Source.QSIPREP: "Processed_Data/derivatives/qsiprep",
    sql.Source.TABULAR: "Processed_Data/derivatives/tabular",
    sql.Source.BIDS: "Bids",
    sql.Source.CAT: "Processed_Data/derivatives/CAT",
    sql.Source.TABULAR_CAT12: "Processed_Data/derivatives/tabular_cat12",
}
RAW_DATA_PATH = f"{NAS_ROOT}/Raw_Data"
FREESURFER_PATH = f"{NAS_ROOT}/Processed_Data/derivatives/freesurfer"


def nas_path(relative: str) -> str:
    return f"{NAS_ROOT}/{relative}"


# --------------------------------------------------------------------------- fakes

class FakeEntry:
    """Mimics the parts of smbclient.SMBDirEntry that cron uses."""

    def __init__(self, parent: str, name: str, mtime: datetime, is_dir: bool):
        self.name = name
        self.path = f"{parent}/{name}"
        self._is_dir = is_dir
        self._dir_info = SimpleNamespace(last_write_time=mtime)

    def is_dir(self) -> bool:
        return self._is_dir


class FakeNAS:
    """In memory directory tree standing in for smbclient.scandir."""

    def __init__(self, delay: float = 0.0):
        self.tree: dict[str, list[FakeEntry]] = {}
        self.failures: dict[tuple[str, str], list[BaseException]] = {}
        self.calls: list[tuple[str, str]] = []
        self.delay = delay
        self._lock = threading.Lock()
        self._concurrent: dict[str, int] = {}
        self.max_concurrent: dict[str, int] = {}

    def add(self, parent: str, name: str, *, mtime: datetime = OLD_MTIME, is_dir: bool = True) -> str:
        entry = FakeEntry(parent, name, mtime, is_dir)
        self.tree.setdefault(parent, []).append(entry)
        self.tree.setdefault(entry.path, [])
        return entry.path

    def fail(self, path: str, search: str, *errors: BaseException) -> None:
        """Raise the given errors on the next scandir calls, the last one keeps being raised."""
        self.failures[(path, search)] = list(errors)

    def scandir(self, path, search="*"):
        path = str(path)
        with self._lock:
            self.calls.append((path, search))
            self._concurrent[search] = self._concurrent.get(search, 0) + 1
            self.max_concurrent[search] = max(self.max_concurrent.get(search, 0), self._concurrent[search])
        try:
            if self.delay:
                time.sleep(self.delay)
            errors = self.failures.get((path, search))
            if errors:
                raise errors.pop(0) if len(errors) > 1 else errors[0]
            return [e for e in self.tree.get(path, []) if fnmatch.fnmatch(e.name, search)]
        finally:
            with self._lock:
                self._concurrent[search] -= 1


def build_standard_nas(nas: FakeNAS) -> None:
    """Two subjects with two sessions each for every sub/ses source plus raw data and freesurfer."""
    for relative in SUB_SES_SOURCES.values():
        root = nas_path(relative)
        for sub_index, sub in enumerate(["sub-S000001", "sub-S000002"]):
            sub_path = nas.add(root, sub)
            nas.add(sub_path, f"ses-20260101{sub_index}100")
            nas.add(sub_path, f"ses-20260102{sub_index}100")
            nas.add(sub_path, "notes.txt", is_dir=False)
        nas.add(root, "dataset_description.json", is_dir=False)

    nas.add(RAW_DATA_PATH, "20260105_0930")
    nas.add(RAW_DATA_PATH, "20260106_1015")
    nas.add(RAW_DATA_PATH, "not_a_scan")
    nas.add(RAW_DATA_PATH, "20260107_1100", is_dir=False)

    nas.add(FREESURFER_PATH, "sub-S000001")
    nas.add(FREESURFER_PATH, "sub-S000001_ses-202601011200")
    nas.add(FREESURFER_PATH, "sub-S000001_ses-202601011200.long.sub-S000001")
    nas.add(FREESURFER_PATH, "fsaverage")


SHEET_ROWS = ["scan id", "20260110_1000", "20260111_1100", None, "bad value"]


# --------------------------------------------------------------------------- fixtures

@pytest.fixture
def anyio_backend():
    return "asyncio"


def _wait_for_postgres(url: str, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            with psycopg.connect(url, connect_timeout=2) as conn:
                conn.execute("SELECT 1")
            return
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)


@pytest.fixture(scope="session")
def postgres_url():
    url = os.getenv("TEST_DATABASE_URL")
    if url:
        yield url
        return

    try:
        container = subprocess.run(
            ["docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=test",
             "-p", "127.0.0.1::5432", "postgres:17"],
            check=True, capture_output=True, text=True,
        ).stdout.strip()
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        pytest.skip(f"docker not available and TEST_DATABASE_URL not set: {e}")

    try:
        # the image's temporary initdb server only listens on a unix socket,
        # so a tcp connection succeeding means the real server is up
        port = subprocess.run(
            ["docker", "port", container, "5432"], check=True, capture_output=True, text=True
        ).stdout.split(":")[-1].strip()
        url = f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
        _wait_for_postgres(url)
        yield url
    finally:
        subprocess.run(["docker", "stop", container], capture_output=True)


@pytest.fixture(autouse=True)
def isolate_cron(monkeypatch):
    """Guard against real network access and make retries instant."""

    def no_real_smb(*args, **kwargs):
        raise AssertionError("tests must not touch the real SMB share, use the nas fixture")

    def no_real_sheet(*args, **kwargs):
        raise AssertionError("tests must not fetch the real google sheet, use the sheet fixture")

    monkeypatch.setattr(cron.smbclient, "scandir", no_real_smb)
    monkeypatch.setattr(cron.smbclient, "register_session", no_real_smb)
    monkeypatch.setattr(cron.pd, "read_csv", no_real_sheet)
    monkeypatch.setattr(cron, "NAS_PATH", Path(f"{NAS_ROOT}/"))
    monkeypatch.setattr(cron, "MAX_SMB_SLEEP", 0)
    monkeypatch.setattr(cron, "random", SimpleNamespace(random=lambda: 0.0))
    # a fresh semaphore per test, asyncio primitives get bound to the loop that first waits on them
    monkeypatch.setattr(cron, "_subject_semaphore", asyncio.Semaphore(cron.SMB_WORKERS))


@pytest.fixture
def nas(monkeypatch):
    fake = FakeNAS()
    monkeypatch.setattr(cron.smbclient, "scandir", fake.scandir)
    monkeypatch.setattr(cron.smbclient, "register_session", lambda *args, **kwargs: None)
    return fake


@pytest.fixture
def sheet(monkeypatch):
    rows = list(SHEET_ROWS)
    monkeypatch.setattr(cron.pd, "read_csv", lambda *args, **kwargs: pd.DataFrame({0: rows}))
    return rows


@pytest.fixture
async def db(postgres_url, monkeypatch):
    """Point sql at the test database with empty tables, yields a helper to run queries."""
    pool = AsyncConnectionPool(postgres_url, min_size=1, max_size=cron.SMB_WORKERS + 5, open=False)
    await pool.open()
    monkeypatch.setattr(sql, "_pool", pool)
    async with pool.connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS scan_data_history, smb_scraping_jobs")
        cursor = conn.cursor()
        await sql.create_scan_history_table(cursor)
        await sql.create_smb_scraping_jobs_table(cursor)

    async def query(statement: str, params: tuple = ()) -> list[tuple]:
        async with pool.connection() as conn:
            cursor = await conn.execute(statement, params)
            return await cursor.fetchall()

    yield query
    await pool.close()


async def jobs_by_source(query) -> dict[str, list[tuple]]:
    """source -> [(status, stop_time, error_msg), ...] in insertion order."""
    result: dict[str, list[tuple]] = {}
    for source, status, stop_time, error_msg in await query(
        "SELECT source, status, stop_time, error_msg FROM smb_scraping_jobs ORDER BY id"
    ):
        result.setdefault(source, []).append((status, stop_time, error_msg))
    return result


async def rows_for(query, source: sql.Source) -> list[tuple]:
    return await query(
        "SELECT scan_time, status, full_path, scanner_subject_id FROM scan_data_history "
        "WHERE source = %s ORDER BY scan_time, status",
        (source.value,),
    )


# --------------------------------------------------------------------------- pure helpers

def test_format_datetime_raw():
    assert cron._format_datetime_raw("20260105_0930") == "2026-01-05 09:30:00"


def test_format_datetime_ses():
    assert cron._format_datetime_ses("202601051430") == "2026-01-05 14:30:00"


def test_format_datetime_rejects_garbage():
    with pytest.raises(ValueError):
        cron._format_datetime_raw("not_a_date")


@pytest.mark.parametrize(
    ("regex", "string", "expected"),
    [
        (cron.SUB_ID_RE, "sub-S000123_ses-202601011200", "S000123"),
        (cron.SES_TIMESTAMP_RE, "sub-S000123_ses-202601011200", "202601011200"),
        (cron.SUB_ID_RE, "fsaverage", None),
        (cron.SES_TIMESTAMP_RE, "sub-S000123", None),
    ],
)
def test_first_group_or_none(regex, string, expected):
    assert cron._first_group_or_none(regex, string) == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [("20260105_0930", True), ("20260105_093", False), ("x20260105_0930", False), ("scan id", False)],
)
def test_timestamp_regex(name, expected):
    assert bool(cron.TIMESTAMP_RE.fullmatch(name)) is expected


# --------------------------------------------------------------------------- ScanData validation

def test_scan_data_accepts_regular_row():
    data = sql.ScanData(
        scan_time="2026-01-05 09:30:00",
        source=sql.Source.RAW_DATA,
        status=sql.Status.UNKOWN,
        full_path="//nas/Raw_Data/20260105_0930",
        scanner_subject_id=None,
    )
    assert data.scan_time == datetime(2026, 1, 5, 9, 30)


def test_scan_data_allows_missing_time_for_freesurfer_subject():
    data = sql.ScanData(
        scan_time=None,
        source=sql.Source.FREESURFER,
        status=sql.Status.FREESURFER_SUBJECT_ONLY_DIR,
        full_path=None,
        scanner_subject_id="S000001",
    )
    assert data.scan_time is None


@pytest.mark.parametrize(
    ("source", "subject_id"),
    [(sql.Source.RAW_DATA, "S000001"), (sql.Source.FREESURFER, None)],
)
def test_scan_data_rejects_missing_time(source, subject_id):
    with pytest.raises(ValidationError):
        sql.ScanData(
            scan_time=None, source=source, status=sql.Status.UNKOWN,
            full_path=None, scanner_subject_id=subject_id,
        )


def test_scan_data_rejects_unknown_source():
    with pytest.raises(ValidationError):
        sql.ScanData(
            scan_time="2026-01-05 09:30:00", source="NOT_A_SOURCE",
            status=sql.Status.UNKOWN, full_path=None, scanner_subject_id=None,
        )


# --------------------------------------------------------------------------- retries

async def test_exponential_decay_retries_until_success():
    attempts = []

    async def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise SMBException("transient")
        return "ok"

    assert await cron._exponential_decay(flaky, SMBException, 5, 0) == "ok"
    assert len(attempts) == 3


async def test_exponential_decay_gives_up_after_max_attempts():
    attempts = []

    async def always_fails():
        attempts.append(1)
        raise SMBException("down")

    with pytest.raises(SMBException):
        await cron._exponential_decay(always_fails, SMBException, 4, 0)
    assert len(attempts) == 4


async def test_exponential_decay_does_not_retry_other_exceptions():
    attempts = []

    async def broken():
        attempts.append(1)
        raise RuntimeError("bug")

    with pytest.raises(RuntimeError):
        await cron._exponential_decay(broken, SMBException, 5, 0)
    assert len(attempts) == 1


# --------------------------------------------------------------------------- smb listing

def test_get_smb_dirs_skips_files_and_old_dirs(nas):
    nas.add("//fake-nas/x", "old", mtime=datetime(2026, 1, 1, tzinfo=timezone.utc))
    nas.add("//fake-nas/x", "new", mtime=datetime(2026, 3, 1, tzinfo=timezone.utc))
    nas.add("//fake-nas/x", "file", mtime=datetime(2026, 3, 1, tzinfo=timezone.utc), is_dir=False)

    result = cron._get_smb_dirs("//fake-nas/x", datetime(2026, 2, 1, tzinfo=timezone.utc))
    assert [e.name for e in result] == ["new"]


def test_get_smb_dirs_without_cutoff_returns_all_dirs(nas):
    nas.add("//fake-nas/x", "a")
    nas.add("//fake-nas/x", "b")
    nas.add("//fake-nas/x", "file", is_dir=False)

    assert sorted(e.name for e in cron._get_smb_dirs("//fake-nas/x", None)) == ["a", "b"]


async def test_get_smb_dirs_applies_search_pattern(nas):
    nas.add("//fake-nas/x", "sub-S000001")
    nas.add("//fake-nas/x", "ses-202601011200")

    assert [e.name for e in await cron.get_sub_dirs("//fake-nas/x", None)] == ["sub-S000001"]
    assert [e.name for e in await cron.get_ses_dirs("//fake-nas/x", None)] == ["ses-202601011200"]


def fail_then_succeed(monkeypatch, nas: FakeNAS, path: str, search: str, times: int) -> None:
    """Make scandir raise SMBException `times` times for path/search, then behave normally."""
    remaining = [SMBException("transient") for _ in range(times)]
    original = nas.scandir

    def scandir(scan_path, scan_search="*"):
        if str(scan_path) == path and scan_search == search and remaining:
            nas.calls.append((path, search))
            raise remaining.pop(0)
        return original(scan_path, scan_search)

    monkeypatch.setattr(cron.smbclient, "scandir", scandir)


async def test_get_smb_dirs_retries_smb_errors(nas, monkeypatch):
    nas.add("//fake-nas/x", "a")
    fail_then_succeed(monkeypatch, nas, "//fake-nas/x", "*", times=2)

    assert [e.name for e in await cron.get_smb_dirs("//fake-nas/x", None)] == ["a"]
    assert nas.calls.count(("//fake-nas/x", "*")) == 3


# --------------------------------------------------------------------------- listing functions

async def test_insert_raw_data_keeps_only_scan_folders(nas):
    build_standard_nas(nas)
    scans = await cron.insert_raw_data(None)

    assert [s.scan_time for s in scans] == [datetime(2026, 1, 5, 9, 30), datetime(2026, 1, 6, 10, 15)]
    assert {s.source for s in scans} == {sql.Source.RAW_DATA}
    assert scans[0].full_path == f"{RAW_DATA_PATH}/20260105_0930"


async def test_list_google_sheets_keeps_only_scan_ids(sheet):
    scans = await cron.list_google_sheets()

    assert [s.scan_time for s in scans] == [datetime(2026, 1, 10, 10, 0), datetime(2026, 1, 11, 11, 0)]
    assert {s.source for s in scans} == {sql.Source.GOOGLE_SHEETS}


async def test_list_freesurfer_classifies_directories(nas):
    build_standard_nas(nas)
    scans = await cron.list_freesurfer(None)

    by_status = {s.status: s for s in scans}
    assert set(by_status) == {
        sql.Status.FREESURFER_SUBJECT_ONLY_DIR,
        sql.Status.FREESURFER_SUBJECT_SESSION_DIR,
        sql.Status.FREESURFER_SUBJECT_SESSION_LONG_DIR,
    }
    assert by_status[sql.Status.FREESURFER_SUBJECT_ONLY_DIR].scan_time is None
    assert by_status[sql.Status.FREESURFER_SUBJECT_SESSION_DIR].scan_time == datetime(2026, 1, 1, 12, 0)
    assert {s.scanner_subject_id for s in scans} == {"S000001"}  # fsaverage is skipped


# --------------------------------------------------------------------------- sql layer

async def test_create_tables_is_idempotent(db):
    async with sql.connection() as conn:
        cursor = conn.cursor()
        await sql.create_scan_history_table(cursor)
        await sql.create_smb_scraping_jobs_table(cursor)


async def test_insert_job_entry_defaults(db):
    async with sql.connection() as conn:
        first = await sql.insert_job_entry(conn.cursor(), sql.Source.BIDS)
        second = await sql.insert_job_entry(conn.cursor(), sql.Source.CAT)

    assert second > first
    rows = await db("SELECT source, status, start_time, stop_time, error_msg FROM smb_scraping_jobs ORDER BY id")
    assert [(r[0], r[1]) for r in rows] == [("BIDS", "ONGOING"), ("CAT", "ONGOING")]
    assert all(r[2] is not None and r[3] is None and r[4] is None for r in rows)


async def test_update_successful_job(db):
    async with sql.connection() as conn:
        job_id = await sql.insert_job_entry(conn.cursor(), sql.Source.BIDS)
        await sql.update_successful_job(conn.cursor(), job_id)

    [(status, stop_time, error_msg)] = await db("SELECT status, stop_time, error_msg FROM smb_scraping_jobs")
    assert status == "COMPLETED"
    assert stop_time is not None
    assert error_msg is None


async def test_update_job_based_on_exceptions_without_errors_completes(db):
    async with sql.connection() as conn:
        job_id = await sql.insert_job_entry(conn.cursor(), sql.Source.BIDS)
        await sql.update_job_based_on_exceptions(conn.cursor(), job_id, [])

    assert (await db("SELECT status FROM smb_scraping_jobs")) == [("COMPLETED",)]


async def test_update_job_based_on_exceptions_records_tracebacks(db):
    try:
        raise ValueError("first problem")
    except ValueError as e:
        first = e
    async with sql.connection() as conn:
        job_id = await sql.insert_job_entry(conn.cursor(), sql.Source.BIDS)
        await sql.update_job_based_on_exceptions(conn.cursor(), job_id, [first, RuntimeError("second problem")])

    [(status, stop_time, error_msg)] = await db("SELECT status, stop_time, error_msg FROM smb_scraping_jobs")
    assert status == "ERROR"
    assert stop_time is not None
    assert "first problem" in error_msg and "second problem" in error_msg
    assert "Traceback" in error_msg


async def test_get_last_completed_scan_with_buffer(db):
    newest = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    await db(
        """INSERT INTO smb_scraping_jobs (source, status, start_time) VALUES
           ('BIDS', 'COMPLETED', %s),
           ('BIDS', 'COMPLETED', %s),
           ('BIDS', 'ERROR', %s),
           ('CAT', 'ONGOING', %s)
           RETURNING id""",
        (newest - timedelta(days=1), newest, newest + timedelta(days=1), newest),
    )
    async with sql.connection() as conn:
        result = await sql.get_last_completed_scan_with_buffer(conn.cursor())

    assert set(result) == {source.value for source in sql.Source}
    assert result["BIDS"] == newest - timedelta(minutes=sql.TIME_BUFFER_BETWEEN_SCANS_MINUTES)
    assert result["BIDS"].tzinfo is not None and result["BIDS"].utcoffset() == timedelta(0)
    assert result["CAT"] is None  # only ONGOING jobs
    assert result["RAW_DATA"] is None  # never ran


async def test_insert_scan_single_inserts_and_ignores_duplicates(db):
    scan = sql.ScanData(
        scan_time="2026-01-05 09:30:00", source=sql.Source.RAW_DATA,
        status=sql.Status.UNKOWN, full_path="//nas/a", scanner_subject_id=None,
    )
    async with sql.connection() as conn:
        await sql.insert_scan_single(conn.cursor(), scan)
        await sql.insert_scan_single(conn.cursor(), scan.model_copy(update={"full_path": "//nas/b"}))

    rows = await rows_for(db, sql.Source.RAW_DATA)
    assert rows == [(datetime(2026, 1, 5, 9, 30), "UNKOWN", "//nas/a", None)]


async def test_insert_scan_single_skips_rows_without_scan_time(db):
    scan = sql.ScanData(
        scan_time=None, source=sql.Source.FREESURFER,
        status=sql.Status.FREESURFER_SUBJECT_ONLY_DIR, full_path="//nas/fs", scanner_subject_id="S000001",
    )
    async with sql.connection() as conn:
        await sql.insert_scan_single(conn.cursor(), scan)
        # the transaction must still be usable afterwards
        await sql.insert_job_entry(conn.cursor(), sql.Source.FREESURFER)

    assert await db("SELECT count(*) FROM scan_data_history") == [(0,)]
    assert await db("SELECT count(*) FROM smb_scraping_jobs") == [(1,)]


async def test_connection_rolls_back_on_error(db):
    with pytest.raises(RuntimeError):
        async with sql.connection() as conn:
            await sql.insert_job_entry(conn.cursor(), sql.Source.BIDS)
            raise RuntimeError("fail mid transaction")

    assert await db("SELECT count(*) FROM smb_scraping_jobs") == [(0,)]


# --------------------------------------------------------------------------- full scraping runs

async def test_update_db_full_run(db, nas, sheet):
    build_standard_nas(nas)
    await cron.update_db()

    jobs = await jobs_by_source(db)
    assert set(jobs) == {source.value for source in sql.Source}
    for source, entries in jobs.items():
        assert [(status, error) for status, _, error in entries] == [("COMPLETED", None)], source
        assert entries[0][1] is not None, source

    for source, relative in SUB_SES_SOURCES.items():
        rows = await rows_for(db, source)
        assert len(rows) == 4, source
        assert {r[3] for r in rows} == {"S000001", "S000002"}
        assert {r[1] for r in rows} == {"UNKOWN"}
        assert all(r[2].startswith(nas_path(relative)) for r in rows)

    assert len(await rows_for(db, sql.Source.RAW_DATA)) == 2
    assert len(await rows_for(db, sql.Source.GOOGLE_SHEETS)) == 2
    # the subject only freesurfer dir has no scan_time and is skipped
    assert {r[1] for r in await rows_for(db, sql.Source.FREESURFER)} == {
        "FREESURFER_SUBJECT_SESSION_DIR",
        "FREESURFER_SUBJECT_SESSION_LONG_DIR",
    }


async def test_update_db_second_run_only_scrapes_modified_dirs(db, nas, sheet):
    build_standard_nas(nas)
    await cron.update_db()
    rows_before = await db("SELECT count(*) FROM scan_data_history")

    # a new session shows up, which also bumps the subject directory's mtime
    now = datetime.now(tz=timezone.utc)
    root = nas_path(SUB_SES_SOURCES[sql.Source.BIDS])
    sub_path = nas.add(root, "sub-S000003", mtime=now)
    nas.add(sub_path, "ses-202609011200", mtime=now)
    nas.calls.clear()

    await cron.update_db()

    # unmodified subject directories are not even listed anymore
    ses_listings = [path for path, search in nas.calls if search == "ses-*"]
    assert ses_listings == [sub_path]
    assert (await db("SELECT count(*) FROM scan_data_history"))[0][0] == rows_before[0][0] + 1
    assert (await rows_for(db, sql.Source.BIDS))[-1][3] == "S000003"


async def test_update_db_repeated_run_does_not_duplicate_rows(db, nas, sheet):
    build_standard_nas(nas)
    await cron.update_db()
    first = await db("SELECT count(*) FROM scan_data_history")
    # wipe job history so nothing is skipped the second time
    await db("DELETE FROM smb_scraping_jobs RETURNING id")
    await cron.update_db()

    assert await db("SELECT count(*) FROM scan_data_history") == first


async def test_failed_top_level_listing_marks_job_as_error(db, nas, sheet):
    """Regression: the job used to stay ONGOING forever."""
    build_standard_nas(nas)
    nas.fail(nas_path(SUB_SES_SOURCES[sql.Source.CAT]), "sub-*", RuntimeError("boom CAT"))

    await cron.update_db()

    jobs = await jobs_by_source(db)
    [(status, stop_time, error_msg)] = jobs["CAT"]
    assert status == "ERROR"
    assert stop_time is not None
    assert "boom CAT" in error_msg

    [(orchestrator_status, _, orchestrator_error)] = jobs["ORCHESTRATORE"]
    assert orchestrator_status == "ERROR"
    assert "boom CAT" in orchestrator_error

    for source in set(jobs) - {"CAT", "ORCHESTRATORE"}:
        assert jobs[source][0][0] == "COMPLETED", source
    assert await rows_for(db, sql.Source.CAT) == []


async def test_no_job_is_left_ongoing_when_everything_fails(db, nas, sheet, monkeypatch):
    for relative in SUB_SES_SOURCES.values():
        nas.fail(nas_path(relative), "sub-*", RuntimeError("share gone"))
    nas.fail(RAW_DATA_PATH, "*", RuntimeError("share gone"))
    nas.fail(FREESURFER_PATH, "*", RuntimeError("share gone"))
    monkeypatch.setattr(cron.pd, "read_csv", lambda *a, **k: (_ for _ in ()).throw(OSError("sheet gone")))

    await cron.update_db()

    statuses = {source: entries[0][0] for source, entries in (await jobs_by_source(db)).items()}
    assert statuses == {source.value: "ERROR" for source in sql.Source}


async def test_failed_subject_marks_source_as_error_but_keeps_other_subjects(db, nas, sheet):
    build_standard_nas(nas)
    root = nas_path(SUB_SES_SOURCES[sql.Source.QSIPREP])
    nas.fail(f"{root}/sub-S000001", "ses-*", RuntimeError("broken subject"))

    await cron.update_db()

    jobs = await jobs_by_source(db)
    assert jobs["QSIPREP"][0][0] == "ERROR"
    assert "broken subject" in jobs["QSIPREP"][0][2]
    assert {r[3] for r in await rows_for(db, sql.Source.QSIPREP)} == {"S000002"}
    assert jobs["QSIRECON"][0][0] == "COMPLETED"


async def test_transient_smb_error_is_retried(db, nas, sheet, monkeypatch):
    build_standard_nas(nas)
    root = nas_path(SUB_SES_SOURCES[sql.Source.QSIRECON])
    fail_then_succeed(monkeypatch, nas, root, "sub-*", times=2)

    await cron.update_db()

    assert (await jobs_by_source(db))["QSIRECON"][0][0] == "COMPLETED"
    assert nas.calls.count((root, "sub-*")) == 3
    assert len(await rows_for(db, sql.Source.QSIRECON)) == 4


async def test_sheet_failure_does_not_roll_back_other_sources(db, nas, monkeypatch):
    """raw data, sheets and freesurfer share one transaction, each needs its own savepoint."""
    build_standard_nas(nas)

    def broken_sheet(*args, **kwargs):
        raise OSError("sheet unreachable")

    monkeypatch.setattr(cron.pd, "read_csv", broken_sheet)
    await cron.update_db()

    jobs = await jobs_by_source(db)
    assert jobs["GOOGLE_SHEETS"][0][0] == "ERROR"
    assert "sheet unreachable" in jobs["GOOGLE_SHEETS"][0][2]
    assert jobs["RAW_DATA"][0][0] == "COMPLETED"
    assert jobs["FREESURFER"][0][0] == "COMPLETED"
    assert len(await rows_for(db, sql.Source.RAW_DATA)) == 2
    assert len(await rows_for(db, sql.Source.FREESURFER)) == 2


async def test_invalid_row_fails_only_its_source(db, nas, sheet, monkeypatch):
    """A row failing mid-insert rolls back that source's savepoint, nothing else."""
    build_standard_nas(nas)
    original = sql.insert_scan_single

    async def failing_insert(cursor, scan_data):
        if scan_data.source == sql.Source.RAW_DATA and scan_data.scan_time == datetime(2026, 1, 6, 10, 15):
            await cursor.execute("SELECT 1/0")
        await original(cursor, scan_data)

    monkeypatch.setattr(sql, "insert_scan_single", failing_insert)
    await cron.update_db()

    jobs = await jobs_by_source(db)
    assert jobs["RAW_DATA"][0][0] == "ERROR"
    assert await rows_for(db, sql.Source.RAW_DATA) == []  # the first raw row was rolled back too
    assert jobs["GOOGLE_SHEETS"][0][0] == "COMPLETED"
    assert len(await rows_for(db, sql.Source.GOOGLE_SHEETS)) == 2


async def test_register_session_failure_marks_orchestrator_as_error(db, nas, sheet, monkeypatch):
    def refuse(*args, **kwargs):
        raise SMBException("access denied")

    monkeypatch.setattr(cron.smbclient, "register_session", refuse)
    with pytest.raises(SMBException):
        await cron.update_db()

    jobs = await jobs_by_source(db)
    assert list(jobs) == ["ORCHESTRATORE"]
    [(status, stop_time, error_msg)] = jobs["ORCHESTRATORE"]
    assert status == "ERROR" and stop_time is not None
    assert "access denied" in error_msg


async def test_subject_scraping_respects_concurrency_limit(db, nas, sheet, monkeypatch):
    monkeypatch.setattr(cron, "_subject_semaphore", asyncio.Semaphore(3))
    nas.delay = 0.02
    root = nas_path(SUB_SES_SOURCES[sql.Source.BIDS])
    for i in range(20):
        sub_path = nas.add(root, f"sub-S{i:06d}")
        nas.add(sub_path, f"ses-2026010{i % 9 + 1}{i:02d}00")

    await cron.update_db()

    assert nas.max_concurrent["ses-*"] <= 3
    assert len(await rows_for(db, sql.Source.BIDS)) == 20


async def test_init_and_commit_creates_scan_table(postgres_url, monkeypatch):
    pool = AsyncConnectionPool(postgres_url, open=False)
    monkeypatch.setattr(sql, "_pool", pool)
    async with await psycopg.AsyncConnection.connect(postgres_url, autocommit=True) as conn:
        await conn.execute("DROP TABLE IF EXISTS scan_data_history, smb_scraping_jobs")
    try:
        await cron.init_and_commit()  # also opens the pool
        async with pool.connection() as conn:
            cursor = await conn.execute("SELECT to_regclass('scan_data_history')")
            assert (await cursor.fetchone())[0] == "scan_data_history"
    finally:
        await pool.close()
