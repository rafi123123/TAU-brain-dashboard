"""initial schema

Mirrors api/db/schema.sql (the source sqlc generates the db package from).
Every statement keeps an IF NOT EXISTS / duplicate_object guard so this
migration also applies cleanly to a database that was created by the old
create_tables() startup code, before alembic was introduced.

Revision ID: f9f6112b9e59
Revises:
Create Date: 2026-10-12

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f9f6112b9e59"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


ENUMS = [
    """CREATE TYPE source AS ENUM (
        'QSIPREP',
        'QSIRECON',
        'FREESURFER',
        'CAT',
        'TABULAR_CAT12',
        'GOOGLE_SHEETS',
        'RAW_DATA',
        'TABULAR',
        'BIDS',
        'ORCHESTRATORE'
    )""",
    """CREATE TYPE status AS ENUM (
        'UNKOWN'
    )""",
    """CREATE TYPE job_status AS ENUM (
        'ONGOING',
        'COMPLETED',
        'ERROR'
    )""",
]

TABLES = [
    """CREATE TABLE IF NOT EXISTS scan_data_history (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        scraped_at TIMESTAMPTZ DEFAULT now(),
        scan_time TIMESTAMP NOT NULL,
        source source NOT NULL,
        status status NOT NULL,
        full_path TEXT,
        scanner_subject_id TEXT,
        UNIQUE(scan_time, source, status)
    )""",
    """CREATE TABLE IF NOT EXISTS smb_scraping_jobs (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        start_time TIMESTAMPTZ DEFAULT now(),
        stop_time TIMESTAMPTZ,
        source source NOT NULL,
        status job_status NOT NULL DEFAULT 'ONGOING',
        error_msg TEXT
    )""",
]


def upgrade() -> None:
    for ddl in ENUMS:
        # CREATE TYPE has no IF NOT EXISTS, so guard against duplicates instead
        op.execute(f"DO $$ BEGIN {ddl}; EXCEPTION WHEN duplicate_object THEN NULL; END $$")
    for ddl in TABLES:
        op.execute(ddl)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS scan_data_history")
    op.execute("DROP TABLE IF EXISTS smb_scraping_jobs")
    op.execute("DROP TYPE IF EXISTS source, status, job_status")
