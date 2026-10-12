-- CREATE TYPE has no IF NOT EXISTS so create_tables() wraps these statements in a
-- duplicate_object guard at runtime (sqlc can't parse DO blocks, so keep them plain here).
-- NOTE: comments in this file must not contain semicolons, create_tables() splits on them
CREATE TYPE source AS ENUM (
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
);

CREATE TYPE status AS ENUM (
    'UNKOWN'
);

CREATE TYPE job_status AS ENUM (
    'ONGOING',
    'COMPLETED',
    'ERROR'
);

CREATE TABLE IF NOT EXISTS scan_data_history (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    scan_time TIMESTAMP NOT NULL,
    source source NOT NULL,
    status status NOT NULL,
    full_path TEXT,
    scanner_subject_id TEXT,
    UNIQUE(scan_time, source, status)
);

CREATE TABLE IF NOT EXISTS smb_scraping_jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    start_time TIMESTAMPTZ DEFAULT now(),
    stop_time TIMESTAMPTZ,
    source source NOT NULL,
    status job_status NOT NULL DEFAULT 'ONGOING',
    error_msg TEXT
);
