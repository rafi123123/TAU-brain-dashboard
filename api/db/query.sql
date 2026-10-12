-- name: InsertJobEntry :one
INSERT INTO smb_scraping_jobs (source) VALUES (sqlc.arg(source)) RETURNING id;

-- name: UpdateJobEntry :exec
UPDATE smb_scraping_jobs
SET status = sqlc.arg(status), error_msg = sqlc.narg(error_msg), stop_time = sqlc.narg(stop_time)
WHERE id = sqlc.arg(id);

-- name: GetLastCompletedJobs :many
SELECT source, max(start_time)::timestamptz AS start_time
FROM smb_scraping_jobs
WHERE status = 'COMPLETED'
GROUP BY source;

-- name: InsertScan :exec
INSERT INTO scan_data_history (
    scan_time,
    source,
    status,
    full_path,
    scanner_subject_id
)
VALUES (
    sqlc.arg(scan_time),
    sqlc.arg(source),
    sqlc.arg(status),
    sqlc.narg(full_path),
    sqlc.narg(scanner_subject_id)
)
ON CONFLICT (scan_time, source, status) DO NOTHING;
