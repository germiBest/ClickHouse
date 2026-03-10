-- Tags: no-fasttest, no-random-settings, no-parallel

-- Verify that LIMIT reduces the amount of data read from S3.
-- Without the optimization, the input format would read up to max_block_size rows
-- (65409 by default) before producing the first chunk. With the optimization,
-- ReadFromObjectStorageStep caps the block size to the LIMIT value, so the format
-- produces a small chunk and the pipeline is cancelled before downloading more data.

-- Write a ~10 MB file (400000 rows x 27 bytes each) to S3.
INSERT INTO FUNCTION s3(s3_conn, filename = '04033_data.tsv', format = TSV, structure = 'id UInt64, data String')
SELECT number, repeat('x', 20) FROM numbers(400000)
SETTINGS s3_truncate_on_insert = 1;

-- Read the full file to establish a baseline.
SELECT *
FROM s3(s3_conn, filename = '04033_data.tsv', format = TSV, structure = 'id UInt64, data String')
FORMAT Null
SETTINGS max_threads = 1, log_queries = 1, log_queries_min_type = 'QUERY_FINISH';

-- Read with LIMIT 10.
SELECT *
FROM s3(s3_conn, filename = '04033_data.tsv', format = TSV, structure = 'id UInt64, data String')
LIMIT 10
FORMAT Null
SETTINGS max_threads = 1, log_queries = 1, log_queries_min_type = 'QUERY_FINISH';

SYSTEM FLUSH LOGS query_log;

-- The LIMIT query should read much less data than the full scan.
-- Full file is ~10 MB, LIMIT 10 should read at most 1 MB (one buffer fill).
-- We check that the LIMIT query reads less data, and at least 5x less.
SELECT
    limit_bytes < full_bytes AS limit_reads_less,
    limit_bytes * 5 < full_bytes AS at_least_5x_reduction
FROM
(
    SELECT
        (
            SELECT ProfileEvents['ReadBufferFromS3Bytes']
            FROM system.query_log
            WHERE event_date >= yesterday()
                AND event_time >= now() - 600
                AND current_database = currentDatabase()
                AND type = 'QueryFinish'
                AND query LIKE '%04033_data.tsv%'
                AND query NOT LIKE '%system.query_log%'
                AND query NOT LIKE '%LIMIT%'
                AND query NOT LIKE '%INSERT%'
            ORDER BY event_time DESC
            LIMIT 1
        ) AS full_bytes,
        (
            SELECT ProfileEvents['ReadBufferFromS3Bytes']
            FROM system.query_log
            WHERE event_date >= yesterday()
                AND event_time >= now() - 600
                AND current_database = currentDatabase()
                AND type = 'QueryFinish'
                AND query LIKE '%04033_data.tsv%LIMIT 10%'
                AND query NOT LIKE '%system.query_log%'
            ORDER BY event_time DESC
            LIMIT 1
        ) AS limit_bytes
);

-- Verify the LIMIT query returns exactly 10 rows.
SELECT count() FROM (
    SELECT *
    FROM s3(s3_conn, filename = '04033_data.tsv', format = TSV, structure = 'id UInt64, data String')
    LIMIT 10
);
