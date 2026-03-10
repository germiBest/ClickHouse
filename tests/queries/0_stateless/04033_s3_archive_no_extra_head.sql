-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- Verify that reading files from an S3 archive does not produce redundant HeadObject
-- requests. The archive file's metadata (fetched once when opening the archive) should
-- be propagated to each ObjectInfoInArchive, so downstream consumers (createReader,
-- schema cache) don't need to re-fetch it.
--
-- Test uses 03036_archive1.zip which contains example1.csv and example2.csv
-- (each with a header row and 2 data rows, totaling 4 rows).
-- With explicit schema (no inference) and a non-glob archive path, we expect
-- exactly 1 HeadObject (for the archive metadata in ArchiveIterator::next).

SET max_threads = 1;

SELECT count()
FROM s3('http://localhost:11111/test/03036_archive1.zip :: *.csv', 'test', 'testtest', 'CSVWithNames', 'id UInt64, data String');

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['S3HeadObject']
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query ILIKE 'SELECT count()%03036_archive1.zip%CSVWithNames%id UInt64%';
