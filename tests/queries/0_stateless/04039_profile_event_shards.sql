-- Tags: shard

-- Single distributed query: 2 shards
SELECT count() FROM remote('127.0.0.{1,2}', system.one) FORMAT Null SETTINGS log_comment = '04039_profile_event_shards_1';
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['Shards']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04039_profile_event_shards_1'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Single distributed query: 5 shards
SELECT count() FROM remote('127.0.0.{1,2,3,4,5}', system.one) FORMAT Null SETTINGS log_comment = '04039_profile_event_shards_2';
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['Shards']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04039_profile_event_shards_2'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;
