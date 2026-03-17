ATTACH TABLE _ UUID 'cdad8906-4926-41b8-9c2f-bb5697b36bc7'
(
    `key` UInt64,
    `value` String,
    `value2` UInt64
)
ENGINE = MergeTree
PRIMARY KEY key
ORDER BY key
SETTINGS index_granularity = 8192
