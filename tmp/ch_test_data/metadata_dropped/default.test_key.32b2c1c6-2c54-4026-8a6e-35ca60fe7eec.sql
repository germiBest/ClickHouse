ATTACH TABLE _ UUID '32b2c1c6-2c54-4026-8a6e-35ca60fe7eec'
(
    `a` DateTime,
    `b` UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(a)
ORDER BY (toStartOfHour(a), b)
SETTINGS index_granularity = 8192
